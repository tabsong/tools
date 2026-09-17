package etcd

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/utils/datautil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	gresolver "google.golang.org/grpc/resolver"
)

const (
	defaultRegisterTimeout = 5 * time.Second
	defaultLeaseTTL        = int64(30)
	keepAliveRetryDelay    = time.Second
	defaultCloseTimeout    = 5 * time.Second
	watchRetryInitialDelay = 100 * time.Millisecond
	watchRetryMaxDelay     = 3 * time.Second
)

// CfgOption defines a function type for modifying clientv3.Config
type CfgOption func(*clientv3.Config)
type addrConn struct {
	conn        *grpc.ClientConn
	addr        string
	isConnected bool
}

// serviceWatcher is the handle for one service's watch goroutine. It exists so
// the goroutine can retire its own map entry without racing a watch that a later
// ensureServiceWatch already put in its place.
type serviceWatcher struct {
	cancel context.CancelFunc
}

// SvcDiscoveryRegistryImpl implementation
type SvcDiscoveryRegistryImpl struct {
	client            *clientv3.Client
	resolver          gresolver.Builder
	dialOptions       []grpc.DialOption
	serviceKey        string
	endpointMgr       endpoints.Manager
	leaseID           clientv3.LeaseID
	rpcRegisterTarget string
	watchNames        []string

	rootDirectory string

	mu                 sync.RWMutex
	connMap            map[string][]*addrConn
	serviceDialOptions map[string][]grpc.DialOption
	serviceWatchMu     sync.Mutex
	serviceWatchers    map[string]*serviceWatcher
	watchKeyMu         sync.Mutex
	watchKeyEntries    map[string]*watchKeyEntry

	regMu             sync.Mutex
	keepAliveCancel   context.CancelFunc
	registeredService string
	registeredHost    string
	registeredPort    int
}

type watchKeyEntry struct {
	key    string
	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.RWMutex
	subs map[*watchKeySubscriber]struct{}
}

type watchKeySubscriber struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan *discovery.WatchKey
}

func (e *watchKeyEntry) addSubscriber(sub *watchKeySubscriber) {
	e.mu.Lock()
	if e.subs == nil {
		e.subs = make(map[*watchKeySubscriber]struct{})
	}
	e.subs[sub] = struct{}{}
	e.mu.Unlock()
}

func (e *watchKeyEntry) removeSubscriber(sub *watchKeySubscriber) bool {
	e.mu.Lock()
	if e.subs == nil {
		e.mu.Unlock()
		return true
	}
	delete(e.subs, sub)
	empty := len(e.subs) == 0
	e.mu.Unlock()
	return empty
}

func (e *watchKeyEntry) broadcast(r *SvcDiscoveryRegistryImpl, event *discovery.WatchKey) {
	e.mu.RLock()
	if len(e.subs) == 0 {
		e.mu.RUnlock()
		return
	}
	subs := make([]*watchKeySubscriber, 0, len(e.subs))
	for sub := range e.subs {
		subs = append(subs, sub)
	}
	e.mu.RUnlock()

	for _, sub := range subs {
		if !sub.push(event) {
			r.removeWatchKeySubscriber(e.key, e, sub)
		}
	}
}

func (e *watchKeyEntry) closeSubscribers() {
	e.mu.RLock()
	if len(e.subs) == 0 {
		e.mu.RUnlock()
		return
	}
	subs := make([]*watchKeySubscriber, 0, len(e.subs))
	for sub := range e.subs {
		subs = append(subs, sub)
	}
	e.mu.RUnlock()

	for _, sub := range subs {
		sub.cancel()
	}
}

func (s *watchKeySubscriber) push(event *discovery.WatchKey) bool {
	select {
	case <-s.ctx.Done():
		return false
	default:
	}

	select {
	case s.events <- event:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// run follows one key prefix for as long as anyone is subscribed. Like the
// service watch, a broken watch is retried rather than treated as the end of the
// stream: ending it closes every subscriber and leaves WatchKey returning nil to
// its caller, which reads as an orderly finish and leaves nobody to notice that
// updates simply stopped.
func (e *watchKeyEntry) run(r *SvcDiscoveryRegistryImpl) {
	defer func() {
		e.closeSubscribers()
		r.removeWatchKeyEntry(e.key, e)
	}()

	delay := watchRetryInitialDelay
	replay := false
	for e.ctx.Err() == nil {
		err := e.watchOnce(r, replay, func() { delay = watchRetryInitialDelay })
		if e.ctx.Err() != nil {
			return
		}
		log.ZWarn(e.ctx, "key watch interrupted, retrying", err,
			zap.String("key", e.key), zap.Duration("retryIn", delay))
		if !sleepWithContext(e.ctx, delay) {
			return
		}
		delay = min(delay*2, watchRetryMaxDelay)
		// Whatever changed while the watch was down produced no event, so every
		// later attempt republishes the current state and lets subscribers
		// converge. The first attempt does not: callers subscribe for changes,
		// and a stream that opens with the whole prefix would be a surprise.
		replay = true
	}
}

// watchOnce reads the prefix, optionally republishes it, then follows it from
// that exact revision so nothing slips through between the two.
func (e *watchKeyEntry) watchOnce(r *SvcDiscoveryRegistryImpl, replay bool, recovered func()) error {
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	r.mu.RLock()
	client := r.client
	r.mu.RUnlock()
	if client == nil {
		return errs.New("etcd client closed", "key", e.key)
	}

	snapshot, err := client.Get(ctx, e.key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if replay {
		for _, kv := range snapshot.Kvs {
			e.broadcast(r, &discovery.WatchKey{Type: discovery.WatchTypePut, Key: kv.Key, Value: kv.Value})
		}
	}

	updates := client.Watch(ctx, e.key, clientv3.WithPrefix(),
		clientv3.WithRev(snapshot.Header.Revision+1), clientv3.WithProgressNotify())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, open := <-updates:
			if !open {
				return errs.New("key watch closed", "key", e.key)
			}
			if err := resp.Err(); err != nil {
				return err
			}
			recovered()
			for _, event := range resp.Events {
				watchKey := &discovery.WatchKey{Key: event.Kv.Key, Value: event.Kv.Value}
				switch event.Type {
				case mvccpb.PUT:
					watchKey.Type = discovery.WatchTypePut
				case mvccpb.DELETE:
					watchKey.Type = discovery.WatchTypeDelete
				default:
					continue
				}
				e.broadcast(r, watchKey)
			}
		}
	}
}

func createNoOpLogger() *zap.Logger {
	// Create a no-op write syncer
	noOpWriter := zapcore.AddSync(io.Discard)

	// Create a basic zap core with the no-op writer
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		noOpWriter,
		zapcore.InfoLevel, // You can set this to any level that suits your needs
	)

	// Create the logger using the core
	return zap.New(core)
}

// NewSvcDiscoveryRegistry creates a new service discovery registry implementation
func NewSvcDiscoveryRegistry(rootDirectory string, endpoints []string, watchNames []string, options ...CfgOption) (*SvcDiscoveryRegistryImpl, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		// Increase keep-alive queue capacity and message size
		PermitWithoutStream: true,
		Logger:              createNoOpLogger(),
		MaxCallSendMsgSize:  10 * 1024 * 1024, // 10 MB
	}

	// Apply provided options to the config
	for _, opt := range options {
		opt(&cfg)
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	// Use the self-healing resolver instead of etcd's official one: that one
	// abandons its watch for good on any error (ErrCompacted is routine), and
	// its ResolveNow is a no-op, so a client whose watch died keeps dialing
	// addresses that no longer exist until the process restarts.
	r := resolverBuilder{client: client}

	s := &SvcDiscoveryRegistryImpl{
		client:             client,
		resolver:           r,
		rootDirectory:      rootDirectory,
		connMap:            make(map[string][]*addrConn),
		serviceDialOptions: make(map[string][]grpc.DialOption),
		watchNames:         watchNames,
		serviceWatchers:    make(map[string]*serviceWatcher),
		watchKeyEntries:    make(map[string]*watchKeyEntry),
	}

	s.watchServiceChanges()
	return s, nil
}

// initializeConnMap fetches all existing endpoints for the given service and populates the local map
func (r *SvcDiscoveryRegistryImpl) initializeConnMap(service string, opts ...grpc.DialOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return fmt.Errorf("etcd client closed")
	}

	ctx := context.Background()
	fullPrefix := fmt.Sprintf("%s/%s", r.rootDirectory, service)
	resp, err := r.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	oldList := r.connMap[fullPrefix]

	addrMap := make(map[string]*addrConn, len(oldList))
	for _, conn := range oldList {
		addrMap[conn.addr] = conn
	}
	newList := make([]*addrConn, 0, len(oldList))
	for _, kv := range resp.Kvs {
		prefix, addr := r.splitEndpoint(string(kv.Key))
		if addr == "" {
			continue
		}
		if _, _, err = net.SplitHostPort(addr); err != nil {
			continue
		}
		if prefix != fullPrefix {
			continue
		}

		if conn, ok := addrMap[addr]; ok {
			conn.isConnected = true
			continue
		}

		dialOpts := append([]grpc.DialOption{}, r.dialOptions...)
		if storedOpts, ok := r.serviceDialOptions[fullPrefix]; ok && len(storedOpts) > 0 {
			dialOpts = append(dialOpts, storedOpts...)
		} else if len(opts) > 0 {
			dialOpts = append(dialOpts, opts...)
		}
		dialOpts = append(dialOpts, grpc.WithResolvers(r.resolver))

		err := r.checkOpts(dialOpts...) // Check opts in include mw.GrpcClient()
		if err != nil {
			return errs.WrapMsg(err, "checkOpts is failed")
		}

		conn, err := grpc.DialContext(context.Background(), addr, dialOpts...)
		if err != nil {
			continue
		}
		newList = append(newList, &addrConn{conn: conn, addr: addr, isConnected: false})
	}
	for _, conn := range oldList {
		if conn.isConnected {
			conn.isConnected = false
			newList = append(newList, conn)
			continue
		}
		if err = conn.conn.Close(); err != nil {
			log.ZWarn(ctx, "close conn err", err)
		}
	}
	r.connMap[fullPrefix] = newList

	return nil
}

// WithDialTimeout sets a custom dial timeout for the etcd client
func WithDialTimeout(timeout time.Duration) CfgOption {
	return func(cfg *clientv3.Config) {
		cfg.DialTimeout = timeout
	}
}

// WithMaxCallSendMsgSize sets a custom max call send message size for the etcd client
func WithMaxCallSendMsgSize(size int) CfgOption {
	return func(cfg *clientv3.Config) {
		cfg.MaxCallSendMsgSize = size
	}
}

// WithUsernameAndPassword sets a username and password for the etcd client
func WithUsernameAndPassword(username, password string) CfgOption {
	return func(cfg *clientv3.Config) {
		cfg.Username = username
		cfg.Password = password
	}
}

// GetUserIdHashGatewayHost returns the gateway host for a given user ID hash
func (r *SvcDiscoveryRegistryImpl) GetUserIdHashGatewayHost(ctx context.Context, userId string) (string, error) {
	return "", nil
}

// GetConns returns gRPC client connections for a given service name
func (r *SvcDiscoveryRegistryImpl) GetConns(ctx context.Context, serviceName string, opts ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	if err := r.ensureServiceWatch(serviceName); err != nil {
		return nil, err
	}

	fullServiceKey := fmt.Sprintf("%s/%s", r.rootDirectory, serviceName)

	if len(opts) > 0 {
		r.mu.Lock()
		r.serviceDialOptions[fullServiceKey] = append([]grpc.DialOption(nil), opts...)
		r.mu.Unlock()
	}

	r.mu.RLock()
	existing := r.connMap[fullServiceKey]
	r.mu.RUnlock()

	if len(existing) == 0 {
		if err := r.initializeConnMap(serviceName, opts...); err != nil {
			return nil, err
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return datautil.Batch(func(t *addrConn) grpc.ClientConnInterface { return t.conn }, r.connMap[fullServiceKey]), nil
}

// GetConn returns a single gRPC client connection for a given service name
func (r *SvcDiscoveryRegistryImpl) GetConn(ctx context.Context, serviceName string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	target := fmt.Sprintf("etcd:///%s/%s", r.rootDirectory, serviceName)

	dialOpts := append(append(r.dialOptions, opts...), grpc.WithResolvers(r.resolver))

	err := r.checkOpts(dialOpts...) // Check opts in include mw.GrpcClient()
	if err != nil {
		return nil, errs.WrapMsg(err, "checkOpts is failed")
	}

	return grpc.DialContext(ctx, target, dialOpts...)
}

// GetSelfConnTarget returns the connection target for the current service
func (r *SvcDiscoveryRegistryImpl) GetSelfConnTarget() string {
	return r.rpcRegisterTarget
}

func (r *SvcDiscoveryRegistryImpl) IsSelfNode(cc grpc.ClientConnInterface) bool {
	cli, ok := cc.(*grpc.ClientConn)
	if !ok {
		return false
	}
	return r.GetSelfConnTarget() == cli.Target()
}

// AddOption appends gRPC dial options to the existing options
func (r *SvcDiscoveryRegistryImpl) AddOption(opts ...grpc.DialOption) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetConnMap()
	r.dialOptions = append(r.dialOptions, opts...)
}

// Register registers a new service endpoint with etcd
func (r *SvcDiscoveryRegistryImpl) Register(ctx context.Context, serviceName, host string, port int, opts ...grpc.DialOption) error {
	r.regMu.Lock()
	defer r.regMu.Unlock()

	if r.client == nil {
		return fmt.Errorf("etcd client is closed")
	}

	if r.keepAliveCancel != nil {
		r.keepAliveCancel()
		r.keepAliveCancel = nil
	}

	if r.leaseID != 0 {
		if _, err := r.client.Revoke(context.Background(), r.leaseID); err != nil {
			log.ZWarn(ctx, "failed to revoke previous lease", err, zap.String("service", serviceName), zap.String("addr", net.JoinHostPort(host, strconv.Itoa(port))))
		}
		r.leaseID = 0
	}

	registerCtx, cancel := withTimeout(ctx, defaultRegisterTimeout)
	defer cancel()

	if err := r.registerLocked(registerCtx, serviceName, host, port); err != nil {
		return err
	}

	keepCtx, keepCancel := context.WithCancel(context.Background())
	r.keepAliveCancel = keepCancel
	go r.keepAliveLoop(keepCtx)

	return nil
}

func (r *SvcDiscoveryRegistryImpl) registerLocked(ctx context.Context, serviceName, host string, port int) error {
	if ctx == nil {
		ctx = context.Background()
	}

	serviceDir := fmt.Sprintf("%s/%s", r.rootDirectory, serviceName)
	serviceKey := fmt.Sprintf("%s/%s", serviceDir, net.JoinHostPort(host, strconv.Itoa(port)))

	manager, err := endpoints.NewManager(r.client, serviceDir)
	if err != nil {
		return err
	}

	leaseResp, err := r.client.Grant(ctx, defaultLeaseTTL)
	if err != nil {
		return err
	}

	endpointAddr := net.JoinHostPort(host, strconv.Itoa(port))
	endpoint := endpoints.Endpoint{Addr: endpointAddr}

	if err := manager.AddEndpoint(ctx, serviceKey, endpoint, clientv3.WithLease(leaseResp.ID)); err != nil {
		return err
	}

	r.endpointMgr = manager
	r.serviceKey = serviceKey
	r.leaseID = leaseResp.ID
	r.rpcRegisterTarget = endpointAddr
	r.registeredService = serviceName
	r.registeredHost = host
	r.registeredPort = port

	return nil
}

func (r *SvcDiscoveryRegistryImpl) keepAliveLoop(ctx context.Context) {
outer:
	for {
		if ctx.Err() != nil {
			return
		}
		client := r.client
		if client == nil {
			return
		}

		r.regMu.Lock()
		leaseID := r.leaseID
		r.regMu.Unlock()
		if leaseID == 0 {
			return
		}

		ch, err := client.KeepAlive(ctx, leaseID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !r.reRegister(ctx, err) {
				if !sleepWithContext(ctx, keepAliveRetryDelay) {
					return
				}
			}
			continue
		}

		for {
			select {
			case <-ctx.Done():
				return
			case ka, ok := <-ch:
				if !ok || ka == nil {
					if ctx.Err() != nil {
						return
					}
					if !r.reRegister(ctx, fmt.Errorf("keepalive channel closed")) {
						if !sleepWithContext(ctx, keepAliveRetryDelay) {
							return
						}
					}
					continue outer
				}
			}
		}
	}
}

func (r *SvcDiscoveryRegistryImpl) reRegister(ctx context.Context, cause error) bool {
	r.regMu.Lock()
	defer r.regMu.Unlock()

	if r.client == nil || r.registeredService == "" || r.registeredHost == "" {
		return false
	}

	addr := net.JoinHostPort(r.registeredHost, strconv.Itoa(r.registeredPort))
	log.ZWarn(context.Background(), "etcd keepalive lost, re-registering endpoint", cause, zap.String("service", r.registeredService), zap.String("addr", addr))

	if r.leaseID != 0 {
		if _, err := r.client.Revoke(context.Background(), r.leaseID); err != nil {
			log.ZWarn(context.Background(), "failed to revoke stale lease", err, zap.String("service", r.registeredService), zap.String("addr", addr))
		}
		r.leaseID = 0
	}

	retryCtx, cancel := withTimeout(ctx, defaultRegisterTimeout)
	defer cancel()

	if err := r.registerLocked(retryCtx, r.registeredService, r.registeredHost, r.registeredPort); err != nil {
		log.ZWarn(context.Background(), "re-register endpoint failed", err, zap.String("service", r.registeredService), zap.String("addr", addr))
		return false
	}

	return true
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), d)
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// keepAliveLease maintains the lease alive by sending keep-alive requests
func (r *SvcDiscoveryRegistryImpl) keepAliveLease(leaseID clientv3.LeaseID) {
	ch, err := r.client.KeepAlive(context.Background(), leaseID)
	if err != nil {
		return
	}
	for ka := range ch {
		if ka != nil {
		} else {
			return
		}
	}
}

// watchServiceChanges watches for changes in the service directory
func (r *SvcDiscoveryRegistryImpl) watchServiceChanges() {
	for _, s := range r.watchNames {
		if err := r.ensureServiceWatch(s); err != nil {
			log.ZWarn(context.Background(), "ensure service watch err", err, zap.String("service", s))
		}
	}
}

func (r *SvcDiscoveryRegistryImpl) ensureServiceWatch(service string) error {
	r.serviceWatchMu.Lock()
	if _, exists := r.serviceWatchers[service]; exists {
		r.serviceWatchMu.Unlock()
		return nil
	}

	if r.client == nil {
		r.serviceWatchMu.Unlock()
		return fmt.Errorf("etcd client closed")
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	watcher := &serviceWatcher{cancel: cancel}
	r.serviceWatchers[service] = watcher
	r.serviceWatchMu.Unlock()

	go r.runServiceWatch(watchCtx, service, watcher)

	return nil
}

// runServiceWatch keeps one service's connection map in step with etcd. Each
// attempt resyncs from a full snapshot and then follows the prefix; when the
// watch breaks — a compacted revision is routine, not exceptional — the loop
// backs off and starts over instead of returning. Returning would strand the map
// on addresses that no longer resolve, and GetConns only refetches when the map
// is empty, so a stale-but-populated map is never revisited: every caller keeps
// dialing dead pods until the process restarts.
func (r *SvcDiscoveryRegistryImpl) runServiceWatch(ctx context.Context, service string, watcher *serviceWatcher) {
	defer r.clearServiceWatcher(service, watcher)

	delay := watchRetryInitialDelay
	for ctx.Err() == nil {
		err := r.watchServiceOnce(ctx, service, func() { delay = watchRetryInitialDelay })
		if ctx.Err() != nil {
			return
		}
		log.ZWarn(ctx, "service watch interrupted, retrying", err,
			zap.String("service", service), zap.Duration("retryIn", delay))
		if !sleepWithContext(ctx, delay) {
			return
		}
		delay = min(delay*2, watchRetryMaxDelay)
	}
}

// watchServiceOnce runs a single snapshot-then-follow cycle, returning the error
// that ended it so the caller can decide whether to retry.
func (r *SvcDiscoveryRegistryImpl) watchServiceOnce(ctx context.Context, service string, recovered func()) error {
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.mu.RLock()
	client := r.client
	r.mu.RUnlock()
	if client == nil {
		return errs.New("etcd client closed", "service", service)
	}

	if err := r.initializeConnMap(service); err != nil {
		return err
	}

	updates := client.Watch(watchCtx, fmt.Sprintf("%s/%s", r.rootDirectory, service),
		clientv3.WithPrefix(), clientv3.WithProgressNotify())
	for {
		select {
		case <-watchCtx.Done():
			return watchCtx.Err()
		case update, open := <-updates:
			if !open {
				return errs.New("service watch closed", "service", service)
			}
			if err := update.Err(); err != nil {
				return err
			}
			recovered()
			if len(update.Events) == 0 {
				continue
			}
			if err := r.initializeConnMap(service); err != nil {
				log.ZWarn(watchCtx, "initializeConnMap in watch err", err, zap.String("service", service))
			}
		}
	}
}

// clearServiceWatcher retires the map entry, but only while it still points at
// this goroutine's own watcher.
func (r *SvcDiscoveryRegistryImpl) clearServiceWatcher(service string, watcher *serviceWatcher) {
	r.serviceWatchMu.Lock()
	if current, ok := r.serviceWatchers[service]; ok && current == watcher {
		delete(r.serviceWatchers, service)
	}
	r.serviceWatchMu.Unlock()
}

func (r *SvcDiscoveryRegistryImpl) stopServiceWatches() {
	r.serviceWatchMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.serviceWatchers))
	for _, watcher := range r.serviceWatchers {
		cancels = append(cancels, watcher.cancel)
	}
	r.serviceWatchers = make(map[string]*serviceWatcher)
	r.serviceWatchMu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (r *SvcDiscoveryRegistryImpl) stopKeyWatches() {
	r.watchKeyMu.Lock()
	entries := make([]*watchKeyEntry, 0, len(r.watchKeyEntries))
	for _, entry := range r.watchKeyEntries {
		entries = append(entries, entry)
	}
	r.watchKeyEntries = make(map[string]*watchKeyEntry)
	r.watchKeyMu.Unlock()

	for _, entry := range entries {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
}

func (r *SvcDiscoveryRegistryImpl) getOrCreateWatchKeyEntry(key string) (*watchKeyEntry, error) {
	r.watchKeyMu.Lock()
	if entry, ok := r.watchKeyEntries[key]; ok {
		r.watchKeyMu.Unlock()
		return entry, nil
	}
	if r.client == nil {
		r.watchKeyMu.Unlock()
		return nil, fmt.Errorf("etcd client closed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	entry := &watchKeyEntry{
		key:    key,
		ctx:    ctx,
		cancel: cancel,
		subs:   make(map[*watchKeySubscriber]struct{}),
	}
	r.watchKeyEntries[key] = entry
	r.watchKeyMu.Unlock()

	go entry.run(r)
	return entry, nil
}

func (r *SvcDiscoveryRegistryImpl) removeWatchKeySubscriber(key string, entry *watchKeyEntry, sub *watchKeySubscriber) {
	if sub == nil || entry == nil {
		return
	}
	sub.cancel()
	empty := entry.removeSubscriber(sub)
	if !empty {
		return
	}

	r.watchKeyMu.Lock()
	if current, ok := r.watchKeyEntries[key]; ok && current == entry {
		delete(r.watchKeyEntries, key)
	}
	r.watchKeyMu.Unlock()

	if entry.cancel != nil {
		entry.cancel()
	}
}

func (r *SvcDiscoveryRegistryImpl) removeWatchKeyEntry(key string, entry *watchKeyEntry) {
	r.watchKeyMu.Lock()
	if current, ok := r.watchKeyEntries[key]; ok && current == entry {
		delete(r.watchKeyEntries, key)
	}
	r.watchKeyMu.Unlock()
}

// splitEndpoint splits the endpoint string into prefix and address
func (r *SvcDiscoveryRegistryImpl) splitEndpoint(input string) (string, string) {
	lastSlashIndex := strings.LastIndex(input, "/")
	if lastSlashIndex != -1 {
		part1 := input[:lastSlashIndex]
		part2 := input[lastSlashIndex+1:]
		return part1, part2
	}
	return input, ""
}

// UnRegister removes the service endpoint from etcd
func (r *SvcDiscoveryRegistryImpl) UnRegister() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()

	r.regMu.Lock()
	if r.keepAliveCancel != nil {
		r.keepAliveCancel()
		r.keepAliveCancel = nil
	}

	mgr := r.endpointMgr
	serviceKey := r.serviceKey
	leaseID := r.leaseID
	client := r.client

	r.endpointMgr = nil
	r.serviceKey = ""
	r.leaseID = 0
	r.registeredService = ""
	r.registeredHost = ""
	r.registeredPort = 0
	r.regMu.Unlock()

	if mgr == nil || serviceKey == "" {
		return nil
	}

	if err := mgr.DeleteEndpoint(ctx, serviceKey); err != nil {
		return err
	}

	if leaseID != 0 && client != nil {
		if _, err := client.Revoke(ctx, leaseID); err != nil {
			log.ZWarn(ctx, "failed to revoke lease during unregister", err, zap.String("serviceKey", serviceKey))
		}
	}

	return nil
}

// Close closes the etcd client connection
func (r *SvcDiscoveryRegistryImpl) Close() {
	r.stopServiceWatches()
	r.stopKeyWatches()

	if err := r.UnRegister(); err != nil {
		log.ZWarn(context.Background(), "failed to unregister on close", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.resetConnMap()
	r.serviceDialOptions = make(map[string][]grpc.DialOption)
	if r.client != nil {
		_ = r.client.Close()
		r.client = nil
	}
}

// Check verifies if etcd is running by checking the existence of the root node and optionally creates it with a lease
func Check(ctx context.Context, etcdServers []string, etcdRoot string, createIfNotExist bool, options ...CfgOption) error {
	cfg := clientv3.Config{
		Endpoints: etcdServers,
	}
	for _, opt := range options {
		opt(&cfg)
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return errs.WrapMsg(err, "failed to connect to etcd")
	}
	defer client.Close()

	var opCtx context.Context
	var cancel context.CancelFunc
	if cfg.DialTimeout != 0 {
		opCtx, cancel = context.WithTimeout(ctx, cfg.DialTimeout)
	} else {
		opCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
	}
	defer cancel()

	resp, err := client.Get(opCtx, etcdRoot)
	if err != nil {
		return errs.WrapMsg(err, "failed to get the root node from etcd")
	}

	if len(resp.Kvs) == 0 {
		if createIfNotExist {
			var leaseTTL int64 = 10
			var leaseResp *clientv3.LeaseGrantResponse
			if leaseTTL > 0 {
				leaseResp, err = client.Grant(opCtx, leaseTTL)
				if err != nil {
					return errs.WrapMsg(err, "failed to create lease in etcd")
				}
			}
			putOpts := []clientv3.OpOption{}
			if leaseResp != nil {
				putOpts = append(putOpts, clientv3.WithLease(leaseResp.ID))
			}

			_, err := client.Put(opCtx, etcdRoot, "", putOpts...)
			if err != nil {
				return errs.WrapMsg(err, "failed to create the root node in etcd")
			}
		} else {
			return fmt.Errorf("root node %s does not exist in etcd", etcdRoot)
		}
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) GetClient() *clientv3.Client {
	return r.client
}

func (r *SvcDiscoveryRegistryImpl) checkOpts(opts ...grpc.DialOption) error {
    return nil
}

func (r *SvcDiscoveryRegistryImpl) resetConnMap() {
	ctx := context.Background()
	for _, conn := range r.connMap {
		for _, c := range conn {
			if err := c.conn.Close(); err != nil {
				log.ZWarn(ctx, "failed to close conn", err)
			}
		}
	}
	r.connMap = make(map[string][]*addrConn)
}

func (r *SvcDiscoveryRegistryImpl) SetKey(ctx context.Context, key string, data []byte) error {
	if _, err := r.client.Put(ctx, key, string(data)); err != nil {
		return errs.WrapMsg(err, "etcd put err")
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) SetWithLease(ctx context.Context, key string, val []byte, ttl int64) error {
	leaseResp, err := r.client.Grant(ctx, ttl) //
	if err != nil {
		return errs.Wrap(err)
	}

	_, err = r.client.Put(ctx, key, string(val), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return errs.Wrap(err)
	}

	go r.keepAliveLease(leaseResp.ID)

	return nil
}

func (r *SvcDiscoveryRegistryImpl) GetKey(ctx context.Context, key string) ([]byte, error) {
	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, errs.WrapMsg(err, "etcd get err")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func (r *SvcDiscoveryRegistryImpl) GetKeyWithPrefix(ctx context.Context, key string) ([][]byte, error) {
	resp, err := r.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, errs.WrapMsg(err, "etcd get err")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return datautil.Batch(func(kv *mvccpb.KeyValue) []byte { return kv.Value }, resp.Kvs), nil
}

func (r *SvcDiscoveryRegistryImpl) DelData(ctx context.Context, key string) error {
	if _, err := r.client.Delete(ctx, key); err != nil {
		return errs.WrapMsg(err, "etcd delete err")
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) WatchKey(ctx context.Context, key string, fn discovery.WatchKeyHandler) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if fn == nil {
		return fmt.Errorf("watch handler is nil")
	}

	key = fmt.Sprintf("%s/%s", r.rootDirectory, key)

	entry, err := r.getOrCreateWatchKeyEntry(key)
	if err != nil {
		return err
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub := &watchKeySubscriber{
		ctx:    subCtx,
		cancel: cancel,
		events: make(chan *discovery.WatchKey, 16),
	}

	entry.addSubscriber(sub)
	defer r.removeWatchKeySubscriber(key, entry, sub)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub.ctx.Done():
			return nil
		case event := <-sub.events:
			if event == nil {
				continue
			}
			if err := fn(event); err != nil {
				return err
			}
		}
	}
}
