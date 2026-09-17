package etcd

import (
	"context"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/tools/discovery"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type resolverKV struct {
	clientv3.KV
	get func(context.Context) (*clientv3.GetResponse, error)
}

func (kv resolverKV) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return kv.get(ctx)
}

type resolverWatch struct {
	clientv3.Watcher
	watch func(context.Context, int64) clientv3.WatchChan
}

func (watch resolverWatch) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return watch.watch(ctx, clientv3.OpGet(key, opts...).Rev())
}

type resolverConn struct {
	resolver.ClientConn
	states chan resolver.State
	errors chan error
}

func (conn resolverConn) UpdateState(state resolver.State) error {
	conn.states <- state
	return nil
}

func (conn resolverConn) ReportError(err error) { conn.errors <- err }

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("resolver operation did not complete")
		var zero T
		return zero
	}
}

// testContext stands in for t.Context(), which needs go1.24; this module still
// declares go1.22 so that consumers are not forced onto a newer toolchain.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

// A watch that dies must not strand the address list. This is the regression
// test for the production failure: the old resolver abandoned its watch on the
// first error and left gRPC dialing endpoints that had already been replaced.
func Test_Resolver_replaces_snapshot_when_watch_closes(t *testing.T) {
	client := clientv3.NewCtxClient(testContext(t))
	var calls atomic.Int64
	client.KV = resolverKV{get: func(context.Context) (*clientv3.GetResponse, error) {
		revision := calls.Add(1)
		response := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: revision}}
		switch revision {
		case 1:
			response.Kvs = []*mvccpb.KeyValue{{Key: []byte("service/A"), Value: []byte(`{"Op":0,"Addr":"A","Metadata":{"zone":"old"}}`)}}
		case 3:
			response.Kvs = []*mvccpb.KeyValue{{Key: []byte("service/B"), Value: []byte(`{"Op":0,"Addr":"B","Metadata":{"zone":"new"}}`)}}
		}
		return response, nil
	}}
	revisions := make(chan int64, 3)
	active := make(chan context.Context, 1)
	client.Watcher = resolverWatch{watch: func(ctx context.Context, revision int64) clientv3.WatchChan {
		revisions <- revision
		updates := make(chan clientv3.WatchResponse)
		if revision < 4 {
			close(updates)
		} else {
			active <- ctx
		}
		return updates
	}}
	conn := resolverConn{states: make(chan resolver.State, 4), errors: make(chan error, 4)}
	watcher, err := (resolverBuilder{client: client}).Build(resolver.Target{URL: url.URL{Path: "/service"}}, conn, resolver.BuildOptions{})
	require.NoError(t, err)
	t.Cleanup(watcher.Close)

	require.Equal(t, "A", receive(t, conn.states).Addresses[0].Addr)
	require.Empty(t, receive(t, conn.states).Addresses)
	state := receive(t, conn.states)
	require.Len(t, state.Addresses, 1)
	require.Equal(t, "B", state.Addresses[0].Addr)
	require.Equal(t, map[string]interface{}{"zone": "new"}, state.Addresses[0].Metadata)
	for _, revision := range []int64{2, 3, 4} {
		require.Equal(t, revision, receive(t, revisions))
	}
	watchCtx := receive(t, active)
	watcher.Close()
	require.ErrorIs(t, watchCtx.Err(), context.Canceled)
}

func Test_Resolver_cancels_work_when_closed(t *testing.T) {
	for _, phase := range []string{"snapshot", "watch", "backoff", "client"} {
		t.Run(phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(testContext(t))
			defer cancel()
			client := clientv3.NewCtxClient(parent)
			entered := make(chan context.Context, 1)
			client.KV = resolverKV{get: func(ctx context.Context) (*clientv3.GetResponse, error) {
				if phase == "snapshot" || phase == "client" {
					entered <- ctx
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if phase == "backoff" {
					return nil, errors.New("snapshot unavailable")
				}
				return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
			}}
			client.Watcher = resolverWatch{watch: func(ctx context.Context, _ int64) clientv3.WatchChan {
				entered <- ctx
				return make(chan clientv3.WatchResponse)
			}}
			conn := resolverConn{states: make(chan resolver.State, 1), errors: make(chan error, 1)}
			watcher, err := (resolverBuilder{client: client}).Build(resolver.Target{URL: url.URL{Path: "/service"}}, conn, resolver.BuildOptions{})
			require.NoError(t, err)
			t.Cleanup(watcher.Close)
			if phase == "backoff" {
				require.EqualError(t, receive(t, conn.errors), "snapshot unavailable")
			} else {
				operation := receive(t, entered)
				t.Cleanup(func() { require.ErrorIs(t, operation.Err(), context.Canceled) })
			}

			closed := make(chan struct{})
			if phase == "client" {
				cancel()
				closed = watcher.(*endpointResolver).done
			} else {
				go func() {
					watcher.Close()
					close(closed)
				}()
			}
			receive(t, closed)
		})
	}
}

// Retries must space themselves out rather than spin. Upstream pins the exact
// schedule with testing/synctest; on go1.22 the delays are asserted against real
// elapsed time, so this only checks the first two gaps to stay quick.
func Test_Resolver_retries_with_growing_backoff(t *testing.T) {
	client := clientv3.NewCtxClient(testContext(t))
	attempts := make(chan time.Time, 3)
	client.KV = resolverKV{get: func(context.Context) (*clientv3.GetResponse, error) {
		select {
		case attempts <- time.Now():
		default:
		}
		return nil, errors.New("snapshot unavailable")
	}}
	client.Watcher = resolverWatch{watch: func(context.Context, int64) clientv3.WatchChan {
		updates := make(chan clientv3.WatchResponse)
		close(updates)
		return updates
	}}
	conn := resolverConn{states: make(chan resolver.State, 8), errors: make(chan error, 8)}
	watcher, err := (resolverBuilder{client: client}).Build(resolver.Target{URL: url.URL{Path: "/service"}}, conn, resolver.BuildOptions{})
	require.NoError(t, err)
	t.Cleanup(watcher.Close)

	first, second, third := receive(t, attempts), receive(t, attempts), receive(t, attempts)
	require.GreaterOrEqual(t, second.Sub(first), watchRetryInitialDelay)
	require.Greater(t, third.Sub(second), second.Sub(first))
}

// The fan-out path has the same failure mode as the resolver did: GetConns only
// refetches when the connection map is empty, so a watch that exits for good
// leaves every caller dialing addresses that no longer exist.
func Test_ServiceWatch_resyncs_after_watch_closes(t *testing.T) {
	client := clientv3.NewCtxClient(testContext(t))
	snapshots := make(chan struct{}, 4)
	client.KV = resolverKV{get: func(context.Context) (*clientv3.GetResponse, error) {
		select {
		case snapshots <- struct{}{}:
		default:
		}
		return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
	}}
	client.Watcher = resolverWatch{watch: func(context.Context, int64) clientv3.WatchChan {
		updates := make(chan clientv3.WatchResponse)
		close(updates)
		return updates
	}}

	registry := &SvcDiscoveryRegistryImpl{
		client:             client,
		rootDirectory:      "openim",
		connMap:            make(map[string][]*addrConn),
		serviceDialOptions: make(map[string][]grpc.DialOption),
		serviceWatchers:    make(map[string]*serviceWatcher),
		watchKeyEntries:    make(map[string]*watchKeyEntry),
	}
	require.NoError(t, registry.ensureServiceWatch("conversation"))
	t.Cleanup(registry.stopServiceWatches)

	// Three snapshots means the watch died twice and came back on its own.
	for i := 0; i < 3; i++ {
		receive(t, snapshots)
	}
}

// A watch goroutine that exits must retire only its own map entry, so that a
// watch started in the meantime keeps running.
func Test_ServiceWatch_clears_only_its_own_entry(t *testing.T) {
	registry := &SvcDiscoveryRegistryImpl{serviceWatchers: make(map[string]*serviceWatcher)}
	retired, current := &serviceWatcher{cancel: func() {}}, &serviceWatcher{cancel: func() {}}

	registry.serviceWatchers["conversation"] = current
	registry.clearServiceWatcher("conversation", retired)
	require.Same(t, current, registry.serviceWatchers["conversation"])

	registry.clearServiceWatcher("conversation", current)
	require.NotContains(t, registry.serviceWatchers, "conversation")
}

// The key-watch path fails the same way, and more quietly: ending the loop closes
// every subscriber and hands WatchKey's caller a nil error, so a consumer that
// stopped receiving updates has nothing to distinguish that from a clean finish.
func Test_KeyWatch_replays_state_after_reconnect(t *testing.T) {
	ctx := testContext(t)
	client := clientv3.NewCtxClient(ctx)
	client.KV = resolverKV{get: func(context.Context) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 1},
			Kvs:    []*mvccpb.KeyValue{{Key: []byte("openim/config/a"), Value: []byte("v")}},
		}, nil
	}}
	var watches atomic.Int64
	client.Watcher = resolverWatch{watch: func(context.Context, int64) clientv3.WatchChan {
		updates := make(chan clientv3.WatchResponse)
		if watches.Add(1) == 1 {
			close(updates)
		}
		return updates
	}}
	registry := &SvcDiscoveryRegistryImpl{
		client:          client,
		rootDirectory:   "openim",
		watchKeyEntries: make(map[string]*watchKeyEntry),
	}

	events := make(chan *discovery.WatchKey, 4)
	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		_ = registry.WatchKey(watchCtx, "config", func(event *discovery.WatchKey) error {
			events <- event
			return nil
		})
	}()

	// The watch delivers nothing, so an event can only have come from the replay
	// that follows the reconnect.
	event := receive(t, events)
	require.Equal(t, discovery.WatchTypePut, event.Type)
	require.Equal(t, "openim/config/a", string(event.Key))
	require.Equal(t, "v", string(event.Value))
	require.GreaterOrEqual(t, watches.Load(), int64(2))
}

// The first attempt must not replay: WatchKey is a subscription to changes, and
// opening it with the whole prefix would be a behaviour change for its callers.
func Test_KeyWatch_does_not_replay_on_first_attempt(t *testing.T) {
	ctx := testContext(t)
	client := clientv3.NewCtxClient(ctx)
	client.KV = resolverKV{get: func(context.Context) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 1},
			Kvs:    []*mvccpb.KeyValue{{Key: []byte("openim/config/a"), Value: []byte("v")}},
		}, nil
	}}
	client.Watcher = resolverWatch{watch: func(context.Context, int64) clientv3.WatchChan {
		return make(chan clientv3.WatchResponse)
	}}
	registry := &SvcDiscoveryRegistryImpl{
		client:          client,
		rootDirectory:   "openim",
		watchKeyEntries: make(map[string]*watchKeyEntry),
	}

	events := make(chan *discovery.WatchKey, 4)
	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go func() {
		_ = registry.WatchKey(watchCtx, "config", func(event *discovery.WatchKey) error {
			events <- event
			return nil
		})
	}()

	select {
	case event := <-events:
		t.Fatalf("first attempt published %q before any change", event.Key)
	case <-time.After(3 * watchRetryInitialDelay):
	}
}
