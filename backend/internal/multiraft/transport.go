package multiraft

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// PeerConnPool supplies outbound gRPC connections to cluster peers keyed by
// Raft server address. Production nodes use cluster.PeerConns so MultiRaft and
// ClusterService share one ClientConn per peer.
type PeerConnPool interface {
	ConnForAddress(addr raft.ServerAddress) (*grpc.ClientConn, error)
}

// Transport multiplexes multiple Raft groups over a single gRPC service.
// Each group gets a scoped raft.Transport via GroupTransport(). All groups
// share the same peer connection pool. K is the group ID type (must be comparable).
// The encodeKey/decodeKey functions convert between K and the []byte wire format.
type Transport[K comparable] struct {
	localAddress raft.ServerAddress
	encodeKey    func(K) []byte
	decodeKey    func([]byte) K

	peerPool PeerConnPool

	mu     sync.RWMutex
	groups map[K]*groupState

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// groupState holds the per-group dispatch state (rpcChan + heartbeat handler).
type groupState struct {
	rpcChan          chan raft.RPC
	doneCh           chan struct{} // closed when the group is removed
	heartbeatFunc    func(raft.RPC)
	heartbeatFuncMtx sync.Mutex
	heartbeatTimeout time.Duration
}

// multiRaftClient wraps a grpc.ClientConn for manually-invoked RPCs.
// No generated gRPC stubs — the service is registered via ServiceDesc.
type multiRaftClient struct {
	cc *grpc.ClientConn
}

const servicePath = "/gastrolog.v1.MultiRaftTransportService/"

// Per-RPC deadlines. A paused (SIGSTOPed) peer keeps its TCP socket open
// but never responds at the application layer; without a deadline, the
// caller goroutine hangs until OS TCP keepalive (minutes) kicks in. That
// cascades into leader replication stalls and cluster-wide head-of-line
// blocking — see gastrolog-5oofa.
//
// Values are chosen against hraft's defaults (HeartbeatTimeout=1s,
// ElectionTimeout=1s, LeaderLeaseTimeout=500ms): tight enough that a
// slow peer fails fast and hraft retries, generous enough to tolerate
// normal network jitter and short GC pauses.
const (
	appendEntriesRPCTimeout   = 3 * time.Second
	heartbeatRPCTimeout       = 1 * time.Second
	requestVoteRPCTimeout     = 2 * time.Second
	requestPreVoteRPCTimeout  = 2 * time.Second
	timeoutNowRPCTimeout      = 2 * time.Second
	installSnapshotRPCTimeout = 5 * time.Minute // bulk transfer; bounded by chunk/snapshot size
)

func (c *multiRaftClient) AppendEntries(ctx context.Context, req *gastrologv1.MultiRaftAppendEntriesRequest) (*gastrologv1.MultiRaftAppendEntriesResponse, error) {
	resp := new(gastrologv1.MultiRaftAppendEntriesResponse)
	if err := c.cc.Invoke(ctx, servicePath+"AppendEntries", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *multiRaftClient) RequestVote(ctx context.Context, req *gastrologv1.MultiRaftRequestVoteRequest) (*gastrologv1.MultiRaftRequestVoteResponse, error) {
	resp := new(gastrologv1.MultiRaftRequestVoteResponse)
	if err := c.cc.Invoke(ctx, servicePath+"RequestVote", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *multiRaftClient) RequestPreVote(ctx context.Context, req *gastrologv1.MultiRaftRequestPreVoteRequest) (*gastrologv1.MultiRaftRequestPreVoteResponse, error) {
	resp := new(gastrologv1.MultiRaftRequestPreVoteResponse)
	if err := c.cc.Invoke(ctx, servicePath+"RequestPreVote", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *multiRaftClient) BatchHeartbeat(ctx context.Context, req *gastrologv1.MultiRaftBatchHeartbeatRequest) (*gastrologv1.MultiRaftBatchHeartbeatResponse, error) {
	resp := new(gastrologv1.MultiRaftBatchHeartbeatResponse)
	if err := c.cc.Invoke(ctx, servicePath+"BatchHeartbeat", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *multiRaftClient) TimeoutNow(ctx context.Context, req *gastrologv1.MultiRaftTimeoutNowRequest) (*gastrologv1.MultiRaftTimeoutNowResponse, error) {
	resp := new(gastrologv1.MultiRaftTimeoutNowResponse)
	if err := c.cc.Invoke(ctx, servicePath+"TimeoutNow", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

var installSnapshotDesc = grpc.StreamDesc{
	StreamName:    "InstallSnapshot",
	ClientStreams: true,
}

func (c *multiRaftClient) InstallSnapshot(ctx context.Context) (grpc.ClientStream, error) {
	return c.cc.NewStream(ctx, &installSnapshotDesc, servicePath+"InstallSnapshot")
}

var appendEntriesPipelineDesc = grpc.StreamDesc{
	StreamName:    "AppendEntriesPipeline",
	ServerStreams: true,
	ClientStreams: true,
}

func (c *multiRaftClient) AppendEntriesPipeline(ctx context.Context) (grpc.ClientStream, error) {
	return c.cc.NewStream(ctx, &appendEntriesPipelineDesc, servicePath+"AppendEntriesPipeline")
}

// New creates a MultiRaftTransport bound to a local address.
// Call SetPeerConnPool before any outbound RPC (cluster.Server.SetRaft does
// this in production; tests use DialerPeerPool).
func New[K comparable](localAddress raft.ServerAddress, encodeKey func(K) []byte, decodeKey func([]byte) K) *Transport[K] {
	return &Transport[K]{
		localAddress: localAddress,
		encodeKey:    encodeKey,
		decodeKey:    decodeKey,
		groups:       make(map[K]*groupState),
		shutdownCh:   make(chan struct{}),
	}
}

// GroupTransport returns a raft.Transport scoped to a single group.
// Creates the group state if it doesn't exist yet.
func (t *Transport[K]) GroupTransport(groupID K) raft.Transport {
	t.mu.Lock()
	gs, ok := t.groups[groupID]
	if !ok {
		gs = &groupState{
			rpcChan: make(chan raft.RPC),
			doneCh:  make(chan struct{}),
		}
		t.groups[groupID] = gs
	}
	t.mu.Unlock()
	return &groupTransport[K]{parent: t, groupID: groupID, state: gs}
}

// RemoveGroup removes a group and closes its RPC channel.
// Any Raft instance consuming from the channel will see it close.
func (t *Transport[K]) RemoveGroup(groupID K) {
	t.mu.Lock()
	gs, ok := t.groups[groupID]
	if ok {
		delete(t.groups, groupID)
	}
	t.mu.Unlock()
	if ok {
		close(gs.doneCh)
	}
}

// getGroup returns the group state for dispatch. Returns nil if not found.
func (t *Transport[K]) getGroup(groupID K) *groupState {
	t.mu.RLock()
	gs := t.groups[groupID]
	t.mu.RUnlock()
	return gs
}

// getPeer returns a client for a peer via the configured PeerConnPool.
func (t *Transport[K]) getPeer(target raft.ServerAddress) (*multiRaftClient, error) {
	if t.peerPool == nil {
		return nil, errors.New("multiraft transport: peer connection pool not configured")
	}
	cc, err := t.peerPool.ConnForAddress(target)
	if err != nil {
		return nil, err
	}
	return &multiRaftClient{cc: cc}, nil
}

// SetPeerConnPool wires the shared outbound connection pool. Required before
// any outbound Raft RPC.
func (t *Transport[K]) SetPeerConnPool(pool PeerConnPool) {
	t.peerPool = pool
}

// LocalAddr returns the advertised local address.
func (t *Transport[K]) LocalAddr() raft.ServerAddress {
	return t.localAddress
}

// Close shuts down all connections and closes all group consumer channels.
func (t *Transport[K]) Close() error {
	t.shutdownLock.Lock()
	defer t.shutdownLock.Unlock()
	if t.shutdown {
		return nil
	}
	close(t.shutdownCh)
	t.shutdown = true

	// Signal all groups to stop. doneCh unblocks senders and the
	// Consumer() bridge goroutine, which closes the output channel.
	t.mu.Lock()
	for k, gs := range t.groups {
		close(gs.doneCh)
		delete(t.groups, k)
	}
	t.mu.Unlock()
	return nil
}

// ---------- groupTransport: raft.Transport for a single group ----------

// groupTransport implements raft.Transport, raft.WithClose, raft.WithPeers,
// and raft.WithPreVote for a single Raft group.
type groupTransport[K comparable] struct {
	parent  *Transport[K]
	groupID K
	state   *groupState
}

var (
	_ raft.Transport  = (*groupTransport[string])(nil)
	_ raft.WithClose  = (*groupTransport[string])(nil)
	_ raft.WithPeers  = (*groupTransport[string])(nil)
	_ raft.WithPreVote = (*groupTransport[string])(nil)
)

func (g *groupTransport[K]) Consumer() <-chan raft.RPC {
	// Return a bridge channel that closes when doneCh fires,
	// so callers using `for range Consumer()` exit cleanly.
	out := make(chan raft.RPC)
	go func() {
		defer close(out)
		for {
			select {
			case rpc, ok := <-g.state.rpcChan:
				if !ok {
					return
				}
				out <- rpc
			case <-g.state.doneCh:
				return
			}
		}
	}()
	return out
}
func (g *groupTransport[K]) LocalAddr() raft.ServerAddress { return g.parent.localAddress }

func (g *groupTransport[K]) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	start := time.Now()
	connStart := start
	c, err := g.parent.getPeer(target)
	connWait := time.Since(connStart)
	if err != nil {
		traceOutboundAppendEntries(g.groupID, target, args, connWait, 0, time.Since(start), err)
		return err
	}
	timeout := appendEntriesRPCTimeout
	if isHeartbeat(args) {
		timeout = heartbeatRPCTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	rpcStart := time.Now()
	ret, err := c.AppendEntries(ctx, encodeAppendEntriesRequest(g.parent.encodeKey(g.groupID), args))
	rpcDur := time.Since(rpcStart)
	total := time.Since(start)
	traceOutboundAppendEntries(g.groupID, target, args, connWait, rpcDur, total, err)
	if err != nil {
		return err
	}
	*resp = *decodeAppendEntriesResponse(ret)
	return nil
}

func (g *groupTransport[K]) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	c, err := g.parent.getPeer(target)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestVoteRPCTimeout)
	defer cancel()
	ret, err := c.RequestVote(ctx, encodeRequestVoteRequest(g.parent.encodeKey(g.groupID), args))
	if err != nil {
		return err
	}
	*resp = *decodeRequestVoteResponse(ret)
	return nil
}

func (g *groupTransport[K]) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	c, err := g.parent.getPeer(target)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestPreVoteRPCTimeout)
	defer cancel()
	ret, err := c.RequestPreVote(ctx, encodeRequestPreVoteRequest(g.parent.encodeKey(g.groupID), args))
	if err != nil {
		return err
	}
	*resp = *decodeRequestPreVoteResponse(ret)
	return nil
}

func (g *groupTransport[K]) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	c, err := g.parent.getPeer(target)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeoutNowRPCTimeout)
	defer cancel()
	ret, err := c.TimeoutNow(ctx, encodeTimeoutNowRequest(g.parent.encodeKey(g.groupID), args))
	if err != nil {
		return err
	}
	*resp = *decodeTimeoutNowResponse(ret)
	return nil
}

func (g *groupTransport[K]) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, req *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	c, err := g.parent.getPeer(target)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), installSnapshotRPCTimeout)
	defer cancel()
	stream, err := c.InstallSnapshot(ctx)
	if err != nil {
		return err
	}
	if err := stream.SendMsg(encodeInstallSnapshotRequest(g.parent.encodeKey(g.groupID), req)); err != nil {
		return err
	}
	var buf [16384]byte
	for {
		n, readErr := data.Read(buf[:])
		if readErr == io.EOF || (readErr == nil && n == 0) {
			break
		}
		if readErr != nil {
			return readErr
		}
		if err := stream.SendMsg(&gastrologv1.MultiRaftInstallSnapshotRequest{
			GroupId: g.parent.encodeKey(g.groupID),
			Data:    buf[:n],
		}); err != nil {
			return err
		}
	}
	rawResp := new(gastrologv1.MultiRaftInstallSnapshotResponse)
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if err := stream.RecvMsg(rawResp); err != nil {
		return err
	}
	*resp = *decodeInstallSnapshotResponse(rawResp)
	return nil
}

func (g *groupTransport[K]) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	c, err := g.parent.getPeer(target)
	if err != nil {
		return nil, err
	}
	// Pipelined AppendEntries is a long-lived stream deliberately kept
	// idle between bursts. DO NOT add a timeout here: hraft treats idle
	// pipelines as healthy and a premature timeout causes cascading
	// pipeline-reopen churn that the placement manager misreads as
	// "peer is unreliable" and reassigns vaults away. See gastrolog-5oofa.
	//
	// Pipeline lifecycle is hraft's responsibility: when it wants the
	// pipeline to go away it calls pipelineAPI.Close, which invokes the
	// cancel function stored on the pipeline and tears down the stream.
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.AppendEntriesPipeline(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	p := &pipelineAPI{
		stream:       stream,
		groupID:      g.parent.encodeKey(g.groupID),
		cancel:       cancel,
		inflightCh:   make(chan *appendFuture, 20),
		doneCh:       make(chan raft.AppendFuture, 20),
		receiverDone: make(chan struct{}),
	}
	go p.receiver()
	return p, nil
}

func (g *groupTransport[K]) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	g.state.heartbeatFuncMtx.Lock()
	g.state.heartbeatFunc = cb
	g.state.heartbeatFuncMtx.Unlock()
}

func (g *groupTransport[K]) EncodePeer(_ raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (g *groupTransport[K]) DecodePeer(p []byte) raft.ServerAddress {
	return raft.ServerAddress(p)
}

func (g *groupTransport[K]) Close() error { return nil }

func (g *groupTransport[K]) Connect(target raft.ServerAddress, _ raft.Transport) {
	_, _ = g.parent.getPeer(target)
}

func (g *groupTransport[K]) Disconnect(raft.ServerAddress) {}

func (g *groupTransport[K]) DisconnectAll() {
	_ = g.parent.Close()
}

// ---------- Pipeline ----------

type pipelineAPI struct {
	stream        grpc.ClientStream
	groupID       []byte
	cancel        func()
	inflightChMtx sync.Mutex
	inflightCh    chan *appendFuture
	doneCh        chan raft.AppendFuture
	receiverDone  chan struct{} // closed when receiver() exits
}

func (p *pipelineAPI) AppendEntries(req *raft.AppendEntriesRequest, _ *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	af := &appendFuture{
		start:   time.Now(),
		request: req,
		done:    make(chan struct{}),
	}
	if err := p.stream.SendMsg(encodeAppendEntriesRequest(p.groupID, req)); err != nil {
		return nil, err
	}
	p.inflightChMtx.Lock()
	select {
	case <-p.stream.Context().Done():
	default:
		p.inflightCh <- af
	}
	p.inflightChMtx.Unlock()
	return af, nil
}

func (p *pipelineAPI) Consumer() <-chan raft.AppendFuture { return p.doneCh }

func (p *pipelineAPI) Close() error {
	p.cancel()
	p.inflightChMtx.Lock()
	close(p.inflightCh)
	p.inflightChMtx.Unlock()
	<-p.receiverDone // wait for receiver goroutine to exit
	return nil
}

func (p *pipelineAPI) receiver() {
	defer close(p.receiverDone)
	for af := range p.inflightCh {
		msg := new(gastrologv1.MultiRaftAppendEntriesResponse)
		if err := p.stream.RecvMsg(msg); err != nil {
			af.err = err
		} else {
			af.response = *decodeAppendEntriesResponse(msg)
		}
		close(af.done)
		select {
		case p.doneCh <- af:
		case <-p.stream.Context().Done():
			return
		}
	}
}

type appendFuture struct {
	start    time.Time
	request  *raft.AppendEntriesRequest
	response raft.AppendEntriesResponse
	err      error
	done     chan struct{}
}

func (f *appendFuture) Error() error                          { <-f.done; return f.err }
func (f *appendFuture) Start() time.Time                      { return f.start }
func (f *appendFuture) Request() *raft.AppendEntriesRequest   { return f.request }
func (f *appendFuture) Response() *raft.AppendEntriesResponse { return &f.response }

// ---------- Helpers ----------

func isHeartbeat(req *raft.AppendEntriesRequest) bool {
	return req.Term != 0 && len(req.Addr) != 0 &&
		req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
		len(req.Entries) == 0 && req.LeaderCommitIndex == 0
}

// ErrUnknownGroup is returned when an RPC arrives for a group that
// isn't registered on this transport.
var ErrUnknownGroup = errors.New("unknown raft group")
