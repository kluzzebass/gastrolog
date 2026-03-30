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

// Transport multiplexes multiple Raft groups over a single gRPC service.
// Each group gets a scoped raft.Transport via GroupTransport(). All groups
// share the same peer connection pool. K is the group ID type (must be comparable).
// The encodeKey/decodeKey functions convert between K and the []byte wire format.
type Transport[K comparable] struct {
	localAddress raft.ServerAddress
	dialOptions  []grpc.DialOption
	encodeKey    func(K) []byte
	decodeKey    func([]byte) K

	mu     sync.RWMutex
	groups map[K]*groupState

	connMu sync.Mutex
	conns  map[raft.ServerAddress]*peerConn

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// groupState holds the per-group dispatch state (rpcChan + heartbeat handler).
type groupState struct {
	rpcChan          chan raft.RPC
	heartbeatFunc    func(raft.RPC)
	heartbeatFuncMtx sync.Mutex
	heartbeatTimeout time.Duration
}

// peerConn holds a shared gRPC connection and typed client for one peer.
type peerConn struct {
	clientConn *grpc.ClientConn
	client     *multiRaftClient
	mtx        sync.Mutex
}

// multiRaftClient wraps a grpc.ClientConn for manually-invoked RPCs.
// No generated gRPC stubs — the service is registered via ServiceDesc.
type multiRaftClient struct {
	cc *grpc.ClientConn
}

const servicePath = "/gastrolog.v1.MultiRaftTransportService/"

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
// encodeKey converts a group ID to bytes for the proto wire format.
// decodeKey converts bytes from the wire format back to a group ID.
func New[K comparable](localAddress raft.ServerAddress, dialOptions []grpc.DialOption, encodeKey func(K) []byte, decodeKey func([]byte) K) *Transport[K] {
	return &Transport[K]{
		localAddress: localAddress,
		dialOptions:  dialOptions,
		encodeKey:    encodeKey,
		decodeKey:    decodeKey,
		groups:       make(map[K]*groupState),
		conns:        make(map[raft.ServerAddress]*peerConn),
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
		close(gs.rpcChan)
	}
}

// getGroup returns the group state for dispatch. Returns nil if not found.
func (t *Transport[K]) getGroup(groupID K) *groupState {
	t.mu.RLock()
	gs := t.groups[groupID]
	t.mu.RUnlock()
	return gs
}

// getPeer returns or creates a connection + client for a peer.
func (t *Transport[K]) getPeer(target raft.ServerAddress) (*multiRaftClient, error) {
	t.connMu.Lock()
	pc, ok := t.conns[target]
	if !ok {
		pc = &peerConn{}
		pc.mtx.Lock()
		t.conns[target] = pc
	}
	t.connMu.Unlock()
	if ok {
		pc.mtx.Lock()
	}
	defer pc.mtx.Unlock()
	if pc.clientConn == nil {
		conn, err := grpc.NewClient("passthrough:///"+string(target), t.dialOptions...)
		if err != nil {
			return nil, err
		}
		pc.clientConn = conn
		pc.client = &multiRaftClient{cc: conn}
	}
	return pc.client, nil
}

// SetDialOptions replaces the dial options. Used by tests to inject bufconn
// dialers after construction.
func (t *Transport[K]) SetDialOptions(opts []grpc.DialOption) {
	t.connMu.Lock()
	t.dialOptions = opts
	t.connMu.Unlock()
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

	// Close all group rpcChans so consumer goroutines exit.
	t.mu.Lock()
	for k, gs := range t.groups {
		close(gs.rpcChan)
		delete(t.groups, k)
	}
	t.mu.Unlock()

	t.connMu.Lock()
	defer t.connMu.Unlock()
	for k, pc := range t.conns {
		pc.mtx.Lock()
		if pc.clientConn != nil {
			_ = pc.clientConn.Close()
		}
		pc.mtx.Unlock()
		delete(t.conns, k)
	}
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

func (g *groupTransport[K]) Consumer() <-chan raft.RPC { return g.state.rpcChan }
func (g *groupTransport[K]) LocalAddr() raft.ServerAddress { return g.parent.localAddress }

func (g *groupTransport[K]) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	c, err := g.parent.getPeer(target)
	if err != nil {
		return err
	}
	ctx := context.TODO()
	if g.state.heartbeatTimeout > 0 && isHeartbeat(args) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.state.heartbeatTimeout)
		defer cancel()
	}
	ret, err := c.AppendEntries(ctx, encodeAppendEntriesRequest(g.parent.encodeKey(g.groupID), args))
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
	ret, err := c.RequestVote(context.TODO(), encodeRequestVoteRequest(g.parent.encodeKey(g.groupID), args))
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
	ret, err := c.RequestPreVote(context.TODO(), encodeRequestPreVoteRequest(g.parent.encodeKey(g.groupID), args))
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
	ret, err := c.TimeoutNow(context.TODO(), encodeTimeoutNowRequest(g.parent.encodeKey(g.groupID), args))
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
	stream, err := c.InstallSnapshot(context.TODO())
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
	ctx, cancel := context.WithCancel(context.TODO())
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

func (g *groupTransport[K]) Disconnect(target raft.ServerAddress) {
	t := g.parent
	t.connMu.Lock()
	pc, ok := t.conns[target]
	if ok {
		delete(t.conns, target)
	}
	t.connMu.Unlock()
	if ok {
		pc.mtx.Lock()
		if pc.clientConn != nil {
			_ = pc.clientConn.Close()
		}
		pc.mtx.Unlock()
	}
}

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
