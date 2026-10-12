package cluster

import (
	"context"
	"crypto/x509"
	"fmt"
	"slices"
	"strings"
	"sync"

	hraft "github.com/hashicorp/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/logging"
)

// removedRefusal is the status a member answers a removed node with. The
// detail names the removed node, so the node can tell its removal apart
// from any other refusal — including when it was down while the leader
// would have told it.
func removedRefusal(nodeID, message string) error {
	st, err := status.New(codes.PermissionDenied, message).WithDetails(&gastrologv1.RemovedNodeRefusal{NodeId: nodeID})
	if err != nil {
		return status.Error(codes.PermissionDenied, message)
	}
	return st.Err()
}

// RefusedAsRemoved reports whether err is a member refusing nodeID because
// the cluster removed it.
func RefusedAsRemoved(err error, nodeID string) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		return false
	}
	for _, d := range st.Details() {
		if r, ok := d.(*gastrologv1.RemovedNodeRefusal); ok && r.GetNodeId() == nodeID {
			return true
		}
	}
	return false
}

// SetRefusedAsRemovedObserver registers what runs when a current member
// refuses this node as removed. It is called with the refusing member's
// node ID, possibly many times.
func (s *Server) SetRefusedAsRemovedObserver(fn func(byPeer string)) {
	s.refusedAsRemoved.Store(&fn)
}

// observeRefusedAsRemoved acts on a refusal that named this node and came
// from the peer it dialled. A peer's word has standing only while this
// node's own configuration lists it and its own removal record does not: a
// node the cluster removed — which still answers dials made to it, the
// eviction notice among them — is in no position to say who else was
// removed. And no single member's word is enough: one compromised member,
// or a just-removed one this node has not yet seen go, would otherwise take
// out every node that dials it. Membership ends once a majority of the other
// voters in this node's configuration have refused it, each counted once.
//
// A claimant that keeps refusing is weighed again only once this node's
// Raft log has moved: until then neither the configuration nor the removal
// record can have changed, so the answer cannot either. Raft traffic is
// frequent, and one claimant answering all of it must not cost a
// configuration read and a log line per call.
func (s *Server) observeRefusedAsRemoved(byPeer string) {
	logIndex := s.lastLogIndex()
	if s.removalClaims.counted(byPeer, logIndex) {
		return
	}
	conf, ok := s.localConfiguration()
	if !ok || !refusalEndsMembership(conf.ids(), byPeer, s.isRemoved) {
		if _, logged := s.ignoredRemovalClaims.LoadOrStore(byPeer, struct{}{}); !logged {
			logging.Default(s.logger).Warn("ignoring a claim that this node was removed: the claimant is not a current member",
				"claimant_node_id", byPeer)
		}
		return
	}
	others := conf.otherVoters(s.cfg.NodeID)
	claimants, reached, first := s.removalClaims.add(conf.fingerprint(), logIndex, byPeer, others, s.isRemoved)
	if !reached {
		if first {
			logging.Default(s.logger).Warn("a member refuses this node as removed; waiting for a majority of the other voters before taking it",
				"claimant_node_id", byPeer, "claimants", claimants, "other_voters", len(others))
		}
		return
	}
	if fn := s.refusedAsRemoved.Load(); fn != nil {
		(*fn)(byPeer)
	}
}

func (s *Server) lastLogIndex() uint64 {
	if s.raft == nil {
		return 0
	}
	return s.raft.LastIndex()
}

// removalClaims tallies the distinct members that refused this node as
// removed. The tally belongs to one configuration: when the configuration
// changes, who counts and how many are needed change with it, so it starts
// over.
type removalClaims struct {
	mu        sync.Mutex
	config    string
	logIndex  uint64
	claimants map[string]struct{}
}

// counted reports whether byPeer's claim is already in the tally and the
// Raft log has not moved since the tally was last weighed.
func (c *removalClaims) counted(byPeer string, logIndex uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.claimants[byPeer]
	return ok && logIndex != 0 && logIndex == c.logIndex
}

// add records byPeer's claim under the configuration config, weighed at
// logIndex, and reports how many of otherVoters have claimed, whether that
// is a majority of them, and whether byPeer is new to the tally. Claimants
// the removal record now names are dropped: they had standing when they
// claimed, and have none now.
func (c *removalClaims) add(config string, logIndex uint64, byPeer string, otherVoters []string, removed func(string) bool) (int, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claimants == nil || c.config != config {
		c.config, c.claimants = config, map[string]struct{}{}
	}
	c.logIndex = logIndex
	for claimant := range c.claimants {
		if removed(claimant) {
			delete(c.claimants, claimant)
		}
	}
	_, known := c.claimants[byPeer]
	c.claimants[byPeer] = struct{}{}
	return claimsFromVoters(c.claimants, otherVoters), claimsReachMajority(c.claimants, otherVoters), !known
}

func claimsFromVoters(claimants map[string]struct{}, otherVoters []string) int {
	n := 0
	for _, v := range otherVoters {
		if _, ok := claimants[v]; ok {
			n++
		}
	}
	return n
}

// claimsReachMajority reports whether claimants include more than half of
// otherVoters. A node with no other voter can never be told it was removed
// this way: there is nobody whose word could outweigh another's.
func claimsReachMajority(claimants map[string]struct{}, otherVoters []string) bool {
	if len(otherVoters) == 0 {
		return false
	}
	return claimsFromVoters(claimants, otherVoters) > len(otherVoters)/2
}

// localConfiguration is the Raft configuration this node holds.
type localConfiguration struct {
	servers []hraft.Server
}

func (c localConfiguration) ids() []string {
	ids := make([]string, 0, len(c.servers))
	for _, srv := range c.servers {
		ids = append(ids, string(srv.ID))
	}
	return ids
}

func (c localConfiguration) otherVoters(self string) []string {
	var voters []string
	for _, srv := range c.servers {
		if srv.Suffrage == hraft.Voter && string(srv.ID) != self {
			voters = append(voters, string(srv.ID))
		}
	}
	return voters
}

// fingerprint identifies the configuration by who is in it and with what
// suffrage — everything the tally depends on. hashicorp/raft's
// GetConfiguration does not report the index a configuration was set at.
func (c localConfiguration) fingerprint() string {
	members := make([]string, 0, len(c.servers))
	for _, srv := range c.servers {
		members = append(members, fmt.Sprintf("%s:%d", srv.ID, srv.Suffrage))
	}
	slices.Sort(members)
	return strings.Join(members, ",")
}

func (s *Server) localConfiguration() (localConfiguration, bool) {
	if s.raft == nil {
		return localConfiguration{}, false
	}
	s.configReads.Add(1)
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return localConfiguration{}, false
	}
	return localConfiguration{servers: future.Configuration().Servers}, true
}

// refusalEndsMembership decides whether byPeer's refusal of this node as
// removed ends its membership: only a node in this node's configuration
// that this node does not know to be removed itself has that standing.
func refusalEndsMembership(configured []string, byPeer string, removed func(string) bool) bool {
	if byPeer == "" || !slices.Contains(configured, byPeer) {
		return false
	}
	return !removed(byPeer)
}

// configuredIDs is the node IDs in this node's Raft configuration; nil when
// it cannot be read.
func (s *Server) configuredIDs() []string {
	servers, err := s.Servers()
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(servers))
	for _, srv := range servers {
		ids = append(ids, srv.ID)
	}
	return ids
}

// notifyEvictionMethod is the eviction notice. A refusal to it is never word
// of this node's own removal: the node it goes to is the one being removed.
const notifyEvictionMethod = "/gastrolog.v1.ClusterService/NotifyEviction"

// observeRemovalUnary watches calls to one peer for that peer refusing this
// node as removed. The refusal reaches the Server's check only from the peer
// this node meant to reach, as its certificate proves, and never from the
// eviction notice.
func (m *PeerConnManager) observeRemovalUnary(peerID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var p peer.Peer
		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Peer(&p))...)
		if method != notifyEvictionMethod {
			m.observeRefusal(err, peerID, &p)
		}
		return err
	}
}

func (m *PeerConnManager) observeRemovalStream(peerID string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		p := &peer.Peer{}
		cs, err := streamer(ctx, desc, cc, method, append(opts, grpc.Peer(p))...)
		if err != nil {
			m.observeRefusal(err, peerID, p)
			return nil, err
		}
		return &removalObservingStream{ClientStream: cs, observe: func(err error) { m.observeRefusal(err, peerID, p) }}, nil
	}
}

type removalObservingStream struct {
	grpc.ClientStream
	observe func(error)
}

func (s *removalObservingStream) RecvMsg(msg any) error {
	err := s.ClientStream.RecvMsg(msg)
	if err != nil {
		s.observe(err)
	}
	return err
}

func (m *PeerConnManager) observeRefusal(err error, peerID string, p *peer.Peer) {
	if m.onRefusedAsRemoved == nil || !RefusedAsRemoved(err, m.nodeID) {
		return
	}
	if leaf := peerLeaf(p); leaf == nil || tlsutil.NodeIDFromCert(leaf) != peerID {
		return
	}
	m.onRefusedAsRemoved(peerID)
}

// peerLeaf returns the certificate the remote end of a finished call
// presented. Outbound cluster handshakes complete only once it chains to the
// cluster CA, on every lane.
func peerLeaf(p *peer.Peer) *x509.Certificate {
	if p == nil {
		return nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil
	}
	return tlsInfo.State.PeerCertificates[0]
}
