package cluster

import (
	"context"
	"crypto/x509"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"
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

// SetRefusedAsRemovedObserver registers what runs when a peer this node
// dialed as a member refuses it as removed. It is called with the refusing
// peer's node ID, possibly many times.
func (s *Server) SetRefusedAsRemovedObserver(fn func(byPeer string)) {
	s.refusedAsRemoved.Store(&fn)
}

func (s *Server) observeRefusedAsRemoved(byPeer string) {
	if fn := s.refusedAsRemoved.Load(); fn != nil {
		(*fn)(byPeer)
	}
}

// observeRemovalUnary watches calls to one peer for that peer refusing this
// node as removed. The refusal counts only from the peer this node meant to
// reach: it was dialled because this node's configuration lists it, and its
// certificate must name it — the same standing NotifyEviction's caller needs.
func (m *PeerConnManager) observeRemovalUnary(peerID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var p peer.Peer
		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Peer(&p))...)
		m.observeRefusal(err, peerID, &p)
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
