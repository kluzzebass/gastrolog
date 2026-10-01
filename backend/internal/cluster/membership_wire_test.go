package cluster

// The unit tests above exercise the handler. These drive the whole call over
// a real mTLS cluster listener, because the two things the design rests on —
// that any member serves a join, and that reaching the configuration takes
// the certificate rather than the token — both live in the transport.

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster/tlsutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// startMember stands up a cluster listener with the same TLS and
// interceptors a real node serves with, answering RequestMembership from h.
// It returns the address to dial and the TLS a joiner would hold after
// enrolling as joinerID: a certificate naming that node, chained to the
// member's CA — which is exactly what enrollment issues.
func startMember(t *testing.T, h MembershipHandler, joinerID string) (addr string, joinerTLS *ClusterTLS) {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	memberTLS := nodeTLSNamed(t, ca, "member-node")
	srv := &Server{cfg: Config{TLS: memberTLS}, membershipHandler: h}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer(srv.baseServerOpts(maxChunkTransferBytes)...)
	gsrv.RegisterService(&clusterServiceDesc, srv)
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(func() {
		gsrv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String(), nodeTLSNamed(t, ca, joinerID)
}

// nodeTLSNamed issues a certificate naming nodeID from the given CA — the
// per-node material enrollment hands out.
func nodeTLSNamed(t *testing.T, ca tlsutil.CAKeyPair, nodeID string) *ClusterTLS {
	t.Helper()
	cert, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, nodeID, LaneSANs)
	if err != nil {
		t.Fatalf("GenerateNodeCert(%s): %v", nodeID, err)
	}
	ctls := NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load TLS for %s: %v", nodeID, err)
	}
	return ctls
}

// The joiner was handed one address and has no idea which node leads. Whether
// this member leads is settled behind the RPC, so the call has to succeed
// without the joiner asking.
func TestJoinClusterSucceedsAgainstAMemberThatDoesNotLead(t *testing.T) {
	t.Parallel()

	var served atomic.Bool
	addr, joinerTLS := startMember(t, func(_ context.Context, nodeID, nodeAddr string, voter bool) error {
		if nodeID != "node-2" || nodeAddr != "node-2:4566" || voter {
			t.Errorf("request arrived altered: id=%q addr=%q voter=%v", nodeID, nodeAddr, voter)
		}
		served.Store(true)
		return nil
	}, "node-2")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := JoinCluster(ctx, nil, addr, "node-2", "node-2:4566", joinerTLS, false); err != nil {
		t.Fatalf("a member refused to serve the join: %v", err)
	}
	if !served.Load() {
		t.Fatal("the request never reached the cluster")
	}
}

// A cluster mid-election cannot place the change yet. The joiner has to keep
// asking rather than fail startup, because the condition ends on its own.
func TestJoinClusterWaitsOutAnElection(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	addr, joinerTLS := startMember(t, func(context.Context, string, string, bool) error {
		if attempts.Add(1) < 3 {
			return ErrNoLeader
		}
		return nil
	}, "node-2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := JoinCluster(ctx, nil, addr, "node-2", "node-2:4566", joinerTLS, false); err != nil {
		t.Fatalf("the joiner gave up during an election: %v", err)
	}
	if got := attempts.Load(); got < 3 {
		t.Fatalf("the joiner asked %d times; it should have retried until the cluster could answer", got)
	}
}

// The joiner must not hammer a cluster that has refused it. A wrong request
// is wrong however many times it is sent.
func TestJoinClusterStopsOnARefusal(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	addr, joinerTLS := startMember(t, func(context.Context, string, string, bool) error {
		attempts.Add(1)
		return errors.New("node id already in the configuration at another address")
	}, "node-2")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := JoinCluster(ctx, nil, addr, "node-2", "node-2:4566", joinerTLS, false); err == nil {
		t.Fatal("a refused join was reported as success")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("the joiner retried a refusal %d times", got)
	}
}

// Holding the join token is not holding the certificate. Enrolment issues
// one precisely so that this call can demand it: a caller that watched an
// enrolment, or read a token out of a log, gets no further than the
// handshake.
func TestRequestMembershipRefusesACallerWithoutTheClusterCertificate(t *testing.T) {
	t.Parallel()

	addr, ctls := startMember(t, func(context.Context, string, string, bool) error {
		t.Error("a caller with no cluster certificate reached the configuration")
		return nil
	}, "attacker")

	// Trusts the cluster CA, as a token holder does, but presents nothing.
	caOnly := ctls.ClientTLSConfig().Clone()
	caOnly.Certificates = nil
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(caOnly)))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/gastrolog.v1.ClusterService/RequestMembership",
		&gastrologv1.RequestMembershipRequest{NodeId: []byte("attacker"), NodeAddr: "attacker:4566"},
		&gastrologv1.RequestMembershipResponse{})
	if err == nil {
		t.Fatal("the configuration was changed by a caller holding no cluster certificate")
	}
}

// And the joiner has to be able to get that certificate in the first place,
// so Enrol stays reachable without one. Asserting the refusal above without
// this would pass just as well against a listener that refuses everyone.
func TestEnrollStaysReachableWithoutAClusterCertificate(t *testing.T) {
	t.Parallel()

	addr, _ := startMember(t, nil, "node-2")

	// A joiner has no CA to verify against before it parses the token, so it
	// connects without verification and pins afterwards; skipping straight to
	// the RPC is enough to show the interceptor let it through.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecureTLSCreds()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Enroll",
		&gastrologv1.EnrollRequest{TokenSecret: "whatever"}, &gastrologv1.EnrollResponse{})
	// No enrol handler is registered, so Unavailable is the answer from the
	// handler — which is only reached if the certificate gate let it past.
	if status.Code(err) == codes.Unauthenticated {
		t.Fatal("enrolment demands the certificate it exists to hand out")
	}
}

// insecureTLSCreds mirrors how a joiner connects before it has anything to
// verify against: the connection is unverified at the TLS layer and the real
// enrolment client pins the certificate itself afterwards.
func insecureTLSCreds() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // G402: a joiner has no CA yet; the enrolment client pins the presented chain instead
}

// A certificate is authority to ask membership for the node it names and no
// other. Without the binding, any certificate holder could name a current
// member's ID at its own address, and the add would re-address the victim —
// its traffic routed to the caller. Bound, the only node that can move an
// identity is the one holding its key, which is also what lets a node
// legitimately come back on a new address.
func TestRequestMembershipRefusesAnIDTheCertificateDoesNotName(t *testing.T) {
	t.Parallel()

	addr, joinerTLS := startMember(t, func(_ context.Context, nodeID, _ string, _ bool) error {
		t.Errorf("a caller certified as node-2 changed membership for %q", nodeID)
		return nil
	}, "node-2")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := JoinCluster(ctx, nil, addr, "node-victim", "attacker:4566", joinerTLS, true)
	if err == nil {
		t.Fatal("membership for another node's ID was accepted")
	}
	if status.Code(errors.Unwrap(err)) != codes.PermissionDenied && status.Code(err) != codes.PermissionDenied {
		// JoinCluster wraps; accept either shape but insist on the code.
		t.Fatalf("refusal carries code %v (%v), want PermissionDenied", status.Code(err), err)
	}
}
