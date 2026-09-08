package cluster_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/multiraft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const raftLaneRequestVoteMethod = "/gastrolog.v1.MultiRaftTransportService/RequestVote"

// dialRaftLane opens a gRPC client to addr with SNI set to the config group's
// raft lane, using clientCert (nil for an anonymous dial) and caPool as roots.
func dialRaftLane(t *testing.T, addr string, clientCert []tls.Certificate, caPool *x509.CertPool) *grpc.ClientConn {
	t.Helper()
	laneSNI := multiraft.LaneSNI(cluster.ConfigGroupID)
	cfg := &tls.Config{
		Certificates: clientCert,
		ServerName:   laneSNI,
		// Node certs SAN only gastrolog-raft / gastrolog-cluster, so the
		// per-group lane SNI cannot be hostname-verified; check the cluster
		// CA chain instead, exactly as the production raft dialer does.
		InsecureSkipVerify: true, //nolint:gosec // G402: chain verified in VerifyConnection below
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("no peer certificate")
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: caPool})
			return err
		},
		MinVersion: tls.VersionTLS13,
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		t.Fatalf("dial raft lane: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// callRaftLane issues a RequestVote naming a group the lane does not serve.
// A caller that clears mTLS gets InvalidArgument from the lane's group check —
// proof the RPC reached the handler — without perturbing the node's raft state.
func callRaftLane(t *testing.T, conn *grpc.ClientConn) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &gastrologv1.MultiRaftRequestVoteRequest{GroupId: []byte("not-the-lane-group")}
	return conn.Invoke(ctx, raftLaneRequestVoteMethod, req, &gastrologv1.MultiRaftRequestVoteResponse{})
}

// TestRaftLaneRejectsUncredentialedDial proves the per-group raft lanes
// authenticate their callers. hashicorp/raft applies whatever its transport
// hands it, so a lane that answered an anonymous dial would let anyone with TCP
// reach commit entries to the replicated FSM.
func TestRaftLaneRejectsUncredentialedDial(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TLS raft lane test: starts a raft node")
	}

	ctls := sharedTestClusterTLS(t)
	node := newTLSClusterNode(t, "node-1", ctls, true)
	t.Cleanup(node.close)
	caPool := ctls.State().CAPool

	// Premise: with a cluster certificate the lane is reachable, so a rejection
	// below is about the missing credential and not an unserved lane.
	t.Run("cluster certificate reaches the lane handler", func(t *testing.T) {
		conn := dialRaftLane(t, node.srv.Addr(), []tls.Certificate{ctls.State().Cert}, caPool)
		err := callRaftLane(t, conn)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("authenticated raft lane call = %v, want InvalidArgument from the lane group check", err)
		}
	})

	t.Run("no client certificate is rejected", func(t *testing.T) {
		conn := dialRaftLane(t, node.srv.Addr(), nil, caPool)
		err := callRaftLane(t, conn)
		if err == nil {
			t.Fatal("anonymous raft lane call succeeded; the lane accepts unauthenticated consensus RPCs")
		}
		if status.Code(err) == codes.InvalidArgument {
			t.Fatalf("anonymous raft lane call reached the handler: %v", err)
		}
	})

	t.Run("certificate from a foreign CA is rejected", func(t *testing.T) {
		foreignCA, err := tlsutil.GenerateCA()
		if err != nil {
			t.Fatalf("GenerateCA: %v", err)
		}
		foreignCert, err := tlsutil.GenerateClusterCert(foreignCA.CertPEM, foreignCA.KeyPEM, cluster.LaneSANs)
		if err != nil {
			t.Fatalf("GenerateClusterCert: %v", err)
		}
		pair, err := tls.X509KeyPair(foreignCert.CertPEM, foreignCert.KeyPEM)
		if err != nil {
			t.Fatalf("X509KeyPair: %v", err)
		}
		conn := dialRaftLane(t, node.srv.Addr(), []tls.Certificate{pair}, caPool)
		if err := callRaftLane(t, conn); err == nil {
			t.Fatal("raft lane call with a foreign-CA certificate succeeded")
		} else if status.Code(err) == codes.InvalidArgument {
			t.Fatalf("foreign-CA raft lane call reached the handler: %v", err)
		}
	})
}

// TestServiceLaneStaysReachableWithoutClientCert pins the exemption that keeps
// the raft-lane tightening from locking new nodes out: a node being enrolled
// has no cluster certificate yet, so Enroll must answer an anonymous dial while
// every other service RPC on the same port must not.
func TestServiceLaneStaysReachableWithoutClientCert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TLS service lane test: starts a raft node")
	}

	ctls := sharedTestClusterTLS(t)
	node := newTLSClusterNode(t, "node-1", ctls, true)
	t.Cleanup(node.close)

	enrolled := make(chan struct{}, 1)
	node.srv.SetEnrollHandler(func(context.Context, *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error) {
		enrolled <- struct{}{}
		return &gastrologv1.EnrollResponse{CaCertPem: []byte("ca")}, nil
	})

	// A joining node has no cluster cert and no CA to verify against — it pins
	// the CA fingerprint from its join token instead. Certificates is empty.
	conn, err := grpc.NewClient(node.srv.Addr(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: models the enrollment TOFU dial, which has no CA yet
		MinVersion:         tls.VersionTLS13,
	})))
	if err != nil {
		t.Fatalf("dial service lane: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &gastrologv1.EnrollResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Enroll",
		&gastrologv1.EnrollRequest{NodeId: []byte("node-2")}, resp); err != nil {
		t.Fatalf("Enroll without a client certificate: %v", err)
	}
	select {
	case <-enrolled:
	default:
		t.Fatal("Enroll did not reach the handler")
	}

	// Same connection, non-exempt method: the interceptor must still refuse it.
	err = conn.Invoke(ctx, "/gastrolog.v1.ClusterService/Broadcast",
		&gastrologv1.BroadcastRequest{}, &gastrologv1.BroadcastResponse{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("uncredentialed Broadcast = %v, want Unauthenticated", err)
	}
}
