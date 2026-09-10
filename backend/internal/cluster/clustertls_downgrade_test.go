package cluster

// Cluster credentials carry node identity. A node that holds a ClusterTLS
// object but no material in it is meant to speak TLS and cannot; handing back
// plaintext there is a downgrade the operator never sees, and against a
// TLS-serving peer it fails anyway with an error that explains nothing.
// Every dial path must refuse instead.

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc/credentials/insecure"

	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/multiraft"
)

func loadedClusterTLS(t *testing.T) *ClusterTLS {
	t.Helper()
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	cert, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, LaneSANs)
	if err != nil {
		t.Fatalf("GenerateClusterCert: %v", err)
	}
	ctls := NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load: %v", err)
	}
	return ctls
}

func TestLaneCredsRefuseHandshakeWhenTLSUnloaded(t *testing.T) {
	t.Parallel()
	unloaded := NewClusterTLS()

	for _, lane := range []string{SNIServiceLane, multiraft.LaneSNI("config")} {
		creds := unloaded.TransportCredentialsForServerName(lane)

		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

		conn, _, err := creds.ClientHandshake(context.Background(), lane, client)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("lane %s: handshake succeeded with no cluster TLS material", lane)
		}
		if !errors.Is(err, ErrClusterTLSUnloaded) {
			t.Fatalf("lane %s: handshake failed with %v, want %v", lane, err, ErrClusterTLSUnloaded)
		}

		// A caller inspecting the transport must not read the unloaded
		// window as a plaintext connection.
		if got := creds.Info().SecurityProtocol; got != "tls" {
			t.Fatalf("lane %s: Info reports %q, want tls", lane, got)
		}
	}
}

func TestLaneCredsHandshakeOnceTLSLoads(t *testing.T) {
	t.Parallel()
	// The refusal is about missing material, not about the lane: the same
	// credentials object resolves normally once the state is there.
	creds := loadedClusterTLS(t).TransportCredentialsForServerName(SNIRaftLane)
	if got := creds.Info().SecurityProtocol; got != "tls" {
		t.Fatalf("Info reports %q, want tls", got)
	}
}

func TestDialTransportCredsRefuseUnloadedTLS(t *testing.T) {
	t.Parallel()
	specs := []ConnSpec{
		{PeerNodeID: "peer-1", Lane: LaneRaft, GroupID: "config"},
		{PeerNodeID: "peer-1", Lane: LaneService},
	}

	t.Run("no holder dials plaintext", func(t *testing.T) {
		m := NewPeerConnManager(PeerConnManagerConfig{NodeID: "local"})
		for _, spec := range specs {
			creds, _, err := m.dialTransportCreds(spec)
			if err != nil {
				t.Fatalf("lane %v: %v", spec.Lane, err)
			}
			if creds != insecure.NewCredentials() && creds.Info().SecurityProtocol != "insecure" {
				t.Fatalf("lane %v: want plaintext when no cluster TLS is configured, got %q",
					spec.Lane, creds.Info().SecurityProtocol)
			}
		}
	})

	t.Run("unloaded holder refuses", func(t *testing.T) {
		m := NewPeerConnManager(PeerConnManagerConfig{NodeID: "local", ClusterTLS: NewClusterTLS()})
		for _, spec := range specs {
			creds, _, err := m.dialTransportCreds(spec)
			if err == nil {
				t.Fatalf("lane %v: dial allowed with %q instead of refusing",
					spec.Lane, creds.Info().SecurityProtocol)
			}
			if !errors.Is(err, ErrClusterTLSUnloaded) {
				t.Fatalf("lane %v: refused with %v, want %v", spec.Lane, err, ErrClusterTLSUnloaded)
			}
		}
	})

	t.Run("loaded holder dials TLS", func(t *testing.T) {
		m := NewPeerConnManager(PeerConnManagerConfig{NodeID: "local", ClusterTLS: loadedClusterTLS(t)})
		for _, spec := range specs {
			creds, serverName, err := m.dialTransportCreds(spec)
			if err != nil {
				t.Fatalf("lane %v: %v", spec.Lane, err)
			}
			if got := creds.Info().SecurityProtocol; got != "tls" {
				t.Fatalf("lane %v: dialing with %q, want tls", spec.Lane, got)
			}
			if serverName == "" {
				t.Fatalf("lane %v: no SNI server name", spec.Lane)
			}
		}
	})
}

// An unloaded holder must stop the dial before a connection exists, not
// produce a plaintext one that fails later for an unrelated-looking reason.
func TestPeerDialRefusesUnloadedTLS(t *testing.T) {
	t.Parallel()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	m := NewPeerConnManager(PeerConnManagerConfig{
		NodeID:     "local",
		ClusterTLS: NewClusterTLS(),
		StaticResolve: func(id string) (string, bool) {
			return lis.Addr().String(), id == "peer-1"
		},
	})

	h, err := m.AcquireRaftPeer("peer-1", "config", "downgrade-test")
	if err == nil {
		h.Release()
		t.Fatal("acquired a raft peer connection with no cluster TLS material")
	}
	if !errors.Is(err, ErrClusterTLSUnloaded) {
		t.Fatalf("acquire failed with %v, want %v", err, ErrClusterTLSUnloaded)
	}
}

func TestJoinCredentials(t *testing.T) {
	t.Parallel()

	if creds, err := joinCredentials(nil); err != nil {
		t.Fatalf("no holder: %v", err)
	} else if creds.Info().SecurityProtocol != "insecure" {
		t.Fatalf("no holder: want plaintext, got %q", creds.Info().SecurityProtocol)
	}

	if _, err := joinCredentials(NewClusterTLS()); !errors.Is(err, ErrClusterTLSUnloaded) {
		t.Fatalf("unloaded holder: got %v, want %v", err, ErrClusterTLSUnloaded)
	}

	if creds, err := joinCredentials(loadedClusterTLS(t)); err != nil {
		t.Fatalf("loaded holder: %v", err)
	} else if creds.Info().SecurityProtocol != "tls" {
		t.Fatalf("loaded holder: want tls, got %q", creds.Info().SecurityProtocol)
	}
}
