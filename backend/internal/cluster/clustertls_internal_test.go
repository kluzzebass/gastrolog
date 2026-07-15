package cluster

import (
	"testing"

	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/multiraft"
)

func TestRaftLanePerGroupSNIClientTLS(t *testing.T) {
	t.Parallel()

	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	cert, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, nil)
	if err != nil {
		t.Fatalf("GenerateClusterCert: %v", err)
	}

	ctls := NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load: %v", err)
	}

	perGroup := multiraft.LaneSNI(multiraft.ClusterConfigGroupID)
	cfg := ctls.clientTLSConfigForServerName(perGroup)
	if cfg == nil {
		t.Fatal("expected non-nil client TLS config")
	}
	if cfg.ServerName != perGroup {
		t.Fatalf("ServerName = %q, want %q", cfg.ServerName, perGroup)
	}
	if cfg.VerifyConnection == nil {
		t.Fatal("expected CA-only VerifyConnection for raft lane SNI")
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify for per-group raft lane SNI (hostname not in cert SAN)")
	}
}
