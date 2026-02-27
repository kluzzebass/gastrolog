package cluster_test

import (
	"testing"

	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
)

func TestClusterTLSLoadAndState(t *testing.T) {
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	cert, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, nil)
	if err != nil {
		t.Fatalf("GenerateClusterCert: %v", err)
	}

	ctls := cluster.NewClusterTLS()

	// State should be nil before Load.
	if ctls.State() != nil {
		t.Fatal("expected nil state before Load")
	}

	// Load should succeed.
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// State should be non-nil after Load.
	st := ctls.State()
	if st == nil {
		t.Fatal("expected non-nil state after Load")
	}
	if st.CACert == nil {
		t.Error("expected non-nil CACert")
	}
	if st.CAPool == nil {
		t.Error("expected non-nil CAPool")
	}
}

func TestClusterTLSServerConfig(t *testing.T) {
	ca, _ := tlsutil.GenerateCA()
	cert, _ := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, nil)

	ctls := cluster.NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load: %v", err)
	}

	serverCfg := ctls.ServerTLSConfig()
	if serverCfg == nil {
		t.Fatal("expected non-nil ServerTLSConfig")
	}
	if serverCfg.GetCertificate == nil {
		t.Error("expected GetCertificate callback")
	}
	if serverCfg.GetConfigForClient == nil {
		t.Error("expected GetConfigForClient callback")
	}
}

func TestClusterTLSClientConfig(t *testing.T) {
	ca, _ := tlsutil.GenerateCA()
	cert, _ := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, nil)

	ctls := cluster.NewClusterTLS()
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("Load: %v", err)
	}

	clientCfg := ctls.ClientTLSConfig()
	if clientCfg == nil {
		t.Fatal("expected non-nil ClientTLSConfig")
	}
	if len(clientCfg.Certificates) != 1 {
		t.Errorf("expected 1 certificate, got %d", len(clientCfg.Certificates))
	}
	if clientCfg.RootCAs == nil {
		t.Error("expected non-nil RootCAs")
	}
}

func TestClusterTLSAtomicReload(t *testing.T) {
	ca1, _ := tlsutil.GenerateCA()
	cert1, _ := tlsutil.GenerateClusterCert(ca1.CertPEM, ca1.KeyPEM, nil)

	ca2, _ := tlsutil.GenerateCA()
	cert2, _ := tlsutil.GenerateClusterCert(ca2.CertPEM, ca2.KeyPEM, nil)

	ctls := cluster.NewClusterTLS()

	// Load first cert.
	if err := ctls.Load(cert1.CertPEM, cert1.KeyPEM, ca1.CertPEM); err != nil {
		t.Fatalf("Load 1: %v", err)
	}
	state1 := ctls.State()

	// Reload with second cert.
	if err := ctls.Load(cert2.CertPEM, cert2.KeyPEM, ca2.CertPEM); err != nil {
		t.Fatalf("Load 2: %v", err)
	}
	state2 := ctls.State()

	// States should be different (different CAs).
	if state1.CACert.Equal(state2.CACert) {
		t.Error("expected different CA certs after reload")
	}
}

func TestClusterTLSNilState(t *testing.T) {
	ctls := cluster.NewClusterTLS()

	// ClientTLSConfig should return nil when state is nil.
	if ctls.ClientTLSConfig() != nil {
		t.Error("expected nil ClientTLSConfig when state is nil")
	}
}
