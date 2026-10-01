package cluster

// A cluster-TLS change that moved the CA and one that did not call for
// different answers, and telling them apart is the whole job of HasCA: a node
// that reissued on every change would mint a fresh certificate on each
// startup replay and would end up naming itself rather than wearing the name
// enrolment gave it.

import (
	"testing"

	"gastrolog/internal/cluster/tlsutil"
)

func TestHasCA(t *testing.T) {
	t.Parallel()

	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	other, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, "node-1", LaneSANs)
	if err != nil {
		t.Fatal(err)
	}
	ctls := NewClusterTLS()
	if err := ctls.Load(pair.CertPEM, pair.KeyPEM, ca.CertPEM); err != nil {
		t.Fatal(err)
	}

	if !ctls.HasCA(ca.CertPEM) {
		t.Fatal("the loaded CA was not recognised; every config change would reissue this node's certificate")
	}
	if ctls.HasCA(other.CertPEM) {
		t.Fatal("a different CA was taken for the loaded one; a rotation would leave this node on a certificate peers no longer trust")
	}
	if ctls.HasCA(nil) {
		t.Fatal("garbage was taken for the loaded CA")
	}
	if NewClusterTLS().HasCA(ca.CertPEM) {
		t.Fatal("an unloaded holder claimed a CA it does not have")
	}
}
