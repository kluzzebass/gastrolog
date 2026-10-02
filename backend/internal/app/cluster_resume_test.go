package app

// Containerized deployments pass the join flags on every boot, so the flags
// alone cannot distinguish a first-boot joiner from an existing member
// restarting. A node with enrolled TLS material on disk must resume with it —
// re-enrolling presents an ID the cluster already has, the admission guard
// rightly refuses it, and the node crash-loops on every restart.

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/home"
)

func TestRestartedJoinerResumesInsteadOfReEnrolling(t *testing.T) {
	t.Parallel()

	hd := home.New(t.TempDir())
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	crt, err := tlsutil.GenerateNodeCert(ca.CertPEM, ca.KeyPEM, "node-restart", cluster.LaneSANs)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.SaveFile(hd.ClusterTLSPath(), crt.CertPEM, crt.KeyPEM, ca.CertPEM); err != nil {
		t.Fatalf("SaveFile: %v", err)
	}

	// Join flags set AND material on disk: setupCluster must load the local
	// material and never dial the (dead) join address. An enrollment attempt
	// fails against this address, so a failure here IS the re-enroll bug.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := RunConfig{
		ConfigType:  "raft",
		ClusterAddr: "127.0.0.1:0",
		JoinAddr:    "127.0.0.1:1", // nothing listens here; a dial fails fast
		JoinToken:   "0.deadbeef:deadbeef",
	}
	logger, _ := testLogger()
	srv, clusterTLS, err := setupCluster(ctx, logger, cfg, hd, "node-restart")
	if err != nil {
		t.Fatalf("setupCluster on restart with join flags set: %v", err)
	}
	t.Cleanup(srv.Stop)

	if clusterTLS.State() == nil {
		t.Fatal("restart did not load the enrolled TLS material from disk")
	}
}
