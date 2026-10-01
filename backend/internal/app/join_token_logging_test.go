package app

// GastroLog ingests its own logs. A cluster credential written to one does not
// scroll away: the self ingester stores it durably and makes it searchable by
// anyone who can read a vault, long after the boot that printed it. The key
// the cluster mints join tokens with is the durable credential, so it must not
// reach a log line, and that is worth asserting rather than remembering.

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	sysmem "gastrolog/internal/system/memory"
)

// bootstrapWithCapturedLog bootstraps cluster TLS and returns everything the
// node logged while doing it, plus the minting key it generated and the CA
// hash that identifies the cluster.
func bootstrapWithCapturedLog(t *testing.T) (logged, tokenKey, caHash string) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	store := sysmem.NewStore()
	ctls := cluster.NewClusterTLS()
	if err := bootstrapClusterTLS(context.Background(), store, ctls, "node-1",
		filepath.Join(t.TempDir(), "cluster-tls.json"), logger); err != nil {
		t.Fatal(err)
	}

	sys, err := store.Load(context.Background())
	if err != nil || sys == nil || sys.Runtime.ClusterTLS == nil {
		t.Fatalf("cluster TLS was not stored: %v", err)
	}
	tls := sys.Runtime.ClusterTLS
	return buf.String(), tls.JoinTokenKey, caHashOf([]byte(tls.CACertPEM))
}

func TestBootstrapDoesNotLogTheJoinTokenKey(t *testing.T) {
	t.Parallel()
	logged, tokenKey, caHash := bootstrapWithCapturedLog(t)

	if tokenKey == "" {
		t.Fatal("bootstrap stored no minting key, so this asserts nothing")
	}
	if strings.Contains(logged, tokenKey) {
		t.Fatal("the key that mints join tokens was logged; every token it will ever sign is compromised")
	}

	// The CA hash is public — derivable from any handshake — so logging it
	// costs nothing and lets an operator confirm which cluster a node is in.
	if !strings.Contains(logged, caHash) {
		t.Fatal("nothing in the log identifies the cluster; the CA hash is safe to log and worth logging")
	}
}

// Nor may a bootstrapping node log a usable token. Nothing mints one here
// any more, but a future change that did would put a live credential in the
// log store, so the shape is asserted rather than assumed.
func TestBootstrapLogsNoUsableToken(t *testing.T) {
	t.Parallel()
	logged, _, _ := bootstrapWithCapturedLog(t)

	for _, line := range strings.Split(logged, "\n") {
		for _, field := range strings.Fields(line) {
			value := strings.TrimPrefix(field, "msg=")
			credential, _, err := tlsutil.ParseJoinToken(strings.Trim(value, `"`))
			if err == nil && strings.Contains(credential, ".") {
				t.Fatalf("a line looks like a join token: %q", field)
			}
		}
	}
}

// Removing the credential from the log must not leave an operator with no way
// to get one, so the line that replaced it says where to go.
func TestBootstrapSaysHowToGetAToken(t *testing.T) {
	t.Parallel()
	logged, _, _ := bootstrapWithCapturedLog(t)

	if !strings.Contains(logged, "cluster join-token") {
		t.Fatal("the log does not mention how to mint a token; an operator who used to read one here has nowhere to go")
	}
}
