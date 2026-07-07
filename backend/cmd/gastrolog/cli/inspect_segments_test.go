package cli

import (
	"bytes"
	"io"
	"os"
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func TestPrintVaultSegmentStaging(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	printVaultSegmentStaging("logs", "vault-id", &v1.VaultPipelineBacklog{
		EligibleSegments:         2,
		RegistrySegments:         3,
		RegistryRecords:          100,
		WorkingSegments:          1,
		CompletedStagingSegments: 0,
		HeadSegments:             4,
		PreHeadSegments:          0,
		WorkingBytes:             1024,
		HeadBytes:                4096,
		NodeSegments: []*v1.PipelineNodeSegments{
			{
				NodeId:          []byte("node-a"),
				WorkingSegments: 1,
				HeadSegments:    2,
				WorkingBytes:    512,
				HeadBytes:       2048,
			},
		},
	}, map[string]string{"node-a": "node-1"})

	_ = w.Close()
	os.Stdout = old
	_, _ = io.Copy(&buf, r)

	out := buf.String()
	for _, want := range []string{
		"Vault: logs (vault-id)",
		"Vault-ctl registry: 2 eligible / 3 published",
		"Cluster segment staging",
		"working",
		"head",
		"node-1",
	} {
		if !bytes.Contains([]byte(out), []byte(want)) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestLeaderName(t *testing.T) {
	t.Parallel()
	names := map[string]string{"abc": "node-1"}
	if got := leaderName("abc", names); got != "node-1" {
		t.Fatalf("leaderName = %q", got)
	}
	if got := leaderName("missing", names); got != "missing" {
		t.Fatalf("leaderName fallback = %q", got)
	}
}
