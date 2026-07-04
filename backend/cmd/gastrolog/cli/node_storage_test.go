package cli

// Regression coverage for gastrolog-4gp8h: NodeId on the wire is the UTF-8
// GLID string ([]byte(cfg.NodeID)). add-storage decoded it as binary GLID
// bytes, never matched the existing config, and clobbered a node's storages
// instead of merging.

import (
	"testing"

	"gastrolog/internal/glid"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func TestFileStoragesForNodeMatchesWireEncoding(t *testing.T) {
	t.Parallel()
	nodeID := glid.New().String()
	fs := &v1.FileStorage{Name: "primary", Path: "/data"}
	nscs := []*v1.NodeStorageConfig{
		{NodeId: []byte(glid.New().String()), FileStorages: []*v1.FileStorage{{Name: "other"}}},
		{NodeId: []byte(nodeID), FileStorages: []*v1.FileStorage{fs}}, // wire encoding: UTF-8 GLID string
	}

	got := fileStoragesForNode(nscs, nodeID)
	if len(got) != 1 || got[0] != fs {
		t.Fatalf("fileStoragesForNode = %v, want the existing storage for %s (merge, not clobber)", got, nodeID)
	}
	if got := fileStoragesForNode(nscs, glid.New().String()); got != nil {
		t.Fatalf("unknown node = %v, want nil", got)
	}
}
