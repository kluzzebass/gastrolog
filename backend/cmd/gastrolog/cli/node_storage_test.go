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

// TestNodeAddStorageCmdDiskFreeFlags pins gastrolog-9akebz: disk-free-warn
// and disk-free-floor moved off `vault create/update` onto the storage
// surface (`node add-storage`) — this is the CLI's answer to "where do I
// set the thresholds now." Registration-level check (no live client): both
// flags exist, default empty (inherit the node default), and round-trip
// through Set/GetString the way the RunE closure reads them.
func TestNodeAddStorageCmdDiskFreeFlags(t *testing.T) {
	t.Parallel()
	cmd := newNodeAddStorageCmd()

	for _, name := range []string{"disk-free-warn", "disk-free-floor"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Fatalf("flag %q not registered on node add-storage", name)
		}
		if f.DefValue != "" {
			t.Errorf("flag %q default = %q, want empty (inherits node default)", name, f.DefValue)
		}
	}

	if err := cmd.Flags().Set("disk-free-warn", "10%"); err != nil {
		t.Fatalf("set disk-free-warn: %v", err)
	}
	if err := cmd.Flags().Set("disk-free-floor", "3GB"); err != nil {
		t.Fatalf("set disk-free-floor: %v", err)
	}
	warn, _ := cmd.Flags().GetString("disk-free-warn")
	floor, _ := cmd.Flags().GetString("disk-free-floor")
	if warn != "10%" {
		t.Errorf("disk-free-warn = %q, want %q", warn, "10%")
	}
	if floor != "3GB" {
		t.Errorf("disk-free-floor = %q, want %q", floor, "3GB")
	}
}

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
