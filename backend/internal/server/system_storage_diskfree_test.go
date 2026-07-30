package server_test

// Coverage for the typeable-defaults directive (policy decision 6): the
// disk-free thresholds accept a percentage of the volume ("10%") alongside
// an absolute size, stored verbatim on the storage entity a vault's
// placements reference (not on the vault itself); and an explicit zero
// threshold is rejected like the explicit-0 budgets.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// storageConfigWithThresholds builds a one-node, one-storage
// SetNodeStorageConfigRequest with the given disk-free expressions.
func storageConfigWithThresholds(nodeID string, warn, floor string) *gastrologv1.SetNodeStorageConfigRequest {
	return &gastrologv1.SetNodeStorageConfigRequest{
		Config: &gastrologv1.NodeStorageConfig{
			NodeId: []byte(nodeID),
			FileStorages: []*gastrologv1.FileStorage{
				{
					Name:          "df-storage",
					Path:          "/data/df-storage",
					StorageClass:  1,
					DiskFreeWarn:  warn,
					DiskFreeFloor: floor,
				},
			},
		},
	}
}

// Percent and size expressions are both accepted and stored VERBATIM — the
// operator's string round-trips through store and export untouched.
func TestSetNodeStorageConfigStoresDiskFreePercentVerbatim(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, tc := range []struct{ warn, floor string }{
		{"10%", "3%"},
		{"2.5%", "1.25%"},
		{"10GB", "3GiB"},
		{"15%", "2GiB"}, // mixed forms on one storage
	} {
		nodeID := "node-" + tc.warn + tc.floor
		resp, err := client.SetNodeStorageConfig(ctx, connect.NewRequest(storageConfigWithThresholds(nodeID, tc.warn, tc.floor)))
		if err != nil {
			t.Fatalf("SetNodeStorageConfig(warn=%q floor=%q): %v", tc.warn, tc.floor, err)
		}

		stored, err := store.GetNodeStorageConfig(ctx, nodeID)
		if err != nil || stored == nil || len(stored.FileStorages) != 1 {
			t.Fatalf("GetNodeStorageConfig(%q): stored=%v err=%v", nodeID, stored, err)
		}
		if stored.FileStorages[0].DiskFreeWarn != tc.warn || stored.FileStorages[0].DiskFreeFloor != tc.floor {
			t.Fatalf("stored warn/floor = %q/%q, want %q/%q verbatim",
				stored.FileStorages[0].DiskFreeWarn, stored.FileStorages[0].DiskFreeFloor, tc.warn, tc.floor)
		}

		// The RPC echo — what `config export` serializes — is verbatim too.
		var found bool
		for _, nsc := range resp.Msg.GetSystem().GetNodeStorageConfigs() {
			if string(nsc.GetNodeId()) != nodeID {
				continue
			}
			for _, fs := range nsc.GetFileStorages() {
				if fs.GetDiskFreeWarn() != tc.warn || fs.GetDiskFreeFloor() != tc.floor {
					t.Fatalf("echoed warn/floor = %q/%q, want %q/%q verbatim",
						fs.GetDiskFreeWarn(), fs.GetDiskFreeFloor(), tc.warn, tc.floor)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("node storage config for %q not found in echoed system", nodeID)
		}
	}
}

// Unset thresholds stay empty — "inherit the node default" is the empty
// string, not a materialized value, because the default resolves per node.
func TestSetNodeStorageConfigLeavesDiskFreeUnset(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	nodeID := "node-unset-" + glid.New().String()

	if _, err := client.SetNodeStorageConfig(ctx, connect.NewRequest(storageConfigWithThresholds(nodeID, "", ""))); err != nil {
		t.Fatalf("SetNodeStorageConfig: %v", err)
	}
	stored, err := store.GetNodeStorageConfig(ctx, nodeID)
	if err != nil || stored == nil || len(stored.FileStorages) != 1 {
		t.Fatalf("GetNodeStorageConfig(%q): stored=%v err=%v", nodeID, stored, err)
	}
	if stored.FileStorages[0].DiskFreeWarn != "" || stored.FileStorages[0].DiskFreeFloor != "" {
		t.Fatalf("unset thresholds must stay empty, got %q/%q",
			stored.FileStorages[0].DiskFreeWarn, stored.FileStorages[0].DiskFreeFloor)
	}
}

// Nonsense percentages are rejected at write ingress, not at use.
func TestSetNodeStorageConfigRejectsInvalidDiskFreeExpressions(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, bad := range []string{"150%", "-5%", "%", "10%%", "max(10%, 10GiB)"} {
		nodeID := "node-bad-" + glid.New().String()
		_, err := client.SetNodeStorageConfig(ctx, connect.NewRequest(storageConfigWithThresholds(nodeID, bad, "")))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("disk-free-warn %q: want InvalidArgument, got %v", bad, err)
		}
	}
}

// An explicit zero threshold ("0", "0%") disables the guard for the storage
// and is rejected like the explicit-0 budgets.
func TestSetNodeStorageConfigRejectsZeroDiskFreeThreshold(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, zero := range []string{"0", "0%", "0GB"} {
		nodeID := "node-zero-" + glid.New().String()
		_, err := client.SetNodeStorageConfig(ctx, connect.NewRequest(storageConfigWithThresholds(nodeID, "", zero)))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("disk-free-floor %q: want InvalidArgument, got %v", zero, err)
		}
	}
}
