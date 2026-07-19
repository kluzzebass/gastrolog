package server_test

// Coverage for the retention transfer disposition validation matrix
// (gastrolog-2l918): disposition "transfer" requires a target vault ID,
// the target must not be the source (self-transfer is the cascade
// footgun), and both source and target must be file vaults — cloud and
// memory vaults have different at-rest forms and lifecycle machinery.
// See docs/retention-transfer-disposition-design.md.

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/glid"
)

func fileVaultConfig(id glid.GLID, name string) *gastrologv1.VaultConfig {
	return &gastrologv1.VaultConfig{
		Id:      id.Bytes(),
		Name:    name,
		Enabled: true,
		Type:    gastrologv1.VaultType_VAULT_TYPE_FILE,
	}
}

func putVault(t *testing.T, client gastrologv1connect.SystemServiceClient, cfg *gastrologv1.VaultConfig) error {
	t.Helper()
	_, err := client.PutVault(context.Background(), connect.NewRequest(&gastrologv1.PutVaultRequest{Config: cfg}))
	return err
}

// TestPutVaultTransferRequiresTarget rejects disposition="transfer" with no
// target vault set — a defaults-must-be-typeable violation waiting to
// happen otherwise (transfer can never be a zero-config default; see
// gastrolog-2l918-c3).
func TestPutVaultTransferRequiresTarget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)

	cfg := fileVaultConfig(glid.New(), "transfer-no-target")
	cfg.RetentionDisposition = "transfer"

	err := putVault(t, client, cfg)
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("transfer with no target: want InvalidArgument, got %v", err)
	}
	if !strings.Contains(err.Error(), "target") {
		t.Fatalf("error must name the missing target, got: %v", err)
	}
}

// TestPutVaultTransferRejectsSelfTarget rejects target == source: the
// cascade footgun (a chunk transferred to itself would immediately
// re-qualify for the same retention rule).
func TestPutVaultTransferRejectsSelfTarget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)

	id := glid.New()
	cfg := fileVaultConfig(id, "transfer-self")
	cfg.RetentionDisposition = "transfer"
	cfg.RetentionTransferTargetVaultId = id.Bytes()

	err := putVault(t, client, cfg)
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("self-transfer: want InvalidArgument, got %v", err)
	}
	if !strings.Contains(err.Error(), "itself") && !strings.Contains(err.Error(), "self") && !strings.Contains(err.Error(), "same") {
		t.Fatalf("error must explain the self-transfer rejection, got: %v", err)
	}
}

// TestPutVaultTransferRequiresTargetExists rejects a target vault ID that
// doesn't resolve to any known vault at write time.
func TestPutVaultTransferRequiresTargetExists(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)

	cfg := fileVaultConfig(glid.New(), "transfer-missing-target")
	cfg.RetentionDisposition = "transfer"
	cfg.RetentionTransferTargetVaultId = glid.New().Bytes() // never created

	err := putVault(t, client, cfg)
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("nonexistent target: want InvalidArgument, got %v", err)
	}
	if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "does not exist") && !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("error must explain the missing target, got: %v", err)
	}
}

// TestPutVaultTransferRequiresBothFileVaults rejects transfer when either
// source or target is not a file vault (#4 in the spec: file → file only).
func TestPutVaultTransferRequiresBothFileVaults(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)

	// Create a memory-typed target first.
	memTarget := &gastrologv1.VaultConfig{
		Id:      glid.New().Bytes(),
		Name:    "transfer-mem-target",
		Enabled: true,
		Type:    gastrologv1.VaultType_VAULT_TYPE_MEMORY,
	}
	if err := putVault(t, client, memTarget); err != nil {
		t.Fatalf("create memory target: %v", err)
	}

	// Source is file, target is memory: rejected.
	src := fileVaultConfig(glid.New(), "transfer-file-src")
	src.RetentionDisposition = "transfer"
	src.RetentionTransferTargetVaultId = memTarget.Id
	if err := putVault(t, client, src); err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("file->memory transfer: want InvalidArgument, got %v", err)
	}

	// Create a file target.
	fileTarget := fileVaultConfig(glid.New(), "transfer-file-target")
	if err := putVault(t, client, fileTarget); err != nil {
		t.Fatalf("create file target: %v", err)
	}

	// Source is memory, target is file: rejected.
	memSrc := &gastrologv1.VaultConfig{
		Id:                             glid.New().Bytes(),
		Name:                           "transfer-mem-src",
		Enabled:                        true,
		Type:                           gastrologv1.VaultType_VAULT_TYPE_MEMORY,
		RetentionDisposition:           "transfer",
		RetentionTransferTargetVaultId: fileTarget.Id,
	}
	if err := putVault(t, client, memSrc); err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("memory->file transfer: want InvalidArgument, got %v", err)
	}
}

// TestPutVaultTransferAcceptsValidConfig is the happy path: file source,
// file target, both exist, target != source.
func TestPutVaultTransferAcceptsValidConfig(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)

	target := fileVaultConfig(glid.New(), "transfer-target-ok")
	if err := putVault(t, client, target); err != nil {
		t.Fatalf("create target: %v", err)
	}

	srcID := glid.New()
	src := fileVaultConfig(srcID, "transfer-source-ok")
	src.RetentionDisposition = "transfer"
	src.RetentionTransferTargetVaultId = target.Id
	if err := putVault(t, client, src); err != nil {
		t.Fatalf("valid transfer config rejected: %v", err)
	}

	stored := getStoredVault(t, store, srcID)
	if stored.RetentionDisposition != "transfer" {
		t.Fatalf("stored disposition = %q, want transfer", stored.RetentionDisposition)
	}
	if stored.RetentionTransferTargetVaultID == nil || *stored.RetentionTransferTargetVaultID != glid.FromBytes(target.Id) {
		t.Fatalf("stored target vault ID mismatch: got %v", stored.RetentionTransferTargetVaultID)
	}
}

// TestPutVaultNonTransferIgnoresTarget: a target vault ID set on a
// non-transfer disposition is not itself an error (the operator may be
// switching dispositions back and forth) but is not required either.
func TestPutVaultRouteDispositionDoesNotRequireTarget(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)

	cfg := fileVaultConfig(glid.New(), "route-no-target")
	cfg.RetentionDisposition = "route"
	if err := putVault(t, client, cfg); err != nil {
		t.Fatalf("route disposition without target: %v", err)
	}
}
