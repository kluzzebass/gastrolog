package server_test

// Coverage for the VaultInfo.AdmissionRefused RPC field (gastrolog-33ul6h
// operator directive): the vault "refusing admission" signal must come from
// the backend's own admission-causes collector, not be derived client-side
// from alarm state. These tests drive the REAL orchestrator admission gate
// (via the same exported peer-broadcast hooks production wiring installs —
// SetRemoteVaultStorageProtected / SetRemoteVaultSizeCapped, per the existing
// TestMultiNode_RetentionSubmitDefersOnRemoteCappedDestination pattern) and
// assert the ListVaults/GetVault RPCs report exactly what the gate itself
// would enforce. gastrolog-9akebz: AdmissionRefused entries are now
// VaultAdmissionRefusal{cause, detail} pairs, not bare cause enums.

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// admissionTestSetup mirrors newConfigTestSetup (system_test.go) but also
// exposes the VaultServiceClient the AdmissionRefused field rides on.
func admissionTestSetup(t *testing.T) (gastrologv1connect.SystemServiceClient, gastrologv1connect.VaultServiceClient, system.Store, *orchestrator.Orchestrator) {
	t.Helper()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, SegmentsDir: filepath.Join(t.TempDir(), "segments")})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		VaultsDir: t.TempDir(),
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
			"file":   chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
			"file":   indexfile.NewFactory(),
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{
		AfterConfigApply: testAfterConfigApply(t, orch, cfgStore, factories),
	})
	handler := srv.Handler()
	httpClient := &http.Client{Transport: &embeddedTransport{handler: handler}}

	return gastrologv1connect.NewSystemServiceClient(httpClient, "http://embedded"),
		gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded"),
		cfgStore, orch
}

func putAdmissionTestVault(t *testing.T, client gastrologv1connect.SystemServiceClient, id glid.GLID, name string) {
	t.Helper()
	if _, err := client.PutVault(context.Background(), connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{
			Id:      id.Bytes(),
			Name:    name,
			Enabled: true,
			Type:    gastrologv1.VaultType_VAULT_TYPE_MEMORY,
		},
	})); err != nil {
		t.Fatalf("PutVault(%s): %v", name, err)
	}
}

func vaultInfoFor(vaults []*gastrologv1.VaultInfo, id glid.GLID) *gastrologv1.VaultInfo {
	for _, v := range vaults {
		if glid.FromBytes(v.Id) == id {
			return v
		}
	}
	return nil
}

// TestListVaultsAdmissionRefusedEmpty pins the healthy baseline: a vault
// admitting normally reports an empty AdmissionRefused list, not a nil-vs-
// empty ambiguity or a stale value.
func TestListVaultsAdmissionRefusedEmpty(t *testing.T) {
	t.Parallel()
	sysClient, vaultClient, _, _ := admissionTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	putAdmissionTestVault(t, sysClient, id, "healthy")

	resp, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	info := vaultInfoFor(resp.Msg.Vaults, id)
	if info == nil {
		t.Fatal("vault missing from ListVaults response")
	}
	if len(info.AdmissionRefused) != 0 {
		t.Fatalf("AdmissionRefused = %v, want empty for a healthy vault", info.AdmissionRefused)
	}
}

// TestListVaultsAdmissionRefusedStorageDiskProtect drives the real admission
// gate's peer-broadcast half (SetRemoteVaultStorageProtected) and asserts
// the RPC field reports VAULT_ADMISSION_CAUSE_STORAGE_DISK_PROTECT — the
// same cause vaultAdmissionGate would refuse the vault with — plus a
// non-empty detail naming the reporting node (gastrolog-9akebz).
func TestListVaultsAdmissionRefusedStorageDiskProtect(t *testing.T) {
	t.Parallel()
	sysClient, vaultClient, _, orch := admissionTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	putAdmissionTestVault(t, sysClient, id, "protected")

	orch.SetRemoteVaultStorageProtected(func(vid glid.GLID) bool { return vid == id })
	orch.SetRemoteVaultStorageProtectedNodes(func(vid glid.GLID) []string {
		if vid == id {
			return []string{"data-2"}
		}
		return nil
	})

	resp, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	info := vaultInfoFor(resp.Msg.Vaults, id)
	if info == nil {
		t.Fatal("vault missing from ListVaults response")
	}
	want := []gastrologv1.VaultAdmissionCause{gastrologv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_STORAGE_DISK_PROTECT}
	if !causesMatch(info.AdmissionRefused, want) {
		t.Fatalf("AdmissionRefused = %v, want %v", info.AdmissionRefused, want)
	}
	if len(info.AdmissionRefused) != 1 || info.AdmissionRefused[0].Detail == "" {
		t.Fatalf("AdmissionRefused detail must be populated, got %+v", info.AdmissionRefused)
	}

	// GetVault must agree with ListVaults — same collector, same verdict.
	getResp, err := vaultClient.GetVault(ctx, connect.NewRequest(&gastrologv1.GetVaultRequest{Id: id.Bytes()}))
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if !causesMatch(getResp.Msg.Vault.AdmissionRefused, want) {
		t.Fatalf("GetVault AdmissionRefused = %v, want %v", getResp.Msg.Vault.AdmissionRefused, want)
	}

	// Cause releases: the RPC field must clear on the next read, not stay
	// stuck reporting a stale refusal.
	orch.SetRemoteVaultStorageProtected(func(glid.GLID) bool { return false })
	resp2, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults (after release): %v", err)
	}
	info2 := vaultInfoFor(resp2.Msg.Vaults, id)
	if len(info2.AdmissionRefused) != 0 {
		t.Fatalf("AdmissionRefused after release = %v, want empty", info2.AdmissionRefused)
	}
}

// TestListVaultsAdmissionRefusedCombination drives both the storage-protect
// and max-size peer hooks at once and asserts BOTH causes are reported, in
// gate-check order — the RPC field must show the full set even though
// vaultAdmissionGate itself only acts on the first.
func TestListVaultsAdmissionRefusedCombination(t *testing.T) {
	t.Parallel()
	sysClient, vaultClient, _, orch := admissionTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	putAdmissionTestVault(t, sysClient, id, "double-refused")

	orch.SetRemoteVaultStorageProtected(func(vid glid.GLID) bool { return vid == id })
	orch.SetRemoteVaultSizeCapped(func(vid glid.GLID) bool { return vid == id })

	resp, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	info := vaultInfoFor(resp.Msg.Vaults, id)
	if info == nil {
		t.Fatal("vault missing from ListVaults response")
	}
	want := []gastrologv1.VaultAdmissionCause{
		gastrologv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_STORAGE_DISK_PROTECT,
		gastrologv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_MAX_SIZE_BOUND,
	}
	if !causesMatch(info.AdmissionRefused, want) {
		t.Fatalf("AdmissionRefused = %v, want %v (gate-check order)", info.AdmissionRefused, want)
	}
}

func causesMatch(got []*gastrologv1.VaultAdmissionRefusal, want []gastrologv1.VaultAdmissionCause) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Cause != want[i] {
			return false
		}
	}
	return true
}
