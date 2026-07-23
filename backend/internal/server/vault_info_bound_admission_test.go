package server_test

// gastrolog-5yfaqj: refusal generalizes from max-size to every retention
// policy bound. These tests extend vault_info_admission_test.go's
// coverage (VaultInfo.AdmissionRefused must reflect the backend's own
// admission-causes collector) to the two new causes — driven through the
// same peer-broadcast hooks production wiring installs
// (SetRemoteVaultAgeBoundCapped / SetRemoteVaultChunkCountBoundCapped),
// standing in for a node that only learns about a vault's swept-and-still-
// violated bound via another node's NodeStats broadcast (the retention
// leader for that vault instance is usually elsewhere).

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// TestListVaultsAdmissionRefusedAgeBound pins the age-bound cause end to
// end through the RPC surface: a vault this node never locally swept
// still reports VAULT_ADMISSION_CAUSE_AGE_BOUND once a peer's broadcast
// says so, and the cause clears on the next read once the peer stops
// reporting it.
func TestListVaultsAdmissionRefusedAgeBound(t *testing.T) {
	t.Parallel()
	sysClient, vaultClient, _, orch := admissionTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	putAdmissionTestVault(t, sysClient, id, "age-refused")

	orch.SetRemoteVaultAgeBoundCapped(func(vid glid.GLID) bool { return vid == id })

	resp, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	info := vaultInfoFor(resp.Msg.Vaults, id)
	if info == nil {
		t.Fatal("vault missing from ListVaults response")
	}
	want := []gastrologv1.VaultAdmissionCause{gastrologv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_AGE_BOUND}
	if !causesMatch(info.AdmissionRefused, want) {
		t.Fatalf("AdmissionRefused = %v, want %v", info.AdmissionRefused, want)
	}

	getResp, err := vaultClient.GetVault(ctx, connect.NewRequest(&gastrologv1.GetVaultRequest{Id: id.Bytes()}))
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if !causesMatch(getResp.Msg.Vault.AdmissionRefused, want) {
		t.Fatalf("GetVault AdmissionRefused = %v, want %v", getResp.Msg.Vault.AdmissionRefused, want)
	}

	orch.SetRemoteVaultAgeBoundCapped(func(glid.GLID) bool { return false })
	resp2, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults (after release): %v", err)
	}
	info2 := vaultInfoFor(resp2.Msg.Vaults, id)
	if len(info2.AdmissionRefused) != 0 {
		t.Fatalf("AdmissionRefused after release = %v, want empty", info2.AdmissionRefused)
	}
}

// TestListVaultsAdmissionRefusedAgeAndChunkCountTogether drives both new
// peer hooks at once — a vault violating both bounds must report both
// causes, in gate-check order, exactly like the existing disk-protect +
// max-size combination test.
func TestListVaultsAdmissionRefusedAgeAndChunkCountTogether(t *testing.T) {
	t.Parallel()
	sysClient, vaultClient, _, orch := admissionTestSetup(t)
	ctx := context.Background()
	id := glid.New()
	putAdmissionTestVault(t, sysClient, id, "double-bound-refused")

	orch.SetRemoteVaultAgeBoundCapped(func(vid glid.GLID) bool { return vid == id })
	orch.SetRemoteVaultChunkCountBoundCapped(func(vid glid.GLID) bool { return vid == id })

	resp, err := vaultClient.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	info := vaultInfoFor(resp.Msg.Vaults, id)
	if info == nil {
		t.Fatal("vault missing from ListVaults response")
	}
	want := []gastrologv1.VaultAdmissionCause{
		gastrologv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_AGE_BOUND,
		gastrologv1.VaultAdmissionCause_VAULT_ADMISSION_CAUSE_CHUNK_COUNT_BOUND,
	}
	if !causesMatch(info.AdmissionRefused, want) {
		t.Fatalf("AdmissionRefused = %v, want %v (gate-check order)", info.AdmissionRefused, want)
	}
}
