package server_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// SealVault reports exactly how many open chunks it sealed: the active chunk
// holding records the first time, nothing the second time.
func TestSealVaultReportsWhatItSealed(t *testing.T) {
	t.Parallel()
	clients := newVaultTestSetup(t, 12) // 2 sealed chunks of 5 + an active chunk of 2
	ctx := context.Background()
	req := connect.NewRequest(&gastrologv1.SealVaultRequest{Vault: clients.defaultID.String()})

	resp, err := clients.vault.SealVault(ctx, req)
	if err != nil {
		t.Fatalf("SealVault: %v", err)
	}
	if resp.Msg.SealedCount != 1 {
		t.Fatalf("first SealVault sealed_count = %d, want 1", resp.Msg.SealedCount)
	}

	resp, err = clients.vault.SealVault(ctx, connect.NewRequest(&gastrologv1.SealVaultRequest{Vault: clients.defaultID.String()}))
	if err != nil {
		t.Fatalf("second SealVault: %v", err)
	}
	if resp.Msg.SealedCount != 0 {
		t.Fatalf("second SealVault sealed_count = %d, want 0 with nothing open", resp.Msg.SealedCount)
	}
}

func TestSealVaultUnknownVaultIsNotFound(t *testing.T) {
	t.Parallel()
	clients := newVaultTestSetup(t, 0)
	_, err := clients.vault.SealVault(context.Background(),
		connect.NewRequest(&gastrologv1.SealVaultRequest{Vault: glid.New().String()}))
	if ce := new(connect.Error); !errors.As(err, &ce) || ce.Code() != connect.CodeNotFound {
		t.Fatalf("SealVault on unknown vault: err = %v, want NotFound", err)
	}
}
