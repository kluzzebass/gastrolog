package orchestrator

import (
	"errors"
	"fmt"
	"gastrolog/internal/chunk"
	"testing"
)

// TestIsPlacementChurnErr pins the placement-churn error helper. The matrix
// covers the four authentic origin shapes (local sentinel direct,
// local sentinel wrapped via fmt.Errorf %w, cross-RPC rendered string
// for legacy "vault not found" wording, cross-RPC rendered string for
// new "instance not registered on this node" wording) plus the negative
// cases (nil, unrelated errors).
func TestIsPlacementChurnErr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("disk full"), false},
		{"direct ErrVaultNotFound", ErrVaultNotFound, true},
		{"wrapped ErrVaultNotFound", fmt.Errorf("look up vault: %w", ErrVaultNotFound), true},
		{"direct ErrInstanceNotLocal", ErrInstanceNotLocal, true},
		{"wrapped ErrInstanceNotLocal", fmt.Errorf("seal: %w: vaultInst x in vault y", ErrInstanceNotLocal), true},
		{
			"cross-RPC vault-not-found carried as the shared chunk sentinel",
			fmt.Errorf("follower rejected command: %w: import failed: vaultInst T in vault V", chunk.ErrVaultNotFound),
			true,
		},
		{
			"cross-RPC not-local carried as the shared chunk sentinel",
			fmt.Errorf("follower rejected command: %w: seal failed: vault V", chunk.ErrVaultNotLocal),
			true,
		},
		{
			"prose alone no longer classifies",
			errors.New("follower rejected command: import failed: vault not found: vaultInst T in vault V"),
			false,
		},
		{
			"unrelated 'not found'",
			errors.New("chunk not found"),
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsPlacementChurnErr(tc.err)
			if got != tc.want {
				t.Errorf("IsPlacementChurnErr(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}
