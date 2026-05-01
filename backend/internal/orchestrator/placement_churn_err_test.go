package orchestrator

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsPlacementChurnErr pins the gastrolog-5z607 helper. The matrix
// covers the four authentic origin shapes (local sentinel direct,
// local sentinel wrapped via fmt.Errorf %w, cross-RPC rendered string
// for legacy "vault not found" wording, cross-RPC rendered string for
// new "tier not registered on this node" wording) plus the negative
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
		{"direct ErrTierNotLocal", ErrTierNotLocal, true},
		{"wrapped ErrTierNotLocal", fmt.Errorf("seal: %w: tier x in vault y", ErrTierNotLocal), true},
		{
			"cross-RPC legacy vault-not-found",
			errors.New("follower rejected command: import failed: vault not found: tier T in vault V"),
			true,
		},
		{
			"cross-RPC new tier-not-local",
			errors.New("follower rejected command: seal failed: tier not registered on this node: tier T in vault V"),
			true,
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
