package server

import (
	"fmt"
	"testing"

	"connectrpc.com/connect"

	"gastrolog/internal/orchestrator"
)

// A seal refused because the node cannot commit the pipeline seal is
// retryable once vault-ctl leadership settles, so it reaches the caller as
// Unavailable, not as an internal failure.
func TestMapVaultErrorRefusedSealIsUnavailable(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("seal vault: %w", fmt.Errorf("%w: vault x", orchestrator.ErrNotChunkingLeader))
	if got := mapVaultError(err).Code(); got != connect.CodeUnavailable {
		t.Fatalf("mapVaultError(ErrNotChunkingLeader) code = %s, want %s", got, connect.CodeUnavailable)
	}
}
