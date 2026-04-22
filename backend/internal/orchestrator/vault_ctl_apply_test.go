package orchestrator

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft"
)

func TestApplyVaultControlPlane_NoGroupManager(t *testing.T) {
	t.Parallel()
	o, err := New(Config{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		LocalNodeID: "node-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = o.ApplyVaultControlPlane(glid.New(), vaultraft.MarshalNoop())
	if !errors.Is(err, ErrVaultCtlRaftUnavailable) {
		t.Fatalf("err = %v, want ErrVaultCtlRaftUnavailable", err)
	}
}
