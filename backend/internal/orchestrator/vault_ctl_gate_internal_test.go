package orchestrator

import (
	"errors"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

type recordingApplier struct{ applied [][]byte }

func (a *recordingApplier) Apply(data []byte) error {
	a.applied = append(a.applied, data)
	return nil
}

// TestChunkApplierRefusesCommandAboveActiveLevel pins the gate in front of
// every vault chunk FSM proposal: a command above the active level, or one
// that cannot be decoded, never reaches the applier that proposes or
// forwards it.
func TestChunkApplierRefusesCommandAboveActiveLevel(t *testing.T) {
	t.Parallel()
	retentionAt2 := raftlevel.DefaultTable().With(
		raftlevel.Case((&gastrologv1.VaultCtlCommand{}).ProtoReflect().Descriptor(), "retention_pending"), 2)
	active := uint32(1)
	gate := raftlevel.NewGate(func() uint32 { return active }).WithTable(retentionAt2)
	next := &recordingApplier{}
	a := gatedChunkApplier{gate: gate, next: next}

	cmd := vaultctlfsm.MarshalRetentionPending(chunk.ChunkID{1})
	if err := a.Apply(cmd); !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
		t.Fatalf("Apply at active level 1 = %v, want ErrCommandAboveClusterLevel", err)
	}
	if err := a.Apply([]byte{0xFF}); !errors.Is(err, raftlevel.ErrUndecodableCommand) {
		t.Fatalf("Apply of undecodable bytes = %v, want ErrUndecodableCommand", err)
	}
	if len(next.applied) != 0 {
		t.Fatalf("refused commands reached the applier: %d", len(next.applied))
	}

	active = 2
	if err := a.Apply(cmd); err != nil {
		t.Fatalf("Apply at active level 2: %v", err)
	}
	if len(next.applied) != 1 {
		t.Fatalf("applier saw %d commands, want 1", len(next.applied))
	}
}
