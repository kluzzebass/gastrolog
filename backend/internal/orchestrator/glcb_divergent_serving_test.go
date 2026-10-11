package orchestrator

import (
	"errors"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// A seal apply that finds this home's copy is not the agreed one drops the
// chunk manager's registration and cached mapping of that copy, so a query
// that read it before the check is not served from it again. The agreed copy
// stays registered.
func TestSealApplyForgetsACopyThatIsNotTheAgreedOne(t *testing.T) {
	t.Parallel()
	for _, diverge := range []bool{false, true} {
		name := "agreed copy"
		if diverge {
			name = "divergent copy"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			orch, fx, cm, _, fsm := sealApplyHome(t, t.Context())
			glcbPath := placeGLCBOnHome(t, orch, fx.vaultID, fx)
			if err := cm.RegisterExternalGLCB(fx.sealed.ID, glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
				t.Fatalf("RegisterExternalGLCB: %v", err)
			}
			drainChunk(t, cm, fx.sealed.ID)

			entry := fx.sealed
			if diverge {
				entry = divergentEntry(entry)
			}
			applySeal(t, fsm, entry)

			registered := cm.IsExternalGLCBAt(fx.sealed.ID, glcbPath)
			cursor, err := cm.OpenCursor(fx.sealed.ID)
			if err == nil {
				_ = cursor.Close()
			}
			if !diverge {
				if !registered || err != nil {
					t.Fatalf("agreed copy: registered=%v open=%v, want it still served", registered, err)
				}
				return
			}
			if registered {
				t.Fatal("the chunk manager still names the copy that is not the agreed one")
			}
			if !errors.Is(err, chunk.ErrChunkNotFound) {
				t.Fatalf("OpenCursor after the landing check = %v, want ErrChunkNotFound", err)
			}
		})
	}
}

// The lazy resolver registers a home's copy only when it carries the sealed
// digest, so a lookup between a divergent copy landing and its landing check
// does not memoize it.
func TestResolverRegistersOnlyTheAgreedCopy(t *testing.T) {
	t.Parallel()
	for _, diverge := range []bool{false, true} {
		name := "agreed copy"
		if diverge {
			name = "divergent copy"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fx := buildSealedPipelineGLCB(t, t.Context(), 12, e1Payload, record.Attributes{"service": "api"})
			entry := fx.sealed
			if diverge {
				entry = divergentEntry(entry)
			}
			fsm := vaultctlfsm.New()
			applySeal(t, fsm, entry)
			ti, cm, _ := fileVaultInstance(t, fx.vaultID)
			chunkRoot := filepath.Dir(filepath.Dir(fx.glcbPath))
			installGLCBResolverOn(ti, func() (*vaultctlfsm.FSM, string, bool) { return fsm, chunkRoot, true })

			_, err := cm.Meta(fx.sealed.ID)
			if diverge {
				if !errors.Is(err, chunk.ErrChunkNotFound) {
					t.Fatalf("Meta of a divergent copy = %v, want ErrChunkNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Meta of the agreed copy: %v", err)
			}
		})
	}
}

func drainChunk(t *testing.T, cm chunk.ChunkManager, id chunk.ChunkID) {
	t.Helper()
	cursor, err := cm.OpenCursor(id)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer func() { _ = cursor.Close() }()
	for {
		if _, _, err := cursor.Next(); err != nil {
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			t.Fatalf("Next: %v", err)
		}
	}
}
