package orchestrator

import (
	"log/slog"
	"slices"
	"sync"
	"testing"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ackRecorder stands in for the vault-ctl holder-receipt proposal.
type ackRecorder struct {
	mu    sync.Mutex
	acked []chunk.ChunkID
	done  chan struct{}
}

func newAckRecorder() *ackRecorder { return &ackRecorder{done: make(chan struct{}, 16)} }

func (a *ackRecorder) ack(ids []chunk.ChunkID, _ string) error {
	a.mu.Lock()
	a.acked = append(a.acked, ids...)
	a.mu.Unlock()
	a.done <- struct{}{}
	return nil
}

func (a *ackRecorder) snapshot() []chunk.ChunkID {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.acked)
}

// divergentEntry is the fixture's sealed entry recording a digest the copy on
// the home does not carry — the leader built other bytes than this home.
func divergentEntry(e vaultctlfsm.ManifestEntry) vaultctlfsm.ManifestEntry {
	e.Hash[0] ^= 0xff
	return e
}

// The seal apply lands a copy the home already holds: indexes and a holder
// receipt, but only when the copy carries the digest the seal recorded.
func TestSealApplyLandsOnlyTheAgreedCopy(t *testing.T) {
	t.Parallel()
	for _, diverge := range []bool{false, true} {
		name := "agreed copy"
		if diverge {
			name = "divergent copy"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			orch, fx, cm, im, fsm := sealApplyHome(t, ctx)
			if fx.sealed.Hash == ([32]byte{}) {
				t.Fatal("premise: the fixture's seal recorded the leader build's digest")
			}
			glcbPath := placeGLCBOnHome(t, orch, fx.vaultID, fx)
			if err := cm.RegisterExternalGLCB(fx.sealed.ID, glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
				t.Fatalf("RegisterExternalGLCB: %v", err)
			}
			acks := newAckRecorder()
			orch.findLocalVaultInstance(fx.vaultID).ApplyRaftAckChunkHolders = acks.ack

			entry := fx.sealed
			if diverge {
				entry = divergentEntry(entry)
			}
			applySeal(t, fsm, entry)

			if !diverge {
				awaitIndexes(t, orch, im, fx.sealed.ID)
				<-acks.done
				// Receipts are idempotent; more than one landing may propose the same one.
				if got := slices.Compact(acks.snapshot()); !slices.Equal(got, []chunk.ChunkID{fx.sealed.ID}) {
					t.Fatalf("holder receipts = %v, want the agreed copy's", got)
				}
				return
			}
			// Indexing and the receipt sit behind one gate in onGLCBLanded,
			// and indexing is scheduled synchronously within the seal apply.
			if jobs := indexJobsScheduled(orch); len(jobs) != 0 {
				t.Fatalf("seal apply indexed a copy whose digest differs from the sealed one: %v", jobs)
			}
		})
	}
}

// The catch-up sweep earns a receipt for a copy this home holds and has not
// claimed — the agreed copy only.
func TestSweepClaimsOnlyTheAgreedCopy(t *testing.T) {
	t.Parallel()
	for _, diverge := range []bool{false, true} {
		name := "agreed copy"
		if diverge {
			name = "divergent copy"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api"})
			orch := newTestOrch(t, Config{LocalNodeID: landingHomeNode})
			registerPipelineHome(t, orch, fx.vaultID, true)
			ti, _, _ := fileVaultInstance(t, fx.vaultID)
			acks := newAckRecorder()
			ti.ApplyRaftAckChunkHolders = acks.ack

			entry := fx.sealed
			if diverge {
				entry = divergentEntry(entry)
			}
			fsm := vaultctlfsm.New()
			for _, cmd := range sealCommands(entry) {
				if err := fsm.Apply(&hraft.Log{Data: cmd}); err != nil {
					t.Fatalf("apply: %v", err)
				}
			}
			placeGLCBOnHome(t, orch, fx.vaultID, fx)
			rec := NewVaultLifecycleReconciler(orch, fx.vaultID, ti, landingHomeNode, slog.Default())
			rec.Wire(fsm)

			rec.SweepMissingReplicas()

			got := acks.snapshot()
			if diverge {
				if len(got) != 0 {
					t.Fatalf("the sweep claimed a copy whose digest differs from the sealed one: %v", got)
				}
				return
			}
			if !slices.Equal(got, []chunk.ChunkID{fx.sealed.ID}) {
				t.Fatalf("holder receipts = %v, want the agreed copy's", got)
			}
		})
	}
}
