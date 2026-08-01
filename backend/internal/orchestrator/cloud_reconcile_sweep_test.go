package orchestrator

import (
	"testing"
	"time"
)

// The daily sweep is the only thing that ever notices an object removed outside
// GastroLog — a provider lifecycle rule, a bucket policy, an operator with a
// console. None of those emit an event, so there is nothing to be event-driven
// about, and these tests pin the detection the schedule exists for.
//
// Detection itself is the shared audit; what the sweep owns is time: how long a
// chunk has been missing and what happens once that exceeds the grace period.

func TestReconcileSweepMarksOutOfBandLossSuspect(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	lost := f.ids[0]

	f.deleteBlob(t, lost)
	f.orch.reconcileSweepAll()

	if f.orch.suspects == nil {
		t.Fatal("no suspect tracker on the orchestrator")
	}
	if _, marked := f.orch.suspects.suspectSince(lost); !marked {
		t.Fatalf("chunk %s lost from the store was not marked suspect", lost)
	}
}

// A chunk whose object is present must not stay suspect from an earlier pass. A
// transient 404 would otherwise leave the alert raised forever.
func TestReconcileSweepClearsSuspectWhenObjectReturns(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	id := f.ids[0]

	f.deleteBlob(t, id)
	f.orch.reconcileSweepAll()
	if _, marked := f.orch.suspects.suspectSince(id); !marked {
		t.Fatal("premise: the chunk was never marked suspect")
	}

	// The object comes back: a re-upload, or a provider restore completing.
	f.reuploadBlob(t, id)
	f.orch.reconcileSweepAll()

	if _, stillMarked := f.orch.suspects.suspectSince(id); stillMarked {
		t.Error("chunk still suspect after its object returned")
	}
}

// Inside the grace period the sweep must observe and wait, not delete. Removing
// a chunk on first sighting would turn a provider hiccup into permanent loss of
// the manifest entry.
func TestReconcileSweepWaitsOutTheGracePeriod(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	lost := f.ids[0]
	f.deleteBlob(t, lost)

	f.orch.reconcileSweepAll()
	since, marked := f.orch.suspects.suspectSince(lost)
	if !marked {
		t.Fatal("premise: not marked suspect")
	}

	// A second pass the next day, still inside the default 7-day grace.
	f.orch.now = func() time.Time { return since.Add(24 * time.Hour) }
	f.orch.reconcileSweepAll()

	if _, stillSuspect := f.orch.suspects.suspectSince(lost); !stillSuspect {
		t.Error("chunk dropped from suspect tracking while inside the grace period")
	}
	audit := f.audit(t)
	if len(audit.MissingBlobs) != 1 {
		t.Errorf("MissingBlobs = %v, want the loss still reported during the grace period", audit.MissingBlobs)
	}
}

// A vault with no cloud store must be skipped silently rather than logged as a
// failure every night.
func TestReconcileSweepSkipsLocalOnlyVault(t *testing.T) {
	t.Parallel()
	orch, _ := newLocalOnlyVaultForAudit(t)

	orch.reconcileSweepAll() // must not panic and must mark nothing

	if orch.suspects != nil && len(orch.suspects.ids()) != 0 {
		t.Errorf("local-only vault produced %d suspects", len(orch.suspects.ids()))
	}
}
