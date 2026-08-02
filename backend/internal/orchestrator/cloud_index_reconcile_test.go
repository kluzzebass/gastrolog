package orchestrator

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/chunk"
)

// The round trip that matters: what the audit reports as cache drift is exactly
// what the repair clears, and a second audit comes back clean. Testing either
// half alone would let the two drift apart on which categories they mean.

func TestReconcileClearsStaleIndexEntryTheAuditFound(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	id := f.ids[0]

	// Store and cluster agree the chunk is gone; only the cache still holds it.
	f.deleteBlob(t, id)
	f.entries = nil

	before := f.audit(t)
	if len(before.StaleIndexEntries) != 1 {
		t.Fatalf("premise: audit found %d stale entries, want 1", len(before.StaleIndexEntries))
	}

	repair, err := f.orch.ReconcileVaultCloudIndex(context.Background(), f.vaultID)
	if err != nil {
		t.Fatalf("ReconcileVaultCloudIndex: %v", err)
	}
	if repair.RemovedEntries != 1 {
		t.Errorf("RemovedEntries = %d, want 1 (%+v)", repair.RemovedEntries, repair)
	}

	after := f.audit(t)
	if len(after.StaleIndexEntries) != 0 {
		t.Errorf("audit still reports %d stale entries after the repair", len(after.StaleIndexEntries))
	}
}

// The repair must not touch the durability categories. A missing object is not
// something a cache rebuild can fix, and quietly dropping the entry would
// convert a reported data loss into silence.
func TestReconcileDoesNotHideAMissingObject(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 2)
	lost := f.ids[0]
	f.deleteBlob(t, lost)

	before := f.audit(t)
	if len(before.MissingBlobs) != 1 || before.MissingBlobs[0] != lost {
		t.Fatalf("premise: MissingBlobs = %v, want [%s]", before.MissingBlobs, lost)
	}

	if _, err := f.orch.ReconcileVaultCloudIndex(context.Background(), f.vaultID); err != nil {
		t.Fatalf("ReconcileVaultCloudIndex: %v", err)
	}

	after := f.audit(t)
	if len(after.MissingBlobs) != 1 || after.MissingBlobs[0] != lost {
		t.Errorf("MissingBlobs = %v after repair, want the loss still reported as [%s]",
			after.MissingBlobs, lost)
	}
}

// An untracked object is where the cluster view is the suspect one, so the
// repair must leave the bytes alone and keep reporting them.
func TestReconcileLeavesUntrackedObjectsAlone(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	stray := f.uploadStrayBlob(t)

	if _, err := f.orch.ReconcileVaultCloudIndex(context.Background(), f.vaultID); err != nil {
		t.Fatalf("ReconcileVaultCloudIndex: %v", err)
	}

	audit := f.audit(t)
	var stillThere bool
	for _, id := range audit.UntrackedBlobs {
		if id == stray {
			stillThere = true
		}
	}
	if !stillThere {
		t.Errorf("the untracked object vanished from the audit; repair must not delete bytes (%+v)", audit)
	}
}

func TestReconcileOnLocalOnlyVault(t *testing.T) {
	t.Parallel()
	orch, vaultID := newLocalOnlyVaultForAudit(t)

	_, err := orch.ReconcileVaultCloudIndex(context.Background(), vaultID)
	if !errors.Is(err, chunk.ErrCloudStoreNotConfigured) {
		t.Fatalf("err = %v, want ErrCloudStoreNotConfigured", err)
	}
}

func TestReconcileOnUnknownVault(t *testing.T) {
	t.Parallel()
	f := newAuditFixture(t, 1)
	other := newAuditFixture(t, 0)

	if _, err := f.orch.ReconcileVaultCloudIndex(context.Background(), other.vaultID); err == nil {
		t.Fatal("reconciling a vault this node does not home returned no error")
	}
}
