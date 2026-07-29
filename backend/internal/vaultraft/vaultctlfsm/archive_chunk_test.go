package vaultctlfsm

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// A chunk's CURRENT cloud storage class used to live only in a node-local map
// on whichever node called the cloud API. The archival sweep compares that
// class against the transition chain's target, and on the FSM iteration path it
// read the manifest entry — where the field did not exist — so the comparison
// always saw "" and a multi-step chain (cold -> deep-freeze) could never advance
// past its first step. CmdArchiveChunk makes the class replicated state, so any
// voter can answer "what class is this chunk in?" (gastrolog-35ygqv).

func seedChunk(t *testing.T, f *FSM, id [16]byte) {
	t.Helper()
	applyCmd(t, f, mustMarshalRepatriate(t, ManifestEntry{ID: id, RecordCount: 3, CloudBacked: true}))
}

func TestApplyArchiveChunk_RecordsClassAndDerivesArchived(t *testing.T) {
	t.Parallel()
	f := New()
	id := testChunkID(1)
	seedChunk(t, f, id)

	if got := f.Get(id); got.CloudStorageClass != "" || got.Archived {
		t.Fatalf("seed entry should start unarchived, got class=%q archived=%v", got.CloudStorageClass, got.Archived)
	}

	applyCmd(t, f, MarshalArchiveChunk(id, "cold"))

	got := f.Get(id)
	if got.CloudStorageClass != "cold" {
		t.Errorf("CloudStorageClass = %q, want %q", got.CloudStorageClass, "cold")
	}
	if !got.Archived {
		t.Error("Archived must be derived from a non-empty class")
	}
	if got.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3: archiving must not touch unrelated fields", got.RecordCount)
	}
}

// The defect itself, at the FSM level: a second archive to a COLDER class must
// take effect. The old bookkeeping was a bool, which had nothing to advance.
func TestApplyArchiveChunk_AdvancesToAColderClass(t *testing.T) {
	t.Parallel()
	f := New()
	id := testChunkID(2)
	seedChunk(t, f, id)

	applyCmd(t, f, MarshalArchiveChunk(id, "cold"))
	applyCmd(t, f, MarshalArchiveChunk(id, "deep-freeze"))

	if got := f.Get(id); got.CloudStorageClass != "deep-freeze" {
		t.Fatalf("CloudStorageClass = %q, want %q: the chain must advance, not stick at its first step",
			got.CloudStorageClass, "deep-freeze")
	}
}

// Restore clears the class. Archived has to follow, or the FSM keeps claiming a
// chunk is in offline storage after it has been brought back.
func TestApplyArchiveChunk_EmptyClassClearsArchived(t *testing.T) {
	t.Parallel()
	f := New()
	id := testChunkID(3)
	seedChunk(t, f, id)
	applyCmd(t, f, MarshalArchiveChunk(id, "cold"))

	applyCmd(t, f, MarshalArchiveChunk(id, ""))

	got := f.Get(id)
	if got.CloudStorageClass != "" {
		t.Errorf("CloudStorageClass = %q, want empty after restore", got.CloudStorageClass)
	}
	if got.Archived {
		t.Error("Archived must clear with the class")
	}
}

func TestApplyArchiveChunk_IdempotentAndNoOpWhenMissing(t *testing.T) {
	t.Parallel()
	f := New()
	id := testChunkID(4)
	seedChunk(t, f, id)

	applyCmd(t, f, MarshalArchiveChunk(id, "cold"))
	applyCmd(t, f, MarshalArchiveChunk(id, "cold"))
	if got := f.Get(id); got.CloudStorageClass != "cold" {
		t.Errorf("CloudStorageClass = %q, want %q after a replayed archive", got.CloudStorageClass, "cold")
	}

	// An archive for a chunk this FSM does not know must not error: the cloud
	// call already succeeded, and failing the apply would not un-archive the
	// blob — it would only make the FSM disagree with it.
	applyCmd(t, f, MarshalArchiveChunk(testChunkID(99), "cold"))
}

// The class must survive the snapshot boundary, or a node that joins or
// restarts loses it and the chain stalls again for that node.
func TestArchiveChunk_SnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	src := New()
	id := testChunkID(5)
	seedChunk(t, src, id)
	applyCmd(t, src, MarshalArchiveChunk(id, "deep-freeze"))

	dst := New()
	dst.RestoreProto(src.SnapshotProto())

	if !proto.Equal(src.SnapshotProto(), dst.SnapshotProto()) {
		t.Fatalf("snapshot proto state differs after restore")
	}
	got := dst.Get(id)
	if got == nil {
		t.Fatal("entry missing after restore")
	}
	if got.CloudStorageClass != "deep-freeze" {
		t.Errorf("CloudStorageClass = %q after restore, want %q", got.CloudStorageClass, "deep-freeze")
	}
	if !got.Archived {
		t.Error("Archived must survive the snapshot boundary too")
	}
}
