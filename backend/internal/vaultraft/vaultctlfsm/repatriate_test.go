package vaultctlfsm

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
)

func applyLog(f *FSM, data []byte) any {
	return f.Apply(&hraft.Log{Index: 1, Term: 1, Data: data})
}

// TestApplyRepatriate_HappyPath verifies a manifest entry that's
// absent from the FSM gets re-introduced when CmdRepatriateChunk
// fires. State is forced to Sealed regardless of the payload's
// stated State — repatriation handles sealed chunks only.
// gastrolog-32bf2.
func TestApplyRepatriate_HappyPath(t *testing.T) {
	t.Parallel()
	f := New()
	id := chunk.ChunkID(glid.New())
	entry := ManifestEntry{
		ID:          id,
		WriteStart:  time.Unix(1000, 0).UTC(),
		WriteEnd:    time.Unix(2000, 0).UTC(),
		RecordCount: 42,
		Bytes:       1024,
		State:       chunk.ChunkStateActive, // overwritten by apply
	}
	data, err := MarshalRepatriateChunk(entry)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got := applyLog(f, data); got != nil {
		t.Fatalf("Apply: %v", got)
	}
	got := f.Get(id)
	if got == nil {
		t.Fatalf("repatriated entry missing from manifest")
	}
	if got.State != chunk.ChunkStateSealed {
		t.Errorf("state: got %s, want Sealed (apply must force regardless of payload)", got.State)
	}
	if got.RecordCount != 42 || got.Bytes != 1024 {
		t.Errorf("fields not round-tripped: %+v", got)
	}
}

// TestApplyRepatriate_RefusesIfAlreadyPresent guards against
// resurrecting via repatriate a chunk the FSM already tracks — the
// normal lifecycle commands own state changes for existing
// entries.
func TestApplyRepatriate_RefusesIfAlreadyPresent(t *testing.T) {
	t.Parallel()
	f := New()
	id := chunk.ChunkID(glid.New())

	if got := applyLog(f, MarshalCreateChunk(id, time.Unix(100, 0), time.Unix(100, 0), time.Unix(100, 0))); got != nil {
		t.Fatalf("seed Create: %v", got)
	}
	if got := applyLog(f, MarshalSealChunk(id, time.Unix(200, 0), 10, 1024, time.Unix(100, 0), time.Unix(200, 0), time.Unix(200, 0), true, time.Unix(200, 0))); got != nil {
		t.Fatalf("seed Seal: %v", got)
	}

	data, _ := MarshalRepatriateChunk(ManifestEntry{ID: id, RecordCount: 99})
	got := applyLog(f, data)
	if got == nil {
		t.Fatalf("expected refusal for existing chunk, got nil")
	}
	// Entry must not have been overwritten.
	e := f.Get(id)
	if e.RecordCount != 10 {
		t.Errorf("entry overwritten: got %d, want 10 (original)", e.RecordCount)
	}
}

// TestApplyRepatriate_RefusesIfTombstoned guards against
// resurrecting a chunk the cluster has explicitly forgotten —
// same tombstone guard as CmdCreateChunk (gastrolog-11rzz).
func TestApplyRepatriate_RefusesIfTombstoned(t *testing.T) {
	t.Parallel()
	f := New()
	id := chunk.ChunkID(glid.New())

	if got := applyLog(f, MarshalDeleteChunk(id)); got != nil {
		t.Fatalf("seed tombstone: %v", got)
	}

	data, _ := MarshalRepatriateChunk(ManifestEntry{ID: id})
	got := applyLog(f, data)
	if got == nil {
		t.Fatalf("expected refusal for tombstoned chunk, got nil")
	}
	// Confirm entry still absent.
	if f.Get(id) != nil {
		t.Errorf("entry resurrected despite tombstone")
	}
}
