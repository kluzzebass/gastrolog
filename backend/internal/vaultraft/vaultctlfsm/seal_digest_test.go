package vaultctlfsm

import (
	"bytes"
	"io"
	"testing"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

func sealDigestForTest(b byte) [32]byte {
	var d [32]byte
	d[0], d[31] = b, ^b
	return d
}

// sealDigestLog is a vault-ctl log that seals two chunks, one with a digest
// and one without.
func sealDigestLog(digested, undigested chunk.ChunkID, digest [32]byte) [][]byte {
	at := time.Unix(0, 1_700_000_000_000_000_000)
	return [][]byte{
		MarshalCreateChunk(digested, at, at, at),
		MarshalSealChunk(digested, at.Add(time.Second), 3, 300, at, at, at, true, at.Add(time.Second), digest),
		MarshalCreateChunk(undigested, at, at, at),
		MarshalSealChunk(undigested, at.Add(time.Second), 3, 300, at, at, at, true, at.Add(time.Second), [32]byte{}),
	}
}

func TestSealChunkRecordsTheBlobDigest(t *testing.T) {
	t.Parallel()
	digested, undigested := testChunkID(1), testChunkID(2)
	digest := sealDigestForTest(0x5a)
	f := New()
	for _, cmd := range sealDigestLog(digested, undigested, digest) {
		applyCmd(t, f, cmd)
	}
	if got := f.Get(digested).Hash; got != digest {
		t.Fatalf("sealed digest = %x, want %x", got[:4], digest[:4])
	}
	if got := f.Get(undigested).Hash; got != ([32]byte{}) {
		t.Fatalf("a seal without a digest recorded %x", got[:4])
	}
}

// Every voter replays the same log, and a voter restored from a snapshot must
// hold the same digest as one that applied the log: homes verify their copies
// against whichever they have.
func TestSealDigestSurvivesSnapshotAndReplay(t *testing.T) {
	t.Parallel()
	digested, undigested := testChunkID(1), testChunkID(2)
	digest := sealDigestForTest(0x5a)
	log := sealDigestLog(digested, undigested, digest)

	applied := New()
	for _, cmd := range log {
		applyCmd(t, applied, cmd)
	}
	replayed := New()
	for _, cmd := range log {
		applyCmd(t, replayed, cmd)
	}
	snap, err := applied.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	restored := New()
	if err := restored.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}

	for name, f := range map[string]*FSM{"replayed": replayed, "restored": restored} {
		if got := f.Get(digested).Hash; got != digest {
			t.Errorf("%s voter holds digest %x, want %x", name, got[:4], digest[:4])
		}
		if got := f.Get(undigested).Hash; got != ([32]byte{}) {
			t.Errorf("%s voter invented a digest %x for a chunk sealed without one", name, got[:4])
		}
	}
}

// Homes verify their copies against the first digest recorded. A second seal
// of the same chunk with other bytes — a new leader proposing its own,
// different build — is refused whole: neither the digest nor the metadata it
// describes moves. A re-seal carrying the same digest, or none, still applies.
func TestSealDigestIsWriteOnce(t *testing.T) {
	t.Parallel()
	id := testChunkID(1)
	first, second := sealDigestForTest(0x01), sealDigestForTest(0x02)
	at := time.Unix(0, 1_700_000_000_000_000_000)
	f := New()
	applyCmd(t, f, MarshalCreateChunk(id, at, at, at))
	applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, first))

	result := f.Apply(&hraft.Log{Data: MarshalSealChunk(id, at, 7, 700, at, at, at, true, at, second)})
	if err, ok := result.(error); !ok || err == nil {
		t.Fatal("a seal of other bytes applied over the sealed digest")
	}
	if e := f.Get(id); e.Hash != first || e.RecordCount != 3 || e.Bytes != 300 {
		t.Fatalf("a refused seal changed the entry: hash=%x records=%d bytes=%d", e.Hash[:4], e.RecordCount, e.Bytes)
	}

	applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, first))
	applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, [32]byte{}))
	if got := f.Get(id).Hash; got != first {
		t.Fatalf("digest after re-seals = %x, want the first %x", got[:4], first[:4])
	}
}

func TestSealChunkRejectsAMalformedDigest(t *testing.T) {
	t.Parallel()
	id := testChunkID(1)
	at := time.Unix(0, 1_700_000_000_000_000_000)
	f := New()
	applyCmd(t, f, MarshalCreateChunk(id, at, at, at))
	cmd := NewSealChunk(id, at, 3, 300, at, at, at, true, at, sealDigestForTest(0x01))
	cmd.GetSealChunk().BlobDigest = cmd.GetSealChunk().GetBlobDigest()[:16]
	result := f.Apply(&hraft.Log{Data: mustMarshalCommand(cmd)})
	if err, ok := result.(error); !ok || err == nil {
		t.Fatal("a seal carrying a 16-byte digest applied")
	}
	if e := f.Get(id); e.State == chunk.ChunkStateSealed || e.Hash != ([32]byte{}) {
		t.Fatalf("a rejected seal changed the entry: state=%v hash=%x", e.State, e.Hash[:4])
	}
}

// Hash has one meaning: the digest of the agreed GLCB. A cloud upload of the
// agreed bytes confirms it; an upload of other bytes is refused rather than
// allowed to redefine what every home verifies against.
func TestUploadDigestMustMatchTheSealedDigest(t *testing.T) {
	t.Parallel()
	at := time.Unix(0, 1_700_000_000_000_000_000)
	sealed := sealDigestForTest(0x07)
	cloud := glidFromByte(0x42)

	t.Run("matching upload", func(t *testing.T) {
		t.Parallel()
		id := testChunkID(1)
		f := New()
		applyCmd(t, f, MarshalCreateChunk(id, at, at, at))
		applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, sealed))
		applyCmd(t, f, MarshalUploadChunk(id, 100, 1, 2, 3, 4, sealed, cloud, 0))
		e := f.Get(id)
		if !e.CloudBacked || e.Hash != sealed || e.CloudServiceID != cloud {
			t.Fatalf("matching upload: cloud_backed=%v hash=%x service=%v", e.CloudBacked, e.Hash[:4], e.CloudServiceID)
		}
	})

	t.Run("upload of other bytes", func(t *testing.T) {
		t.Parallel()
		id := testChunkID(1)
		f := New()
		applyCmd(t, f, MarshalCreateChunk(id, at, at, at))
		applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, sealed))
		result := f.Apply(&hraft.Log{Data: MarshalUploadChunk(id, 100, 1, 2, 3, 4, sealDigestForTest(0x08), cloud, 0)})
		if err, ok := result.(error); !ok || err == nil {
			t.Fatal("an upload whose digest differs from the sealed one applied")
		}
		if e := f.Get(id); e.CloudBacked || e.Hash != sealed {
			t.Fatalf("a refused upload changed the entry: cloud_backed=%v hash=%x", e.CloudBacked, e.Hash[:4])
		}
	})

	// A pipeline chunk's upload does not know its blob's digest; the sealed
	// digest stays, and the cloud identity is still recorded.
	t.Run("upload without a known digest", func(t *testing.T) {
		t.Parallel()
		id := testChunkID(1)
		f := New()
		applyCmd(t, f, MarshalCreateChunk(id, at, at, at))
		applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, sealed))
		applyCmd(t, f, MarshalUploadChunk(id, 100, 1, 2, 3, 4, [32]byte{}, cloud, 0))
		e := f.Get(id)
		if !e.CloudBacked || e.Hash != sealed || e.CloudServiceID != cloud {
			t.Fatalf("digest-less upload: cloud_backed=%v hash=%x service=%v, want the sealed digest kept", e.CloudBacked, e.Hash[:4], e.CloudServiceID)
		}
	})

	t.Run("chunk sealed without a digest", func(t *testing.T) {
		t.Parallel()
		id := testChunkID(1)
		uploaded := sealDigestForTest(0x09)
		f := New()
		applyCmd(t, f, MarshalCreateChunk(id, at, at, at))
		applyCmd(t, f, MarshalSealChunk(id, at, 3, 300, at, at, at, true, at, [32]byte{}))
		applyCmd(t, f, MarshalUploadChunk(id, 100, 1, 2, 3, 4, uploaded, cloud, 0))
		if got := f.Get(id).Hash; got != uploaded {
			t.Fatalf("upload of a chunk sealed without a digest recorded %x, want %x", got[:4], uploaded[:4])
		}
	})
}
