package query_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

type stubSpoolReader struct {
	bySeq map[uint64]chunk.Record
}

func (s *stubSpoolReader) ReadByVaultSeq(_ context.Context, _ glid.GLID, seq uint64) (chunk.Record, error) {
	rec, ok := s.bySeq[seq]
	if !ok {
		return chunk.Record{}, errors.New("spool record not found")
	}
	return rec, nil
}

func TestValidateContextRef(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	chunkID := chunk.ChunkID(glid.New())

	if err := query.ValidateContextRef(query.ContextRef{
		VaultID: vaultID, ChunkID: chunkID, Pos: 1,
	}); err != nil {
		t.Fatalf("materialized: %v", err)
	}
	if err := query.ValidateContextRef(query.ContextRef{
		VaultID: vaultID, VaultSeq: 42,
	}); err != nil {
		t.Fatalf("vault_seq: %v", err)
	}
	if err := query.ValidateContextRef(query.ContextRef{
		VaultID: vaultID, ChunkID: chunkID, Pos: 1, VaultSeq: 2,
	}); err == nil {
		t.Fatal("expected error for both chunk ref and vault_seq")
	}
	if err := query.ValidateContextRef(query.ContextRef{VaultID: vaultID}); err == nil {
		t.Fatal("expected error for empty anchor")
	}
}

func TestGetContextMaterializedAnchor(t *testing.T) {
	t.Parallel()
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	records := []chunk.Record{
		{IngestTS: t1, WriteTS: t1, Raw: []byte("one"), EventID: eventID(1)},
		{IngestTS: t2, WriteTS: t2, Raw: []byte("two"), EventID: eventID(2)},
		{IngestTS: t3, WriteTS: t3, Raw: []byte("three"), EventID: eventID(3)},
	}
	for _, rec := range records {
		if _, _, err := s.CM.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	memtest.BuildIndexes(t, s.CM, s.IM)

	active := s.CM.Active()
	meta, err := s.CM.Meta(active.ID)
	if err != nil {
		t.Fatal(err)
	}
	anchorPos := uint64(meta.RecordCount - 1)

	eng := query.New(s.CM, s.IM, nil)
	result, err := eng.GetContext(context.Background(), query.ContextRef{
		VaultID: glid.New(),
		ChunkID: active.ID,
		Pos:     anchorPos,
	}, 1, 1)
	if err != nil {
		t.Fatalf("GetContext: %v", err)
	}
	if string(result.Anchor.Raw) != "three" {
		t.Fatalf("anchor = %q, want three", result.Anchor.Raw)
	}
	if len(result.Before) != 1 || string(result.Before[0].Raw) != "two" {
		t.Fatalf("before = %#v", result.Before)
	}
}

func TestGetContextVaultSeqAnchorMixedLifecycle(t *testing.T) {
	t.Parallel()
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	vaultID := glid.New()
	anchorID := eventID(99)
	spoolRec := chunk.Record{
		IngestTS: t2,
		WriteTS:  t2,
		Raw:      []byte("spool-anchor"),
		EventID:  anchorID,
	}
	for _, rec := range []chunk.Record{
		{IngestTS: t1, WriteTS: t1, Raw: []byte("before"), EventID: eventID(1)},
		{IngestTS: t2, WriteTS: t2, Raw: []byte("dup-in-chunk"), EventID: anchorID, Ref: chunk.RecordRef{Pos: 1}},
		{IngestTS: t3, WriteTS: t3, Raw: []byte("after"), EventID: eventID(3)},
	} {
		if _, _, err := s.CM.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	memtest.BuildIndexes(t, s.CM, s.IM)

	eng := query.New(s.CM, s.IM, nil)
	eng.SetSpoolAnchorReader(&stubSpoolReader{bySeq: map[uint64]chunk.Record{42: spoolRec}})

	result, err := eng.GetContext(context.Background(), query.ContextRef{
		VaultID:  vaultID,
		VaultSeq: 42,
	}, 2, 1)
	if err != nil {
		t.Fatalf("GetContext: %v", err)
	}
	if result.Anchor.EventID != anchorID {
		t.Fatalf("anchor EventID mismatch")
	}
	if string(result.Anchor.Raw) != "spool-anchor" {
		t.Fatalf("anchor raw = %q", result.Anchor.Raw)
	}
	for _, rec := range append(result.Before, result.After...) {
		if rec.EventID == anchorID {
			t.Fatalf("anchor duplicate in context window: %q", rec.Raw)
		}
	}
}

func TestGetContextVaultSeqWithoutReader(t *testing.T) {
	t.Parallel()
	eng := query.New(nil, nil, nil)
	_, err := eng.GetContext(context.Background(), query.ContextRef{
		VaultID: glid.New(), VaultSeq: 1,
	}, 1, 1)
	if !errors.Is(err, query.ErrSpoolAnchorNotAvailable) {
		t.Fatalf("err = %v, want ErrSpoolAnchorNotAvailable", err)
	}
}

func eventID(seq uint32) chunk.EventID {
	return chunk.EventID{
		IngesterID: glid.New(),
		NodeID:     glid.New(),
		IngestTS:   time.Unix(int64(seq), 0).UTC(),
		IngestSeq:  seq,
	}
}
