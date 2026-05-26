package memory

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/spool"
)

func spoolRecord(seq uint32, vaultSeq uint64) chunk.Record {
	ts := time.Unix(int64(seq), 0).UTC()
	return chunk.Record{
		SourceTS: ts,
		IngestTS: ts,
		WriteTS:  ts,
		EventID: chunk.EventID{
			IngesterID: glid.New(),
			NodeID:     glid.New(),
			IngestTS:   ts,
			IngestSeq:  seq,
		},
		Attrs:    chunk.Attributes{"n": "v"},
		Raw:      []byte("payload"),
		VaultSeq: vaultSeq,
	}
}

func TestPutSlotStoresWindowSlot(t *testing.T) {
	t.Parallel()
	m := NewManager()
	if err := m.EnsureWindow(100, 110); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecord(1, 100)); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecord(2, 101)); err != nil {
		t.Fatal(err)
	}
	meta, ok := m.Meta(spool.WindowID{Start: 100, End: 110})
	if !ok {
		t.Fatal("window metadata missing")
	}
	if meta.FirstSeq != 100 || meta.EndSeq != 110 || meta.LastSeq != 101 || meta.RecordCount != 2 {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestSealWindowMarksWindowSealed(t *testing.T) {
	t.Parallel()
	m := NewManager()
	id := spool.WindowID{Start: 50, End: 60}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecord(1, 50)); err != nil {
		t.Fatal(err)
	}
	meta, err := m.SealWindow(id)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Sealed {
		t.Fatalf("sealed meta = %+v", meta)
	}
	if err := m.PutSlot(spoolRecord(2, 51)); err != ErrWindowSealed {
		t.Fatalf("PutSlot on sealed window err = %v, want %v", err, ErrWindowSealed)
	}
}

func TestReadByVaultSeq(t *testing.T) {
	t.Parallel()
	m := NewManager()
	rec := spoolRecord(1, 7)
	if err := m.EnsureWindow(1, 10); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(rec); err != nil {
		t.Fatal(err)
	}
	got, ok := m.ReadByVaultSeq(7)
	if !ok || got.VaultSeq != 7 {
		t.Fatalf("ReadByVaultSeq = %+v ok=%v", got, ok)
	}
}

func TestSegmentMetaCoversSeqAcrossRecords(t *testing.T) {
	t.Parallel()
	m := NewManager()
	id := spool.WindowID{Start: 10, End: 20}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(10); seq <= 12; seq++ {
		if err := m.PutSlot(spoolRecord(uint32(seq), seq)); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ := m.Meta(id)
	if !meta.CoversSeq(11) {
		t.Fatal("segment should cover interior seq")
	}
}

func TestPutSlotOutOfOrder(t *testing.T) {
	t.Parallel()
	m := NewManager()
	id := spool.WindowID{Start: 100, End: 110}
	if err := m.EnsureWindow(id.Start, id.End); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecord(2, 103)); err != nil {
		t.Fatal(err)
	}
	if err := m.PutSlot(spoolRecord(1, 101)); err != nil {
		t.Fatal(err)
	}
	meta, ok := m.Meta(id)
	if !ok {
		t.Fatal("window missing")
	}
	if meta.RecordCount != 2 || meta.LastSeq != 103 {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestPutSlotRequiresWindow(t *testing.T) {
	t.Parallel()
	m := NewManager()
	err := m.PutSlot(spoolRecord(1, 9))
	if err == nil {
		t.Fatal("expected error")
	}
}
