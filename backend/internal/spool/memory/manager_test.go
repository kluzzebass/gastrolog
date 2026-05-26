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

func TestAppendOpensSegmentByFirstSeq(t *testing.T) {
	t.Parallel()
	m := NewManager()
	meta, err := m.Append(spoolRecord(1, 100))
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != spool.SegmentID(100) || meta.FirstSeq != 100 || meta.LastSeq != 100 {
		t.Fatalf("meta = %+v", meta)
	}
	meta2, err := m.Append(spoolRecord(2, 101))
	if err != nil {
		t.Fatal(err)
	}
	if meta2.FirstSeq != 100 || meta2.LastSeq != 101 || meta2.RecordCount != 2 {
		t.Fatalf("after second append meta = %+v", meta2)
	}
}

func TestSealStartsNewSegment(t *testing.T) {
	t.Parallel()
	m := NewManager()
	if _, err := m.Append(spoolRecord(1, 50)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SealActive(); err != nil {
		t.Fatal(err)
	}
	meta, err := m.Append(spoolRecord(2, 200))
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != spool.SegmentID(200) || meta.FirstSeq != 200 {
		t.Fatalf("new segment meta = %+v", meta)
	}
	segs := m.ListSegments()
	if len(segs) != 2 || segs[0].FirstSeq != 50 || segs[1].FirstSeq != 200 {
		t.Fatalf("segments = %+v", segs)
	}
}

func TestReadByVaultSeq(t *testing.T) {
	t.Parallel()
	m := NewManager()
	rec := spoolRecord(1, 7)
	if _, err := m.Append(rec); err != nil {
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
	for seq := uint64(10); seq <= 12; seq++ {
		if _, err := m.Append(spoolRecord(uint32(seq), seq)); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ := m.Meta(spool.SegmentID(10))
	if !meta.CoversSeq(11) {
		t.Fatal("segment should cover interior seq")
	}
}
