package orchestrator_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
)

func newFacadeSetup(t *testing.T) (*orchestrator.Orchestrator, uuid.UUID) {
	t.Helper()
	s := memtest.MustNewStore(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(5),
	})
	id := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterStore(orchestrator.NewStore(id, s.CM, s.IM, s.QE))
	return orch, id
}

func appendRecords(t *testing.T, orch *orchestrator.Orchestrator, storeID uuid.UUID, n int) {
	t.Helper()
	for i := range n {
		ts := time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC)
		rec := chunk.Record{
			IngestTS: ts,
			Raw:      []byte("msg"),
		}
		if _, _, err := orch.Append(storeID, rec); err != nil {
			t.Fatalf("Append record %d: %v", i, err)
		}
	}
}

func TestStoreExists(t *testing.T) {
	orch, id := newFacadeSetup(t)
	if !orch.StoreExists(id) {
		t.Fatal("StoreExists returned false for registered store")
	}
	if orch.StoreExists(uuid.Must(uuid.NewV7())) {
		t.Fatal("StoreExists returned true for unknown store")
	}
}

func TestListChunkMetas(t *testing.T) {
	orch, id := newFacadeSetup(t)
	appendRecords(t, orch, id, 3)

	metas, err := orch.ListChunkMetas(id)
	if err != nil {
		t.Fatalf("ListChunkMetas: %v", err)
	}
	if len(metas) == 0 {
		t.Fatal("expected at least one chunk")
	}
}

func TestListChunkMetas_UnknownStore(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	_, err := orch.ListChunkMetas(uuid.Must(uuid.NewV7()))
	if !errors.Is(err, orchestrator.ErrStoreNotFound) {
		t.Fatalf("expected ErrStoreNotFound, got %v", err)
	}
}

func TestGetChunkMeta(t *testing.T) {
	orch, id := newFacadeSetup(t)
	appendRecords(t, orch, id, 1)

	metas, err := orch.ListChunkMetas(id)
	if err != nil {
		t.Fatalf("ListChunkMetas: %v", err)
	}

	meta, err := orch.GetChunkMeta(id, metas[0].ID)
	if err != nil {
		t.Fatalf("GetChunkMeta: %v", err)
	}
	if meta.ID != metas[0].ID {
		t.Fatalf("chunk ID mismatch: got %s, want %s", meta.ID, metas[0].ID)
	}
}

func TestSealActive(t *testing.T) {
	orch, id := newFacadeSetup(t)
	appendRecords(t, orch, id, 3)

	if err := orch.SealActive(id); err != nil {
		t.Fatalf("SealActive: %v", err)
	}

	metas, err := orch.ListChunkMetas(id)
	if err != nil {
		t.Fatal(err)
	}
	// After sealing, the previously active chunk should be sealed and a new active may exist.
	var foundSealed bool
	for _, m := range metas {
		if m.Sealed && m.RecordCount > 0 {
			foundSealed = true
		}
	}
	if !foundSealed {
		t.Fatal("expected to find a sealed chunk with records")
	}
}

func TestSealActive_Empty(t *testing.T) {
	orch, id := newFacadeSetup(t)
	// No records appended — seal should be a no-op.
	if err := orch.SealActive(id); err != nil {
		t.Fatalf("SealActive on empty store: %v", err)
	}
}

func TestSealActive_UnknownStore(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	err := orch.SealActive(uuid.Must(uuid.NewV7()))
	if !errors.Is(err, orchestrator.ErrStoreNotFound) {
		t.Fatalf("expected ErrStoreNotFound, got %v", err)
	}
}

func TestOpenCursor(t *testing.T) {
	orch, id := newFacadeSetup(t)
	appendRecords(t, orch, id, 2)

	metas, _ := orch.ListChunkMetas(id)
	cursor, err := orch.OpenCursor(id, metas[0].ID)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	defer cursor.Close()

	_, _, err = cursor.Next()
	if err != nil {
		t.Fatalf("cursor.Next: %v", err)
	}
}

func TestAppend(t *testing.T) {
	orch, id := newFacadeSetup(t)
	rec := chunk.Record{
		IngestTS: time.Now(),
		Raw:      []byte("hello"),
	}
	chunkID, pos, err := orch.Append(id, rec)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if chunkID == (chunk.ChunkID{}) {
		t.Fatal("expected non-zero chunk ID")
	}
	_ = pos
}

func TestAppend_UnknownStore(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	_, _, err := orch.Append(uuid.Must(uuid.NewV7()), chunk.Record{Raw: []byte("x")})
	if !errors.Is(err, orchestrator.ErrStoreNotFound) {
		t.Fatalf("expected ErrStoreNotFound, got %v", err)
	}
}

func TestIndexOps(t *testing.T) {
	orch, id := newFacadeSetup(t)
	// Append enough to trigger rotation (policy is 5 records).
	appendRecords(t, orch, id, 6)

	metas, _ := orch.ListChunkMetas(id)
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}
	if sealedID == (chunk.ChunkID{}) {
		t.Fatal("no sealed chunk found after appending 6 records")
	}

	// Build indexes.
	if err := orch.BuildIndexes(context.Background(), id, sealedID); err != nil {
		t.Fatalf("BuildIndexes: %v", err)
	}

	// Check completeness.
	complete, err := orch.IndexesComplete(id, sealedID)
	if err != nil {
		t.Fatalf("IndexesComplete: %v", err)
	}
	if !complete {
		t.Fatal("expected indexes to be complete after build")
	}

	// Get sizes.
	sizes, err := orch.IndexSizes(id, sealedID)
	if err != nil {
		t.Fatalf("IndexSizes: %v", err)
	}
	if len(sizes) == 0 {
		t.Fatal("expected non-empty index sizes after build")
	}

	// Delete indexes.
	if err := orch.DeleteIndexes(id, sealedID); err != nil {
		t.Fatalf("DeleteIndexes: %v", err)
	}

	complete2, _ := orch.IndexesComplete(id, sealedID)
	if complete2 {
		t.Fatal("expected indexes incomplete after deletion")
	}
}

func TestChunkIndexInfos(t *testing.T) {
	orch, id := newFacadeSetup(t)
	appendRecords(t, orch, id, 6)

	metas, _ := orch.ListChunkMetas(id)
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}

	if err := orch.BuildIndexes(context.Background(), id, sealedID); err != nil {
		t.Fatalf("BuildIndexes: %v", err)
	}

	report, err := orch.ChunkIndexInfos(id, sealedID)
	if err != nil {
		t.Fatalf("ChunkIndexInfos: %v", err)
	}
	if !report.Sealed {
		t.Fatal("expected sealed=true in report")
	}
	if len(report.Indexes) != 7 {
		t.Fatalf("expected 7 indexes, got %d", len(report.Indexes))
	}
	// Token index should exist after build.
	if !report.Indexes[0].Exists {
		t.Fatal("expected token index to exist")
	}
}

func TestNewAnalyzer(t *testing.T) {
	orch, id := newFacadeSetup(t)
	a, err := orch.NewAnalyzer(id)
	if err != nil {
		t.Fatalf("NewAnalyzer: %v", err)
	}
	if a == nil {
		t.Fatal("expected non-nil analyzer")
	}
}

func TestNewAnalyzer_UnknownStore(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	_, err := orch.NewAnalyzer(uuid.Must(uuid.NewV7()))
	if !errors.Is(err, orchestrator.ErrStoreNotFound) {
		t.Fatalf("expected ErrStoreNotFound, got %v", err)
	}
}

func TestSupportsChunkMove_MemoryStores(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	s1 := memtest.MustNewStore(t, chunkmem.Config{})
	s2 := memtest.MustNewStore(t, chunkmem.Config{})
	id1 := uuid.Must(uuid.NewV7())
	id2 := uuid.Must(uuid.NewV7())
	orch.RegisterStore(orchestrator.NewStore(id1, s1.CM, s1.IM, s1.QE))
	orch.RegisterStore(orchestrator.NewStore(id2, s2.CM, s2.IM, s2.QE))

	// Memory stores don't support ChunkMover.
	if orch.SupportsChunkMove(id1, id2) {
		t.Fatal("memory stores should not support chunk move")
	}
}

func TestCopyRecords(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	srcStore := memtest.MustNewStore(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(5),
	})
	dstStore := memtest.MustNewStore(t, chunkmem.Config{})

	srcID := uuid.Must(uuid.NewV7())
	dstID := uuid.Must(uuid.NewV7())
	orch.RegisterStore(orchestrator.NewStore(srcID, srcStore.CM, srcStore.IM, srcStore.QE))
	orch.RegisterStore(orchestrator.NewStore(dstID, dstStore.CM, dstStore.IM, dstStore.QE))

	// Append records to source.
	for i := range 8 {
		ts := time.Date(2025, 1, 1, 0, 0, i, 0, time.UTC)
		rec := chunk.Record{IngestTS: ts, Raw: []byte("msg")}
		if _, _, err := srcStore.CM.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Submit and run the copy synchronously.
	job := &orchestrator.JobProgress{}
	if err := orch.CopyRecords(context.Background(), srcID, dstID, job); err != nil {
		t.Fatalf("CopyRecords: %v", err)
	}

	// Verify destination has records.
	dstMetas, err := orch.ListChunkMetas(dstID)
	if err != nil {
		t.Fatal(err)
	}
	var totalRecords int64
	for _, m := range dstMetas {
		totalRecords += m.RecordCount
	}
	if totalRecords != 8 {
		t.Fatalf("expected 8 records in destination, got %d", totalRecords)
	}
}
