package orchestrator

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

func sequencedTestRecord(raw string, ingesterID glid.GLID, seq uint32) chunk.Record {
	now := time.Now().Truncate(time.Nanosecond)
	return chunk.Record{
		SourceTS: now,
		IngestTS: now,
		Attrs:    chunk.Attributes{"msg": raw},
		Raw:      []byte(raw),
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			NodeID:     glid.New(),
			IngestTS:   now,
			IngestSeq:  seq,
		},
	}
}

func registerSequencedTestVault(t *testing.T, orch *Orchestrator, vaultID glid.GLID, followers []system.ReplicationTarget) chunk.ChunkManager {
	t.Helper()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	im := indexmem.NewManager(nil, nil, nil, nil, nil)
	qe := query.New(cm, im, nil)
	v := NewVault(vaultID, &VaultInstance{
		VaultID:          vaultID,
		Type:            "memory",
		Chunks:          cm,
		Indexes:         im,
		Query:           qe,
		FollowerTargets: followers,
	})
	v.WriteModel = system.VaultWriteModelSequenced
	v.ReplicationFactor = 1
	if len(followers) > 0 {
		v.ReplicationFactor = uint32(len(followers) + 1)
	}
	orch.RegisterVault(v)
	wireTestSeqAllocator(orch, vaultID)
	return cm
}

func TestIngestAssignsMonotonicDestinationSeq(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	registerSequencedTestVault(t, orch, vaultID, nil)

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	ingester := glid.New()
	rec1 := sequencedTestRecord("a", ingester, 1)
	rec2 := sequencedTestRecord("b", ingester, 2)

	if err := orch.Ingest(rec1); err != nil {
		t.Fatalf("ingest 1: %v", err)
	}
	if err := orch.Ingest(rec2); err != nil {
		t.Fatalf("ingest 2: %v", err)
	}

	store := orch.vaultSpoolStore(vaultID)
	seq1, ok := store.LookupSeq(rec1.EventID)
	if !ok || seq1 != 1 {
		t.Fatalf("seq1 = %d, ok=%v", seq1, ok)
	}
	seq2, ok := store.LookupSeq(rec2.EventID)
	if !ok || seq2 != 2 {
		t.Fatalf("seq2 = %d, ok=%v", seq2, ok)
	}
	if got := store.IngestHighWatermark(); got != 2 {
		t.Fatalf("H = %d, want 2", got)
	}
}

func TestIngestRetryConsumesNewDestinationSeq(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	registerSequencedTestVault(t, orch, vaultID, nil)

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	rec := sequencedTestRecord("retry", glid.New(), 1)
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("retry ingest: %v", err)
	}

	store := orch.vaultSpoolStore(vaultID)
	if got := store.IngestHighWatermark(); got != 2 {
		t.Fatalf("H after retry = %d, want 2 (each accept consumes a seq)", got)
	}
}

func TestRetentionRouteAssignsDestinationSeq(t *testing.T) {
	t.Parallel()
	sourceID := glid.New()
	archiveID := glid.New()

	sourceCM, _ := chunkmem.NewManager(chunkmem.Config{RotationPolicy: chunk.NewRecordCountPolicy(100)})
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	orch.RegisterVault(NewVaultFromComponents(sourceID, sourceCM, nil, nil))

	registerSequencedTestVault(t, orch, archiveID, nil)

	expr := `_source="retention" AND _vault="` + sourceID.String() + `"`
	cr, _ := CompileRoute(glid.New(), "retain", 0, expr, []RouteDestination{{VaultID: archiveID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	ingester := glid.New()
	for i := range 2 {
		rec := sequencedTestRecord(string(rune('a'+i)), ingester, uint32(i+1))
		if _, _, err := sourceCM.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := sourceCM.Seal(); err != nil {
		t.Fatal(err)
	}
	metas, _ := sourceCM.List()
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}

	runner := &retentionRunner{
		vaultID:     sourceID,
		disposition: system.RetentionDispositionRoute,
		cm:          sourceCM,
		orch:        orch,
		logger:      orch.logger,
	}
	runner.fireRetentionEvent(sealedID)

	store := orch.vaultSpoolStore(archiveID)
	if store.IngestHighWatermark() != 2 {
		t.Fatalf("archive H = %d, want 2", store.IngestHighWatermark())
	}
	if store.SpoolDurableWatermark() != 2 {
		t.Fatalf("archive spool watermark = %d, want 2", store.SpoolDurableWatermark())
	}
}

func TestReplicaFanOutPreservesDestinationSeq(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	registerSequencedTestVault(t, orch, vaultID, []system.ReplicationTarget{
		{NodeID: "node-1", StorageID: system.SyntheticStorageID("node-1")},
	})
	v := orch.vaults[vaultID]
	v.ReplicationFactor = 2

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	rec := sequencedTestRecord("rf", glid.New(), 1)
	rec.WaitForReplica = true
	pa, err := orch.ingestWithSource(&rec, SourceContext{Kind: SourceIngest})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if pa == nil || len(pa.replication) != 1 {
		t.Fatalf("replication tasks = %v", pa)
	}

	leaderStore := orch.vaultSpoolStore(vaultID)
	seq, ok := leaderStore.LookupSeq(rec.EventID)
	if !ok || seq != 1 {
		t.Fatalf("leader seq = %d ok=%v", seq, ok)
	}
	if leaderStore.IngestHighWatermark() != 0 {
		t.Fatalf("H before replication = %d, want 0", leaderStore.IngestHighWatermark())
	}

	stored, err := leaderStore.ReadByVaultSeq(nil, vaultID, seq)
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	if err := orch.applySpoolReplicaWrite(vaultID, stored); err != nil {
		t.Fatalf("local replica apply: %v", err)
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, pa, rec)
	if err := <-ack; err != nil {
		t.Fatalf("ack replication: %v", err)
	}
	if got := leaderStore.IngestHighWatermark(); got != seq {
		t.Fatalf("H after replication = %d, want %d", got, seq)
	}
}

func TestIngestHighWatermarkBlockedOnFailedReplication(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	orch.chunkReplicator = &failAllReplicator{}
	registerSequencedTestVault(t, orch, vaultID, []system.ReplicationTarget{
		{NodeID: "node-2", StorageID: system.SyntheticStorageID("node-2")},
	})
	v := orch.vaults[vaultID]
	v.ReplicationFactor = 2

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	rec := sequencedTestRecord("fail-rf", glid.New(), 1)
	rec.WaitForReplica = true
	pa, err := orch.ingestWithSource(&rec, SourceContext{Kind: SourceIngest})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	ack := make(chan error, 1)
	orch.ackAfterReplication(ack, pa, rec)
	if err := <-ack; err == nil {
		t.Fatal("expected replication failure")
	}
	if got := orch.vaultSpoolStore(vaultID).IngestHighWatermark(); got != 0 {
		t.Fatalf("H after failed replication = %d, want 0", got)
	}
	if got := orch.vaultSpoolStore(vaultID).SpoolDurableWatermark(); got != 1 {
		t.Fatalf("spool watermark = %d, want 1 (local spool only)", got)
	}
}

type failAllReplicator struct{}

func (f *failAllReplicator) AppendRecords(_ context.Context, _ string, _ glid.GLID, _ chunk.ChunkID, _ []chunk.Record) error {
	return chunk.ErrChunkNotFound
}

func (f *failAllReplicator) ImportSealedChunk(context.Context, string, glid.GLID, chunk.ChunkID, chunk.RecordIterator) error {
	return nil
}

func (f *failAllReplicator) SealVault(context.Context, string, glid.GLID, chunk.ChunkID) error {
	return nil
}

func (f *failAllReplicator) DeleteChunk(context.Context, string, glid.GLID, chunk.ChunkID) error {
	return nil
}

func (f *failAllReplicator) RequestReplicaCatchup(context.Context, string, glid.GLID, []chunk.ChunkID, string) (uint32, error) {
	return 0, nil
}
