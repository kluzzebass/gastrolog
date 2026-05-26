package orchestrator

import (
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

	store := orch.vaultInterimSeqStore(vaultID)
	seq1, ok := store.LookupSeq(rec1.EventID)
	if !ok || seq1 != 1 {
		t.Fatalf("seq1 = %d, ok=%v", seq1, ok)
	}
	seq2, ok := store.LookupSeq(rec2.EventID)
	if !ok || seq2 != 2 {
		t.Fatalf("seq2 = %d, ok=%v", seq2, ok)
	}
}

func TestIngestRetryIdempotentDestinationSeq(t *testing.T) {
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

	store := orch.vaultInterimSeqStore(vaultID)
	seq, ok := store.LookupSeq(rec.EventID)
	if !ok || seq != 1 {
		t.Fatalf("seq after retry = %d ok=%v", seq, ok)
	}
	if len(store.bySeq) != 1 {
		t.Fatalf("expected one assigned seq, got %d", len(store.bySeq))
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

	store := orch.vaultInterimSeqStore(archiveID)
	if len(store.bySeq) != 2 {
		t.Fatalf("archive assignments = %d, want 2", len(store.bySeq))
	}
	if store.bySeq[1].Raw == nil || store.bySeq[2].Raw == nil {
		t.Fatal("expected stored records at seq 1 and 2")
	}
}

func TestReplicaFanOutPreservesDestinationSeq(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	followerCM, _ := chunkmem.NewManager(chunkmem.Config{})
	followerVaultID := vaultID
	followerInst := &VaultInstance{
		VaultID: followerVaultID,
		Type:   "memory",
		Chunks: followerCM,
	}
	followerVault := NewVault(followerVaultID, followerInst)
	followerVault.WriteModel = system.VaultWriteModelSequenced
	orch.RegisterVault(followerVault)
	wireTestSeqAllocator(orch, followerVaultID)

	registerSequencedTestVault(t, orch, vaultID, []system.ReplicationTarget{
		{NodeID: "node-1", StorageID: system.SyntheticStorageID("node-1")},
	})

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	rec := sequencedTestRecord("rf", glid.New(), 1)
	rec.WaitForReplica = true
	pa, err := orch.ingestWithSource(rec, SourceContext{Kind: SourceIngest})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if pa == nil || len(pa.replication) != 1 {
		t.Fatalf("replication tasks = %v", pa)
	}

	leaderStore := orch.vaultInterimSeqStore(vaultID)
	seq, ok := leaderStore.LookupSeq(rec.EventID)
	if !ok || seq != 1 {
		t.Fatalf("leader seq = %d ok=%v", seq, ok)
	}

	if err := orch.applyInterimReplicaWrite(vaultID, leaderStore.bySeq[seq]); err != nil {
		t.Fatalf("local replica apply: %v", err)
	}
	followerStore := orch.vaultInterimSeqStore(vaultID)
	if got, ok := followerStore.LookupSeq(rec.EventID); !ok || got != seq {
		t.Fatalf("follower seq = %d ok=%v want %d", got, ok, seq)
	}
}
