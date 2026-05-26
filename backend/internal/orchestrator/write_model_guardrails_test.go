package orchestrator

import (
	"errors"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// countingChunkManager wraps a memory manager and counts Append calls.
type countingChunkManager struct {
	chunk.ChunkManager
	appends int
}

func (c *countingChunkManager) Append(record chunk.Record) (chunk.ChunkID, uint64, error) {
	c.appends++
	return c.ChunkManager.Append(record)
}

func activeRecordCount(t *testing.T, cm chunk.ChunkManager) int64 {
	t.Helper()
	active := cm.Active()
	if active == nil {
		return 0
	}
	meta, err := cm.Meta(active.ID)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	return meta.RecordCount
}

func TestRouteFanOutSeparateFromReplicaFanOut(t *testing.T) {
	t.Parallel()

	vaultA := glid.New()
	vaultB := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})

	cmA, _ := chunkmem.NewManager(chunkmem.Config{})
	cmB, _ := chunkmem.NewManager(chunkmem.Config{})
	registerVault := func(id glid.GLID, cm chunk.ChunkManager, followers []system.ReplicationTarget) {
		im := indexmem.NewManager(nil, nil, nil, nil, nil)
		qe := query.New(cm, im, nil)
		inst := &VaultInstance{
			VaultID:          id,
			Type:            "memory",
			Chunks:          cm,
			Indexes:         im,
			Query:           qe,
			FollowerTargets: followers,
		}
		v := NewVault(id, inst)
		v.Name = id.String()
		orch.RegisterVault(v)
	}
	registerVault(vaultA, cmA, nil)
	registerVault(vaultB, cmB, []system.ReplicationTarget{{NodeID: "node-2"}})

	route, _ := CompileRoute(glid.New(), "both", 0, "*",
		[]RouteDestination{{VaultID: vaultA}, {VaultID: vaultB}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{route}))

	rec := testRecord("fan-out-boundary")
	rec.WaitForReplica = true
	pa, err := orch.ingestWithSource(rec, SourceContext{Kind: SourceIngest})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if gotA := activeRecordCount(t, cmA); gotA != 1 {
		t.Fatalf("vault A records = %d, want 1 (route fan-out)", gotA)
	}
	if gotB := activeRecordCount(t, cmB); gotB != 1 {
		t.Fatalf("vault B records = %d, want 1 (route fan-out)", gotB)
	}
	if pa == nil || len(pa.replication) != 1 {
		t.Fatalf("replica fan-out tasks = %d, want 1 for RF vault B only", len(pa.replication))
	}
	if pa.replication[0].vaultID != vaultB {
		t.Fatalf("replication vault = %s, want %s", pa.replication[0].vaultID, vaultB)
	}
}

func TestSequencedWriteGateRejectsWithoutAllocator(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	counting := &countingChunkManager{ChunkManager: cm}
	im := indexmem.NewManager(nil, nil, nil, nil, nil)
	qe := query.New(counting, im, nil)
	v := NewVault(vaultID, &VaultInstance{
		VaultID: vaultID,
		Type:   "memory",
		Chunks: counting,
		Indexes: im,
		Query:  qe,
	})
	v.WriteModel = system.VaultWriteModelSequenced
	orch.RegisterVault(v)
	// No wireTestSeqAllocator — sequenced ingest stays gated until allocator is wired.

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	_, err := orch.ingestWithSource(sequencedTestRecord("v2-gated", glid.New(), 1), SourceContext{Kind: SourceIngest})
	if !errors.Is(err, ErrSequencedWriteUnavailable) {
		t.Fatalf("ingest err = %v, want %v", err, ErrSequencedWriteUnavailable)
	}
	if counting.appends != 0 {
		t.Fatalf("sequenced path must not append to active chunk synchronously; appends=%d", counting.appends)
	}
}

func TestDefaultChunkAppendWriteModelUnchanged(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	im := indexmem.NewManager(nil, nil, nil, nil, nil)
	qe := query.New(cm, im, nil)
	v := NewVault(vaultID, &VaultInstance{VaultID: vaultID, Type: "memory", Chunks: cm, Indexes: im, Query: qe})
	v.WriteModel = system.VaultWriteModelChunkAppend
	orch.RegisterVault(v)

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	if err := orch.Ingest(testRecord("v1-default")); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if n := activeRecordCount(t, cm); n != 1 {
		t.Fatalf("records = %d, want 1", n)
	}
}

func TestRouteFanOutMatchesMatchesRouteSet(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	cr, err := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "fanout")
	if err != nil {
		t.Fatal(err)
	}
	rs := NewRouteSet([]*CompiledRoute{cr})
	attrs := chunk.Attributes{"env": "prod"}
	direct := rs.MatchWithSource(attrs, SourceContext{Kind: SourceIngest})
	via := RouteFanOutMatches(rs, attrs, SourceContext{Kind: SourceIngest})
	if len(direct) != len(via) || len(direct) != 1 || direct[0].VaultID != via[0].VaultID {
		t.Fatalf("RouteFanOutMatches diverged: direct=%v via=%v", direct, via)
	}
}

func TestSyncVaultConfigUpdatesWriteModel(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{VaultID: vaultID, Type: "memory", Chunks: cm}))

	if err := orch.SyncVaultConfig(system.VaultConfig{
		ID: vaultID, Name: "t", Enabled: true, WriteModel: string(system.VaultWriteModelSequenced),
	}); err != nil {
		t.Fatal(err)
	}
	if got := orch.vaultWriteModel(vaultID); got != system.VaultWriteModelSequenced {
		t.Fatalf("write model = %q, want %q", got, system.VaultWriteModelSequenced)
	}
}
