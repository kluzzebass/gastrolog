package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// ---------- tier transition benchmarks ----------
//
// Measures throughput of the tier transition pipeline at different record counts.
// Catches regressions in seal latency, transition streaming, and record count
// accuracy under burst load.

// benchFileTier creates a file-backed TierInstance for benchmarks.
func benchFileTier(b *testing.B, tierID uuid.UUID) *TierInstance {
	b.Helper()
	dir := b.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(200000),
	})
	if err != nil {
		b.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)
	return &TierInstance{
		TierID:  tierID,
		Type:    "file",
		Chunks:  cm,
		Indexes: im,
		Query:   query.New(cm, im, nil),
	}
}

// benchCloudFileTier creates a file-backed TierInstance with cloud storage.
// Uses a 200K rotation policy to avoid auto-rotation within benchmark bursts.
func benchCloudFileTier(b *testing.B, tierID, vaultID uuid.UUID, store blobstore.Store) *TierInstance {
	b.Helper()
	dir := b.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            dir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(200000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		b.Fatal(err)
	}
	return &TierInstance{
		TierID: tierID,
		Type:   "cloud",
		Chunks: cm,
	}
}

// benchRetentionRunner creates a retention runner for benchmarks.
func benchRetentionRunner(orch *Orchestrator, vaultID, tierID uuid.UUID, tier *TierInstance) *retentionRunner {
	return &retentionRunner{
		isLeader: true,
		vaultID:  vaultID,
		tierID:   tierID,
		cm:       tier.Chunks,
		im:       tier.Indexes,
		orch:     orch,
		now:      time.Now,
		logger:   slog.Default(),
	}
}

// benchTransitionSetup creates an N-tier chain and returns the orchestrator,
// IDs (vaultID + tierIDs), and tier instances.
func benchTransitionSetup(b *testing.B, tierCount int, withCloud bool) (*Orchestrator, []uuid.UUID, []*TierInstance) {
	b.Helper()
	nodeID := "bench-node"
	vaultID := uuid.Must(uuid.NewV7())

	tierIDs := make([]uuid.UUID, tierCount)
	tiers := make([]*TierInstance, tierCount)
	tierCfgs := make([]system.TierConfig, tierCount)

	for i := range tierCount {
		tierIDs[i] = uuid.Must(uuid.NewV7())
		if withCloud && i == tierCount-1 {
			cloudStore := blobstore.NewMemory()
			tiers[i] = benchCloudFileTier(b, tierIDs[i], vaultID, cloudStore)
			tierCfgs[i] = system.TierConfig{
				ID: tierIDs[i], Name: fmt.Sprintf("tier-%d", i),
				VaultID: vaultID, Position: uint32(i),
			}
		} else {
			tiers[i] = benchFileTier(b, tierIDs[i])
			tierCfgs[i] = system.TierConfig{
				ID: tierIDs[i], Name: fmt.Sprintf("tier-%d", i),
				VaultID: vaultID, Position: uint32(i),
			}
		}
	}

	orch, err := New(Config{LocalNodeID: nodeID})
	if err != nil {
		b.Fatal(err)
	}

	vault := NewVault(vaultID, tiers...)
	vault.Name = "bench"
	orch.RegisterVault(vault)

	store := sysmem.NewStore()
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "bench",
	})
	for _, tc := range tierCfgs {
		_ = store.PutTier(context.Background(), tc)
	}
	orch.sysLoader = &transitionSystemLoader{store: store}

	return orch, append([]uuid.UUID{vaultID}, tierIDs...), tiers
}

// countAllTierRecords is defined in transition_test.go with testing.TB.

// BenchmarkTransitionThroughput measures records/second through a 2-tier
// memory→memory transition at varying burst sizes.
func BenchmarkTransitionThroughput(b *testing.B) {
	for _, size := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			for range b.N {
				orch, ids, tiers := benchTransitionSetup(b, 2, false)
				vaultID, tier0ID := ids[0], ids[1]
				tier0 := tiers[0]
				tier1 := tiers[1]

				// Burst ingest.
				t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
				for i := range size {
					ts := t0.Add(time.Duration(i) * time.Microsecond)
					if _, _, err := tier0.Chunks.Append(chunk.Record{
						IngestTS: ts,
						WriteTS:  ts,
						Raw:      fmt.Appendf(nil, "bench-%d", i),
					}); err != nil {
						b.Fatal(err)
					}
				}

				// Seal.
				if err := tier0.Chunks.Seal(); err != nil {
					b.Fatal(err)
				}

				// Transition.
				metas, _ := tier0.Chunks.List()
				runner := benchRetentionRunner(orch, vaultID, tier0ID, tier0)
				for _, m := range metas {
					runner.transitionChunk(m.ID)
				}

				// Verify record count.
				got := countAllTierRecords(b, tier1.Chunks)
				if got != int64(size) {
					b.Fatalf("expected %d records in tier 1, got %d", size, got)
				}
			}
		})
	}
}

// BenchmarkTransitionThreeTierChain measures throughput through a 3-tier
// memory→memory→memory chain at varying burst sizes.
func BenchmarkTransitionThreeTierChain(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			for range b.N {
				orch, ids, tiers := benchTransitionSetup(b, 3, false)
				vaultID := ids[0]
				tierIDs := ids[1:]

				// Burst ingest into tier 0.
				t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
				for i := range size {
					ts := t0.Add(time.Duration(i) * time.Microsecond)
					if _, _, err := tiers[0].Chunks.Append(chunk.Record{
						IngestTS: ts,
						WriteTS:  ts,
						Raw:      fmt.Appendf(nil, "chain-%d", i),
					}); err != nil {
						b.Fatal(err)
					}
				}

				// Transition through each tier.
				for tierIdx := 0; tierIdx < 2; tierIdx++ {
					cm := tiers[tierIdx].Chunks
					if err := cm.Seal(); err != nil {
						b.Fatal(err)
					}
					metas, _ := cm.List()
					runner := benchRetentionRunner(orch, vaultID, tierIDs[tierIdx], tiers[tierIdx])
					for _, m := range metas {
						runner.transitionChunk(m.ID)
					}
				}

				// Verify: all records in final tier, none in earlier tiers.
				got := countAllTierRecords(b, tiers[2].Chunks)
				if got != int64(size) {
					b.Fatalf("expected %d records in tier 2, got %d", size, got)
				}
			}
		})
	}
}

// BenchmarkTransitionToCloud measures throughput of transitioning records
// into a cloud-backed tier (including PostSealProcess upload).
func BenchmarkTransitionToCloud(b *testing.B) {
	for _, size := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			for range b.N {
				orch, ids, tiers := benchTransitionSetup(b, 2, true)
				vaultID, tier0ID := ids[0], ids[1]
				tier0 := tiers[0]
				cloudTier := tiers[1]

				// Burst ingest.
				t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
				for i := range size {
					ts := t0.Add(time.Duration(i) * time.Microsecond)
					if _, _, err := tier0.Chunks.Append(chunk.Record{
						IngestTS: ts,
						WriteTS:  ts,
						Raw:      fmt.Appendf(nil, "cloud-%d", i),
					}); err != nil {
						b.Fatal(err)
					}
				}

				// Seal and transition to cloud tier.
				if err := tier0.Chunks.Seal(); err != nil {
					b.Fatal(err)
				}
				metas, _ := tier0.Chunks.List()
				runner := benchRetentionRunner(orch, vaultID, tier0ID, tier0)
				for _, m := range metas {
					runner.transitionChunk(m.ID)
				}

				// Seal cloud tier and upload.
				if err := cloudTier.Chunks.Seal(); err != nil {
					b.Fatal(err)
				}
				cloudMetas, _ := cloudTier.Chunks.List()
				processor := cloudTier.Chunks.(chunk.ChunkPostSealProcessor)
				for _, m := range cloudMetas {
					if err := processor.PostSealProcess(context.Background(), m.ID); err != nil {
						b.Fatalf("PostSealProcess: %v", err)
					}
				}

				// Verify record count.
				got := countAllTierRecords(b, cloudTier.Chunks)
				if got != int64(size) {
					b.Fatalf("expected %d records in cloud tier, got %d", size, got)
				}
			}
		})
	}
}
