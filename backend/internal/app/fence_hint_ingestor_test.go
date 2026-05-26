package app

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

func TestFenceHintIngestorLeaderNotHolderPublishesFromRemoteNodeStats(t *testing.T) {
	t.Parallel()

	leaderOrch, vaultID := newLeaderNotHolderFenceOrch(t, 100)
	replicaID := "replica-holder"
	now := time.Unix(0, 42).UTC()
	ingestor := newFenceHintIngestor(leaderOrch, "vault-ctl-leader", nil)
	ingestor.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId:  []byte(replicaID),
		Timestamp: timestamppb.New(now),
		Payload: &gastrologv1.BroadcastMessage_NodeStats{
			NodeStats: &gastrologv1.NodeStats{
				Vaults: []*gastrologv1.VaultStats{{
					Id:                  vaultID.ToProto(),
					IngestHighWatermark: 100,
				}},
			},
		},
	})

	st := leaderOrch.FenceState(vaultID)
	if len(st.Records) != 1 || st.Records[0].UpperBoundSeq != 100 {
		t.Fatalf("fence state = %+v, want upper bound 100 from remote NodeStats hint", st)
	}
}

func TestFenceHintIngestorIgnoresSelfAndStaleRemoteHints(t *testing.T) {
	t.Parallel()

	leaderOrch, vaultID := newLeaderNotHolderFenceOrch(t, 200)
	localID := "vault-ctl-leader"
	ingestor := newFenceHintIngestor(leaderOrch, localID, nil)
	fast := time.Unix(0, 8).UTC()

	ingestor.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId:  []byte(localID),
		Timestamp: timestamppb.New(fast),
		Payload: &gastrologv1.BroadcastMessage_NodeStats{
			NodeStats: &gastrologv1.NodeStats{
				Vaults: []*gastrologv1.VaultStats{{
					Id:                  vaultID.ToProto(),
					IngestHighWatermark: 50,
				}},
			},
		},
	})
	if st := leaderOrch.FenceState(vaultID); len(st.Records) != 0 {
		t.Fatalf("self broadcast must not publish fence; state=%+v", st)
	}

	ingestor.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId:  []byte("fast-replica"),
		Timestamp: timestamppb.New(fast),
		Payload: &gastrologv1.BroadcastMessage_NodeStats{
			NodeStats: &gastrologv1.NodeStats{
				Vaults: []*gastrologv1.VaultStats{{
					Id:                  vaultID.ToProto(),
					IngestHighWatermark: 200,
				}},
			},
		},
	})
	ingestor.HandleBroadcast(&gastrologv1.BroadcastMessage{
		SenderId:  []byte("slow-replica"),
		Timestamp: timestamppb.New(fast.Add(time.Second)),
		Payload: &gastrologv1.BroadcastMessage_NodeStats{
			NodeStats: &gastrologv1.NodeStats{
				Vaults: []*gastrologv1.VaultStats{{
					Id:                  vaultID.ToProto(),
					IngestHighWatermark: 150,
				}},
			},
		},
	})
	st := leaderOrch.FenceState(vaultID)
	if len(st.Records) != 1 || st.Records[0].UpperBoundSeq != 200 {
		t.Fatalf("fence state = %+v, want upper bound 200; stale hint must not regress publish", st)
	}
}

type fenceHintTestLoader struct {
	cfg *system.Config
}

func (l *fenceHintTestLoader) Load(_ context.Context) (*system.System, error) {
	return &system.System{Config: *l.cfg}, nil
}

func newLeaderNotHolderFenceOrch(t *testing.T, maxRecords int64) (*orchestrator.Orchestrator, glid.GLID) {
	t.Helper()
	vaultID := glid.New()
	policyID := glid.New()
	policy := system.RotationPolicyConfig{ID: policyID, MaxRecords: &maxRecords}
	loader := &fenceHintTestLoader{cfg: &system.Config{
		RotationPolicies: []system.RotationPolicyConfig{policy},
		Vaults: []system.VaultConfig{{
			ID:               vaultID,
			Name:             "seq",
			WriteModel:       string(system.VaultWriteModelSequenced),
			RotationPolicyID: &policyID,
		}},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(orch.Close)
	registerSequencedFenceTestVault(t, orch, vaultID)
	orch.WireTestSeqAllocator(vaultID)
	return orch, vaultID
}

func registerSequencedFenceTestVault(t *testing.T, orch *orchestrator.Orchestrator, vaultID glid.GLID) {
	t.Helper()
	cm, _ := chunkmem.NewManager(chunkmem.Config{})
	im := indexmem.NewManager(nil, nil, nil, nil, nil)
	qe := query.New(cm, im, nil)
	v := orchestrator.NewVault(vaultID, &orchestrator.VaultInstance{
		VaultID: vaultID,
		Type:    "memory",
		Chunks:  cm,
		Indexes: im,
		Query:   qe,
	})
	v.WriteModel = system.VaultWriteModelSequenced
	v.ReplicationFactor = 1
	inst := v.Instance
	inst.ListManifest = func() []chunk.ChunkID {
		metas, err := cm.List()
		if err != nil {
			return nil
		}
		ids := make([]chunk.ChunkID, len(metas))
		for i, meta := range metas {
			ids[i] = meta.ID
		}
		return ids
	}
	orch.RegisterVault(v)
}
