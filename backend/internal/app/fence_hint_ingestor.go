package app

import (
	"log/slog"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// fenceHintIngestor consumes NodeStats broadcasts and submits remote replica
// fence hints to the local vault-ctl leader's arbitrator. Hints remain
// ephemeral — publication stays on vault-ctl Raft.
type fenceHintIngestor struct {
	orch        *orchestrator.Orchestrator
	localNodeID string
	logger      *slog.Logger
}

func newFenceHintIngestor(orch *orchestrator.Orchestrator, localNodeID string, logger *slog.Logger) *fenceHintIngestor {
	return &fenceHintIngestor{
		orch:        orch,
		localNodeID: localNodeID,
		logger:      logger,
	}
}

// HandleBroadcast is a cluster broadcast subscriber callback.
func (f *fenceHintIngestor) HandleBroadcast(msg *gastrologv1.BroadcastMessage) {
	if msg == nil {
		return
	}
	ns := msg.GetNodeStats()
	if ns == nil {
		return
	}
	sender := string(msg.SenderId)
	if sender == "" || sender == f.localNodeID {
		return
	}
	observedAt := time.Now().UTC()
	if msg.Timestamp != nil {
		observedAt = msg.Timestamp.AsTime()
	}
	for _, vs := range ns.GetVaults() {
		if vs == nil || vs.IngestHighWatermark == 0 {
			continue
		}
		vaultID := glid.FromBytes(vs.Id)
		if vaultID.IsZero() {
			continue
		}
		hint := orchestrator.FenceHint{
			NodeID:     sender,
			H:          vs.IngestHighWatermark,
			ObservedAt: observedAt,
		}
		if !f.orch.SubmitFenceHint(vaultID, hint) {
			continue
		}
		if err := f.orch.EvaluateVaultFenceAfterHint(vaultID); err != nil {
			f.logger.Warn("fence hint ingest: evaluate failed",
				"vault", vaultID,
				"peer", sender,
				"h", vs.IngestHighWatermark,
				"error", err)
		}
	}
}
