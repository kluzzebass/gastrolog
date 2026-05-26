package server

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// GetSequencedVaultDiagnostics returns local sequenced write-path diagnostics.
func (s *VaultServer) GetSequencedVaultDiagnostics(
	ctx context.Context,
	req *connect.Request[apiv1.GetSequencedVaultDiagnosticsRequest],
) (*connect.Response[apiv1.GetSequencedVaultDiagnosticsResponse], error) {
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	writeModel := system.VaultWriteModelChunkAppend
	if cfg, err := s.getFullVaultConfig(ctx, vaultID); err == nil {
		writeModel = cfg.ResolveWriteModel()
	}

	if writeModel != system.VaultWriteModelSequenced {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("vault write model is not sequenced"))
	}

	diag, err := s.orch.SequencedVaultDiagnostics(vaultID)
	if err != nil {
		if errors.Is(err, orchestrator.ErrVaultNotSequenced) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	nodeID := diag.NodeID
	if nodeID == "" {
		nodeID = s.localNodeID
	}

	resp := &apiv1.GetSequencedVaultDiagnosticsResponse{
		WriteModel:               string(writeModel),
		NodeId:                   nodeID,
		SpoolWatermark:           diag.SpoolWatermark,
		IngestHighWatermark:      diag.IngestHighWatermark,
		FenceHighWatermark:       diag.FenceHighWatermark,
		MaterializationWatermark: diag.MaterializationWatermark,
		ConvergenceWatermark:     diag.ConvergenceWatermark,
		Allocator:                seqAllocatorToProto(diag.Allocator),
		Fences:                   fencesToProto(diag.Fences),
	}
	return connect.NewResponse(resp), nil
}

func seqAllocatorToProto(alloc vaultctlfsm.SeqAllocatorSnapshot) *apiv1.SeqAllocatorDiagnostics {
	out := &apiv1.SeqAllocatorDiagnostics{
		NextSeq: alloc.NextSeq,
		Epoch:   alloc.Epoch,
	}
	for _, sw := range alloc.ActiveSwaths {
		out.ActiveSwaths = append(out.ActiveSwaths, &apiv1.SeqActiveLeaseDiagnostics{
			HolderId:   sw.HolderID,
			Epoch:      sw.Epoch,
			RangeStart: sw.RangeStart,
			RangeEnd:   sw.RangeEnd,
		})
	}
	for _, tail := range alloc.BurnedTails {
		out.BurnedTails = append(out.BurnedTails, &apiv1.SeqBurnedTailDiagnostics{
			Start: tail.Start,
			End:   tail.End,
			Epoch: tail.Epoch,
		})
	}
	return out
}

func fencesToProto(snap vaultctlfsm.FenceSnapshot) []*apiv1.FenceRecordDiagnostics {
	out := make([]*apiv1.FenceRecordDiagnostics, 0, len(snap.Records))
	for _, rec := range snap.Records {
		var created *timestamppb.Timestamp
		if rec.CreatedAtNanos > 0 {
			created = timestamppb.New(time.Unix(0, rec.CreatedAtNanos).UTC())
		}
		out = append(out, &apiv1.FenceRecordDiagnostics{
			Id:            rec.ID,
			UpperBoundSeq: rec.UpperBoundSeq,
			PrevBoundSeq:  rec.PrevBoundSeq,
			CreatedAt:     created,
		})
	}
	return out
}
