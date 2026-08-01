package server

import (
	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// CloudIndexAuditToProto renders one node's cloud-index audit for the wire.
// Exported so the VaultService handler and the cluster forward executor emit
// the same shape — a second conversion would be a second place for the
// categories to drift.
func CloudIndexAuditToProto(a orchestrator.CloudIndexAudit) *apiv1.CloudIndexAudit {
	pb := &apiv1.CloudIndexAudit{
		NodeId:            a.NodeID,
		ExpectedChunks:    int64(a.ExpectedChunks),
		StoreObjects:      int64(a.StoreObjects),
		IndexEntries:      int64(a.IndexEntries),
		ArchivedObjects:   int64(a.ArchivedObjects),
		MissingBlobs:      chunkIDsToProto(a.MissingBlobs),
		UntrackedBlobs:    chunkIDsToProto(a.UntrackedBlobs),
		TombstonedBlobs:   chunkIDsToProto(a.TombstonedBlobs),
		StaleIndexEntries: chunkIDsToProto(a.StaleIndexEntries),
		UnindexedBlobs:    chunkIDsToProto(a.UnindexedBlobs),
	}
	for _, m := range a.SizeMismatches {
		pb.SizeMismatches = append(pb.SizeMismatches, &apiv1.CloudIndexSizeMismatch{
			ChunkId:       glid.GLID(m.ID).ToProto(),
			ExpectedBytes: m.ExpectedBytes,
			StoreBytes:    m.StoreBytes,
		})
	}
	return pb
}

func chunkIDsToProto(ids []chunk.ChunkID) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		out = append(out, glid.GLID(id).ToProto())
	}
	return out
}
