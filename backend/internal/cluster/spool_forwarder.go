package cluster

import (
	"context"
	"errors"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"
)

// SpoolForwarder reads spool slots from remote cluster peers.
type SpoolForwarder struct {
	conns *PeerConns
}

// NewSpoolForwarder returns a fetcher backed by the shared peer connection pool.
func NewSpoolForwarder(conns *PeerConns) *SpoolForwarder {
	return &SpoolForwarder{conns: conns}
}

// ReadSpoolSeq implements orchestrator.SpoolSlotFetcher.
func (f *SpoolForwarder) ReadSpoolSeq(ctx context.Context, nodeID string, vaultID glid.GLID, seq uint64) (chunk.Record, bool, error) {
	if f == nil || f.conns == nil {
		return chunk.Record{}, false, errors.New("spool forwarder not configured")
	}
	conn, err := f.conns.Conn(nodeID)
	if err != nil {
		return chunk.Record{}, false, err
	}
	req := &gastrologv1.ForwardReadSpoolSeqRequest{
		VaultId:  vaultID.ToProto(),
		VaultSeq: seq,
	}
	resp := &gastrologv1.ForwardReadSpoolSeqResponse{}
	if err := conn.Invoke(ctx, "/gastrolog.v1.ClusterService/ForwardReadSpoolSeq", req, resp); err != nil {
		return chunk.Record{}, false, err
	}
	if !resp.GetFound() || resp.GetRecord() == nil {
		return chunk.Record{}, false, nil
	}
	rec := convert.ExportToRecord(resp.GetRecord())
	return rec, true, nil
}
