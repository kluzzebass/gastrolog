package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"iter"
	"slices"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// GetContext returns records surrounding a specific record, searching across
// all vaults in the cluster. The anchor record is read from its owning node
// (local cursor or remote forward), but the before/after context searches
// run the full cluster-wide search path so that records from any vault appear.
func (s *QueryServer) GetContext(
	ctx context.Context,
	req *connect.Request[apiv1.GetContextRequest],
) (*connect.Response[apiv1.GetContextResponse], error) {
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}

	ref, err := query.ContextRefFromProto(req.Msg.Ref)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Step 1: Read the anchor record from its owning vault.
	anchor, anchorRec, err := s.readAnchorRecord(ctx, ref)
	if err != nil {
		return nil, errInternal(err)
	}
	if anchor == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("anchor record not found"))
	}

	// Step 2: Collect context using the full cluster-wide search path.
	before := int(req.Msg.Before)
	after := int(req.Msg.After)
	if before == 0 {
		before = 5
	}
	if after == 0 {
		after = 5
	}

	isAnchor := func(rec *apiv1.Record) bool {
		if rec == nil {
			return false
		}
		return protoRecordMatchesAnchor(rec, anchorRec)
	}

	anchorTS := anchor.GetWriteTs().AsTime()

	beforeRecs, err := s.searchContext(ctx, query.Query{
		End:       anchorTS,
		Limit:     before + 1,
		IsReverse: true,
	}, before, isAnchor)
	if err != nil {
		return nil, errInternal(err)
	}
	slices.Reverse(beforeRecs) // newest-first → oldest-first

	afterRecs, err := s.searchContext(ctx, query.Query{
		Start: anchorTS,
		Limit: after + 1,
	}, after, isAnchor)
	if err != nil {
		return nil, errInternal(err)
	}

	return connect.NewResponse(&apiv1.GetContextResponse{
		Anchor: anchor,
		Before: beforeRecs,
		After:  afterRecs,
	}), nil
}

// readAnchorRecord reads the anchor for GetContext (materialized or vault_seq).
func (s *QueryServer) readAnchorRecord(ctx context.Context, ref query.ContextRef) (*apiv1.Record, chunk.Record, error) {
	if nodeID := s.remoteNodeForVault(ctx, ref.VaultID); nodeID != "" {
		fwd := &apiv1.ForwardGetContextRequest{
			VaultId: ref.VaultID.ToProto(),
			Pos:     ref.Pos,
			VaultSeq: ref.VaultSeq,
		}
		if ref.IsMaterialized() {
			fwd.ChunkId = glid.GLID(ref.ChunkID).ToProto()
		}
		resp, err := s.remoteSearcher.GetContext(ctx, nodeID, fwd)
		if err != nil {
			return nil, chunk.Record{}, fmt.Errorf("remote anchor read: %w", err)
		}
		if resp.Anchor == nil {
			return nil, chunk.Record{}, nil
		}
		proto := exportToRecord(resp.Anchor)
		return proto, protoToChunkRecord(proto), nil
	}

	eng := s.orch.LeaderVaultQueryEngine()
	anchorRec, err := eng.ReadAnchor(ctx, ref)
	if err != nil {
		if errors.Is(err, chunk.ErrVaultNotFound) || errors.Is(err, chunk.ErrChunkNotFound) {
			return nil, chunk.Record{}, nil
		}
		return nil, chunk.Record{}, fmt.Errorf("read anchor vault=%s: %w", ref.VaultID, err)
	}
	return recordToProto(anchorRec), anchorRec, nil
}

// readAnchor reads a single materialized record by chunk ref. Prefer readAnchorRecord.
func (s *QueryServer) readAnchor(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, pos uint64) (*apiv1.Record, error) {
	proto, _, err := s.readAnchorRecord(ctx, query.ContextRef{
		VaultID: vaultID,
		ChunkID: chunkID,
		Pos:     pos,
	})
	return proto, err
}

// searchContext runs a full cluster-wide search (local engine + remote vaults)
// and collects up to n records into a slice, skipping the anchor.
func (s *QueryServer) searchContext(
	ctx context.Context,
	q query.Query,
	n int,
	isAnchor func(*apiv1.Record) bool,
) ([]*apiv1.Record, error) {
	eng := s.orch.LeaderVaultQueryEngine()
	localIter, _ := eng.Search(ctx, q, nil)
	remoteIter, _, _ := s.collectRemote(ctx, q, nil)

	reverse := q.Reverse()
	isBefore := func(a, b time.Time) bool {
		if reverse {
			return a.After(b)
		}
		return a.Before(b)
	}

	remote := drainIterToProto(remoteIter)

	ri := 0
	var result []*apiv1.Record

	for rec, err := range localIter {
		if err != nil {
			return result, err
		}
		// Drain remote records that sort before this local record.
		for ri < len(remote) && isBefore(remote[ri].GetWriteTs().AsTime(), rec.WriteTS) {
			if !isAnchor(remote[ri]) {
				result = append(result, remote[ri])
				if len(result) >= n {
					return result, nil
				}
			}
			ri++
		}
		proto := recordToProto(rec)
		if isAnchor(proto) {
			continue
		}
		result = append(result, proto)
		if len(result) >= n {
			return result, nil
		}
	}

	// Drain remaining remote records.
	for ri < len(remote) {
		if !isAnchor(remote[ri]) {
			result = append(result, remote[ri])
			if len(result) >= n {
				return result, nil
			}
		}
		ri++
	}

	return result, nil
}

// drainIterToProto collects all records from an iterator into a slice of
// proto records. Returns nil if the iterator is nil.
func drainIterToProto(it iter.Seq2[chunk.Record, error]) []*apiv1.Record {
	if it == nil {
		return nil
	}
	var out []*apiv1.Record
	for rec, err := range it {
		if err != nil {
			break
		}
		out = append(out, recordToProto(rec))
	}
	return out
}

// remoteNodeForVault returns the owning node ID if the vault is remote,
// or "" if the vault is local or lookup fails.
//
// Reads VaultConfig.Placements directly (mirrored from vault placements via
// the FSM bridge — gastrolog-257l7).
func (s *QueryServer) remoteNodeForVault(ctx context.Context, vaultID glid.GLID) string {
	// If the vault is registered locally, it's not remote.
	if slices.Contains(s.orch.ListVaults(), vaultID) {
		return ""
	}

	if s.cfgStore == nil {
		return ""
	}

	vaultCfg, err := s.cfgStore.GetVault(ctx, vaultID)
	if err != nil || vaultCfg == nil {
		return ""
	}
	if len(vaultCfg.Placements) == 0 {
		return ""
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return ""
	}
	leaderNodeID := system.LeaderNodeID(vaultCfg.Placements, nscs)
	if leaderNodeID == "" || leaderNodeID == s.localNodeID {
		return ""
	}
	return leaderNodeID
}
