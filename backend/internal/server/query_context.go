package server

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/query"
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

	ref := req.Msg.Ref
	if ref == nil || ref.VaultId == "" || ref.ChunkId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ref must include vault_id, chunk_id, and pos"))
	}

	vaultID, connErr := parseUUID(ref.VaultId)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := chunk.ParseChunkID(ref.ChunkId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid chunk_id: %w", err))
	}

	// Step 1: Read the anchor record from its owning vault.
	anchor, err := s.readAnchor(ctx, vaultID, chunkID, ref.Pos)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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
		return rec.Ref != nil &&
			rec.Ref.VaultId == ref.VaultId &&
			rec.Ref.ChunkId == ref.ChunkId &&
			rec.Ref.Pos == ref.Pos
	}

	anchorTS := anchor.GetWriteTs().AsTime()

	beforeRecs, err := s.searchContext(ctx, query.Query{
		End:       anchorTS,
		Limit:     before + 1,
		IsReverse: true,
	}, before, isAnchor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slices.Reverse(beforeRecs) // newest-first → oldest-first

	afterRecs, err := s.searchContext(ctx, query.Query{
		Start: anchorTS,
		Limit: after + 1,
	}, after, isAnchor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&apiv1.GetContextResponse{
		Anchor: anchor,
		Before: beforeRecs,
		After:  afterRecs,
	}), nil
}

// readAnchor reads a single record by its ref. If the vault is local, reads
// via cursor. If remote, forwards to the owning node.
func (s *QueryServer) readAnchor(ctx context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID, pos uint64) (*apiv1.Record, error) {
	if nodeID := s.remoteNodeForVault(ctx, vaultID); nodeID != "" {
		resp, err := s.remoteSearcher.GetContext(ctx, nodeID, &apiv1.ForwardGetContextRequest{
			VaultId: vaultID.String(),
			ChunkId: chunkID.String(),
			Pos:     pos,
		})
		if err != nil {
			return nil, fmt.Errorf("remote anchor read: %w", err)
		}
		if resp.Anchor == nil {
			return nil, errors.New("remote anchor not found")
		}
		return exportToRecord(resp.Anchor), nil
	}

	eng := s.orch.PrimaryTierQueryEngine()
	anchor, err := eng.ReadRecord(ctx, vaultID, chunkID, pos)
	if err != nil {
		// Chunk may have been deleted by retention between search and context read.
		// Return nil anchor instead of erroring — the caller handles missing anchors.
		if errors.Is(err, chunk.ErrVaultNotFound) || errors.Is(err, chunk.ErrChunkNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read anchor vault=%s chunk=%s pos=%d: %w", vaultID, chunkID, pos, err)
	}
	return recordToProto(anchor), nil
}

// searchContext runs a full cluster-wide search (local engine + remote vaults)
// and collects up to n records into a slice, skipping the anchor.
func (s *QueryServer) searchContext(
	ctx context.Context,
	q query.Query,
	n int,
	isAnchor func(*apiv1.Record) bool,
) ([]*apiv1.Record, error) {
	eng := s.orch.PrimaryTierQueryEngine()
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
// Uses tier-level NodeID (set by the placement manager) for node assignment.
func (s *QueryServer) remoteNodeForVault(ctx context.Context, vaultID uuid.UUID) string {
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

	tiers, err := s.cfgStore.ListTiers(ctx)
	if err != nil {
		return ""
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return ""
	}

	tierMap := make(map[uuid.UUID]*config.TierConfig, len(tiers))
	for i := range tiers {
		tierMap[tiers[i].ID] = &tiers[i]
	}

	// temporary: find the tier's leader node to determine the owning node (until tier election).
	for _, tierID := range config.VaultTierIDs(tiers, vaultCfg.ID) {
		tc := tierMap[tierID]
		if tc == nil {
			continue
		}
		leaderNodeID := tc.LeaderNodeID(nscs)
		if leaderNodeID != "" && leaderNodeID != s.localNodeID {
			return leaderNodeID
		}
	}
	return ""
}
