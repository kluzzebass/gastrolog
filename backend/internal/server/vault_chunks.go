package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
)

// ListChunks returns all chunks in a vault across all nodes that host it.
// Routing: RouteFanOut — collects local chunks + remote chunks from all nodes.
func (s *VaultServer) ListChunks(
	ctx context.Context,
	req *connect.Request[apiv1.ListChunksRequest],
) (*connect.Response[apiv1.ListChunksResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	// Collect local chunks, marking any with retention-pending in vault-ctl Raft.
	pending := s.orch.RetentionPendingChunks(vaultID)
	pendingAcks := s.orch.PendingDeleteAcks(vaultID)
	var reports []chunkReport
	metas, err := s.orch.ListAllChunkMetas(vaultID)
	if err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return nil, mapVaultError(err)
	}
	for _, meta := range metas {
		if req.Msg.ActiveOnly && meta.Sealed {
			continue // lightweight poll: skip sealed chunks
		}
		pb := VaultChunkMetaToProto(meta)
		if pending[meta.ID] {
			pb.RetentionPending = true
		}
		if owed := pendingAcks[meta.ID]; len(owed) > 0 {
			sortedOwed := append([]string(nil), owed...)
			sort.Strings(sortedOwed)
			pb.PendingAckNodeIds = sortedOwed
		}
		reports = append(reports, chunkReport{reportingNode: s.localNodeID, chunk: pb})
	}

	// Full mode: collect remote chunks from all nodes hosting the vault.
	// Skipped in active_only mode — the caller only needs the connected
	// node's active chunk stats for the 5-second refresh; the full
	// cluster-wide picture comes through the stream-driven refetch.
	//
	// Parallel fan-out with per-peer timeout (gastrolog-csspr): a paused
	// or partitioned peer used to block this loop for minutes (gRPC
	// keepalive being the only natural bound), freezing the entire
	// inspector UI on every node that hits this handler. Now each peer
	// gets its own bounded context and they all run concurrently, so
	// total latency is max(peer RTTs) bounded by peerInspectorTimeout.
	// A peer that misses the deadline is silently dropped from the merged
	// view; the UI gets the partial result instead of hanging.
	if !req.Msg.ActiveOnly && s.remoteChunkLister != nil {
		remoteNodes := s.remoteVaultNodes(ctx, vaultID)
		results, ok := peerFanOut(ctx, s.logger, "ListChunks", remoteNodes,
			func(peerCtx context.Context, nodeID string) ([]*apiv1.ChunkMeta, error) {
				remote, err := s.remoteChunkLister.ListChunks(peerCtx, nodeID, &apiv1.ForwardListChunksRequest{
					VaultId: vaultID.ToProto(),
				})
				if err != nil {
					return nil, err
				}
				return remote.Chunks, nil
			})
		for i, chunks := range results {
			if !ok[i] {
				continue
			}
			for _, c := range chunks {
				reports = append(reports, chunkReport{reportingNode: remoteNodes[i], chunk: c})
			}
		}
	}

	// Deduplicate by chunk ID. When a chunk is replicated to multiple nodes,
	// the same chunk ID appears in the raw merge multiple times.
	// Keep the most authoritative version (sealed + compressed > not) and
	// set replica_count to how many distinct nodes reported the chunk (not
	// raw row count — a single node can list the same ID twice when it
	// hosts multiple local instances).
	chunks := dedupChunkReports(reports)
	s.overlayChunkResidency(vaultID, chunks)
	return connect.NewResponse(&apiv1.ListChunksResponse{Chunks: chunks}), nil
}

// overlayChunkResidency replaces the fan-out-derived replica sets with
// authoritative holder receipts from the vault-ctl FSM, so ListChunks
// and WatchChunks stamp the SAME residency semantics. The fan-out set
// (which nodes answered this round) is reachability evidence, not bytes
// truth — a slow peer vanishing for a round used to ping-pong against
// the FSM-stamped watch residency and regress sealed pips to amber
// (gastrolog-68wsli). The reachability set survives only as a fallback
// where no FSM exists for the vault (memory mode / single-node), where
// receipts never happen.
//
// An empty-but-known residency (chunk in the FSM, zero receipts yet)
// overwrites too: for a just-sealed chunk it is the truth — the homes'
// copy-seal receipts haven't landed — and the inspector renders that
// window honestly as per-node catch-up instead of a false full row.
func (s *VaultServer) overlayChunkResidency(vaultID glid.GLID, chunks []*apiv1.ChunkMeta) {
	overlayResidencyWith(chunks, func(cid chunk.ChunkID) []string {
		return s.orch.ChunkResidency(vaultID, cid)
	})
}

// overlayResidencyWith is the pure core of overlayChunkResidency: nil
// residency (no FSM / chunk unknown) keeps the fan-out-derived set;
// anything else — including empty — replaces it. Split out so the
// keep-vs-replace semantics are unit-testable without an orchestrator.
func overlayResidencyWith(chunks []*apiv1.ChunkMeta, residencyOf func(chunk.ChunkID) []string) {
	for _, c := range chunks {
		if len(c.Id) != len(chunk.ChunkID{}) {
			continue
		}
		var cid chunk.ChunkID
		copy(cid[:], c.Id)
		residency := residencyOf(cid)
		if residency == nil {
			continue
		}
		sort.Strings(residency)
		c.ReplicaNodeIds = residency
		if len(residency) > math.MaxInt32 {
			c.ReplicaCount = math.MaxInt32
		} else {
			c.ReplicaCount = int32(len(residency)) //nolint:gosec // G115: bounded by branch above
		}
	}
}

// chunkReport pairs a chunk metadata message with the cluster node that
// produced it. Used so replica_count can mean "distinct nodes" rather than
// "distinct list rows".
type chunkReport struct {
	reportingNode string
	chunk         *apiv1.ChunkMeta
}

// dedupChunkReports collapses multiple entries for the same chunk ID into a
// single authoritative entry. The most advanced version (sealed+compressed)
// wins, and replica_count is the number of distinct reportingNode values that
// listed the chunk.
func dedupChunkReports(reports []chunkReport) []*apiv1.ChunkMeta {
	if len(reports) == 0 {
		return nil
	}
	type agg struct {
		nodes map[string]struct{}
		best  *apiv1.ChunkMeta
	}
	byID := make(map[string]*agg, len(reports))
	anonSeq := 0
	for _, r := range reports {
		c := r.chunk
		if c == nil {
			continue
		}
		key := string(c.Id)
		a := byID[key]
		if a == nil {
			a = &agg{nodes: make(map[string]struct{})}
			byID[key] = a
		}
		nodeKey := r.reportingNode
		if nodeKey == "" {
			// Unit tests (or misconfig): preserve one-replica-per-row semantics.
			nodeKey = fmt.Sprintf("__anon_%d", anonSeq)
			anonSeq++
		}
		a.nodes[nodeKey] = struct{}{}
		if a.best == nil {
			a.best = c
			continue
		}
		anyPending := a.best.RetentionPending || c.RetentionPending
		if moreAuthoritative(c, a.best) {
			a.best = c
		}
		a.best.RetentionPending = anyPending
	}
	out := make([]*apiv1.ChunkMeta, 0, len(byID))
	for _, a := range byID {
		c := a.best
		replicas := len(a.nodes)
		if replicas > math.MaxInt32 {
			c.ReplicaCount = math.MaxInt32
		} else {
			// replicas is capped; cluster node counts do not approach MaxInt32.
			c.ReplicaCount = int32(replicas) //nolint:gosec // G115: bounded by branch above
		}
		// Populate the cluster-wide replica residency set so the
		// inspector can show which nodes physically hold this chunk.
		// Skip the synthetic "__anon_*" keys used by unit tests where
		// no reportingNode was set; those carry no operator value.
		// Sort for deterministic display. See gastrolog-51gme.
		nodeIDs := make([]string, 0, len(a.nodes))
		for nid := range a.nodes {
			if !strings.HasPrefix(nid, "__anon_") {
				nodeIDs = append(nodeIDs, nid)
			}
		}
		sort.Strings(nodeIDs)
		c.ReplicaNodeIds = nodeIDs
		out = append(out, c)
	}
	return out
}

// moreAuthoritative reports whether a is a more-advanced view of the same
// chunk than b. Higher authority = later in the chunk lifecycle. With the
// Compressed flag merged into Sealed (gastrolog-24m1t step 7f), the only
// lifecycle transition is unsealed → sealed.
//
// For two unsealed views of the same chunk — which happens when the leader
// and any follower both report the active chunk in the fan-out — pick the
// one with the higher RecordCount. RecordCount grows monotonically during
// an active chunk's lifetime, so 'higher' means 'fresher'. Without this
// tiebreaker, a leader-peer timeout in the parallel fan-out demotes the
// leader's authoritative view and lets a follower's stale RecordCount
// (followers only replicate sealed chunks, so their active-chunk count
// lags or is zero) win the round, producing visible oscillation in the
// inspector UI as successive ticks flip between leader-wins and
// follower-wins rounds. See gastrolog-1bgvm.
func moreAuthoritative(a, b *apiv1.ChunkMeta) bool {
	ra, rb := chunkLifecycleRank(a), chunkLifecycleRank(b)
	if ra != rb {
		return ra > rb
	}
	if a.RecordCount != b.RecordCount {
		return a.RecordCount > b.RecordCount
	}
	return hasBetterTimeRange(a, b)
}

func chunkLifecycleRank(c *apiv1.ChunkMeta) int {
	if c == nil {
		return 0
	}
	if c.Sealed || c.State == apiv1.ChunkState_CHUNK_STATE_SEALED {
		return 3
	}
	switch c.State {
	case apiv1.ChunkState_CHUNK_STATE_SEALING:
		return 2
	case apiv1.ChunkState_CHUNK_STATE_ACTIVE:
		return 1
	case apiv1.ChunkState_CHUNK_STATE_UNSPECIFIED:
		return 1
	case apiv1.ChunkState_CHUNK_STATE_SEALED:
		return 3
	}
	return 1
}

func hasBetterTimeRange(a, b *apiv1.ChunkMeta) bool {
	aEnd := chunkMetaEndNanos(a)
	bEnd := chunkMetaEndNanos(b)
	if aEnd != bEnd {
		return aEnd > bEnd
	}
	aStart := chunkMetaStartNanos(a)
	bStart := chunkMetaStartNanos(b)
	return aStart > bStart
}

func chunkMetaEndNanos(c *apiv1.ChunkMeta) int64 {
	if c == nil {
		return 0
	}
	if ts := c.IngestEnd; ts != nil && ts.IsValid() && ts.AsTime().Year() >= 2000 {
		return ts.AsTime().UnixNano()
	}
	if ts := c.WriteEnd; ts != nil && ts.IsValid() && ts.AsTime().Year() >= 2000 {
		return ts.AsTime().UnixNano()
	}
	return 0
}

func chunkMetaStartNanos(c *apiv1.ChunkMeta) int64 {
	if c == nil {
		return 0
	}
	if ts := c.IngestStart; ts != nil && ts.IsValid() && ts.AsTime().Year() >= 2000 {
		return ts.AsTime().UnixNano()
	}
	if ts := c.WriteStart; ts != nil && ts.IsValid() && ts.AsTime().Year() >= 2000 {
		return ts.AsTime().UnixNano()
	}
	return 0
}

// remoteVaultNodes returns node IDs of ALL remote nodes that host this
// vault — both leader and followers. Leader provides authoritative chunk
// metadata; followers are queried to verify replica presence for the UI.
//
// Reads VaultConfig.Placements directly.
func (s *VaultServer) remoteVaultNodes(ctx context.Context, vaultID glid.GLID) []string {
	vaultCfg, err := s.cfgStore.GetVault(ctx, vaultID)
	if err != nil || vaultCfg == nil {
		return nil
	}
	if len(vaultCfg.Placements) == 0 {
		return nil
	}
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var nodes []string
	leaderNodeID := system.LeaderNodeID(vaultCfg.Placements, nscs)
	if leaderNodeID != "" && leaderNodeID != s.localNodeID {
		seen[leaderNodeID] = true
		nodes = append(nodes, leaderNodeID)
	}
	for _, sid := range system.FollowerNodeIDs(vaultCfg.Placements, nscs) {
		if sid != s.localNodeID && !seen[sid] {
			seen[sid] = true
			nodes = append(nodes, sid)
		}
	}
	return nodes
}

// GetChunk returns details for a specific chunk.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) GetChunk(
	ctx context.Context,
	req *connect.Request[apiv1.GetChunkRequest],
) (*connect.Response[apiv1.GetChunkResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := parseProtoChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, errInvalidArg(err)
	}

	meta, err := s.orch.GetVaultChunkMeta(vaultID, chunkID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	// Same overlays ListChunks applies (UI/CLI parity, gastrolog-45ywhx):
	// retention-pending and pending-ack state from the receipt protocol,
	// and holder-receipt residency from the vault-ctl FSM. Without these
	// the single-chunk view showed none of the lifecycle state the vault
	// listing (and the UI inspector) shows.
	pb := VaultChunkMetaToProto(meta)
	if pending := s.orch.RetentionPendingChunks(vaultID); pending != nil {
		pb.RetentionPending = pending[chunkID]
	}
	if acks := s.orch.PendingDeleteAcks(vaultID); acks != nil {
		if owed := acks[chunkID]; len(owed) > 0 {
			sortedOwed := append([]string(nil), owed...)
			sort.Strings(sortedOwed)
			pb.PendingAckNodeIds = sortedOwed
		}
	}
	s.overlayChunkResidency(vaultID, []*apiv1.ChunkMeta{pb})

	return connect.NewResponse(&apiv1.GetChunkResponse{
		Chunk: pb,
	}), nil
}

// GetIndexes returns index status for a chunk.
//
// Routing: RouteLocal — the handler tries the local node first, then
// fans out to remote vault-hosting nodes if the chunk has migrated to a
// vault this node doesn't host. Cross-vault migration (warm → cloud,
// etc.) shifts a chunk's owning node, so a single RouteTargeted hop
// would frequently miss and produce ErrChunkNotFound log spam. See
// gastrolog-3570f.
func (s *VaultServer) GetIndexes(
	ctx context.Context,
	req *connect.Request[apiv1.GetIndexesRequest],
) (*connect.Response[apiv1.GetIndexesResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	chunkID, err := parseProtoChunkID(req.Msg.ChunkId)
	if err != nil {
		return nil, errInvalidArg(err)
	}

	// Local first: cheap path when this node hosts the vault.
	if report, err := s.orch.ChunkIndexInfos(vaultID, chunkID); err == nil {
		return connect.NewResponse(reportToProto(report)), nil
	} else if !errors.Is(err, chunk.ErrChunkNotFound) && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return nil, mapVaultError(err)
	}

	// Local doesn't have it — chunk has migrated to another vault on a
	// different node. Fan out to all peers hosting this vault; the one
	// that has the chunk responds with index info, others say
	// ErrChunkNotFound and are silently elided.
	if s.remoteIndexer == nil {
		return nil, mapVaultError(chunk.ErrChunkNotFound)
	}
	remoteNodes := s.remoteVaultNodes(ctx, vaultID)
	if len(remoteNodes) == 0 {
		return nil, mapVaultError(chunk.ErrChunkNotFound)
	}
	results, ok := peerFanOut(ctx, s.logger, "GetIndexes", remoteNodes,
		func(peerCtx context.Context, nodeID string) (*apiv1.GetIndexesResponse, error) {
			remote, err := s.remoteIndexer.GetIndexes(peerCtx, nodeID, &apiv1.ForwardGetIndexesRequest{
				VaultId: vaultID.ToProto(),
				ChunkId: chunkID[:],
			})
			if err != nil {
				return nil, err
			}
			return &apiv1.GetIndexesResponse{
				Sealed:  remote.Sealed,
				Indexes: remote.Indexes,
			}, nil
		})
	for i, resp := range results {
		if !ok[i] || resp == nil || len(resp.Indexes) == 0 {
			continue
		}
		return connect.NewResponse(resp), nil
	}
	return nil, mapVaultError(chunk.ErrChunkNotFound)
}

// reportToProto converts an orchestrator chunk-index report into the
// proto response shape. Extracted from the original GetIndexes body so
// the local-first path and the (future) all-results merge can share it.
func reportToProto(report *orchestrator.ChunkIndexReport) *apiv1.GetIndexesResponse {
	resp := &apiv1.GetIndexesResponse{
		Sealed:  report.Sealed,
		Indexes: make([]*apiv1.IndexInfo, 0, len(report.Indexes)),
	}
	for _, idx := range report.Indexes {
		resp.Indexes = append(resp.Indexes, &apiv1.IndexInfo{
			Name:       idx.Name,
			Exists:     idx.Exists,
			EntryCount: idx.EntryCount,
			SizeBytes:  idx.SizeBytes,
		})
	}
	return resp
}

// AnalyzeChunk returns detailed index analysis for a chunk.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) AnalyzeChunk(
	ctx context.Context,
	req *connect.Request[apiv1.AnalyzeChunkRequest],
) (*connect.Response[apiv1.AnalyzeChunkResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	analyses, err := s.analyzeChunkPayload(vaultID, req.Msg)
	if err != nil {
		return nil, err
	}

	resp := &apiv1.AnalyzeChunkResponse{
		Analyses: make([]*apiv1.ChunkAnalysis, 0, len(analyses)),
	}

	for _, ca := range analyses {
		resp.Analyses = append(resp.Analyses, ChunkAnalysisToProto(ca))
	}

	return connect.NewResponse(resp), nil
}

// analyzeChunkPayload runs index analysis for the whole vault or one chunk.
func (s *VaultServer) analyzeChunkPayload(vaultID glid.GLID, msg *apiv1.AnalyzeChunkRequest) ([]analyzer.ChunkAnalysis, error) {
	if len(msg.ChunkId) == 0 {
		return s.analyzeChunkAll(vaultID)
	}
	return s.analyzeChunkSingle(vaultID, msg.ChunkId)
}

func (s *VaultServer) analyzeChunkAll(vaultID glid.GLID) ([]analyzer.ChunkAnalysis, error) {
	a, err := s.orch.NewAnalyzer(vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}
	agg, err := a.AnalyzeAll()
	if err != nil {
		return nil, errInternal(err)
	}
	return agg.Chunks, nil
}

func (s *VaultServer) analyzeChunkSingle(vaultID glid.GLID, chunkProto []byte) ([]analyzer.ChunkAnalysis, error) {
	chunkID, parseErr := parseProtoChunkID(chunkProto)
	if parseErr != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
	}
	a, err := s.orch.NewAnalyzerForChunk(vaultID, chunkID)
	if err != nil {
		return nil, mapVaultError(err)
	}
	analysis, err := a.AnalyzeChunk(chunkID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return []analyzer.ChunkAnalysis{*analysis}, nil
}

// ValidateVault checks chunk and index integrity for a vault.
// Routing: RouteTargeted — the interceptor forwards to the vault-owning node.
func (s *VaultServer) ValidateVault(
	ctx context.Context,
	req *connect.Request[apiv1.ValidateVaultRequest],
) (*connect.Response[apiv1.ValidateVaultResponse], error) {
	if req.Msg.Vault == "" {
		return nil, errRequired("vault")
	}
	vaultID, connErr := parseUUID(req.Msg.Vault)
	if connErr != nil {
		return nil, connErr
	}

	metas, err := s.orch.ListLocalChunkMetas(vaultID)
	if err != nil {
		return nil, mapVaultError(err)
	}

	resp := ValidateVaultLocal(s.orch, vaultID, metas)
	return connect.NewResponse(resp), nil
}

// ValidateVaultLocal runs chunk and index integrity checks on a local vault.
// Exported so both the VaultServer RPC handler and the cluster executor can
// share the same validation logic.
func ValidateVaultLocal(orch *orchestrator.Orchestrator, vaultID glid.GLID, metas []chunk.ChunkMeta) *apiv1.ValidateVaultResponse {
	resp := &apiv1.ValidateVaultResponse{Valid: true}
	for _, meta := range metas {
		cv := validateChunk(orch, vaultID, meta)
		if !cv.Valid {
			resp.Valid = false
		}
		resp.Chunks = append(resp.Chunks, cv)
	}
	return resp
}

// validateChunk checks a single chunk's cursor readability and index completeness.
func validateChunk(orch *orchestrator.Orchestrator, vaultID glid.GLID, meta chunk.ChunkMeta) *apiv1.ChunkValidation {
	cv := &apiv1.ChunkValidation{
		ChunkId: glid.GLID(meta.ID).ToProto(),
		Valid:   true,
	}

	cursor, err := orch.OpenCursor(vaultID, meta.ID)
	if err != nil {
		cv.Valid = false
		cv.Issues = append(cv.Issues, fmt.Sprintf("cannot open cursor: %v", err))
		return cv
	}

	var recordCount int64
	for {
		_, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			cv.Valid = false
			cv.Issues = append(cv.Issues, fmt.Sprintf("read error at record %d: %v", recordCount, err))
			break
		}
		recordCount++
	}
	_ = cursor.Close()

	if meta.RecordCount > 0 && recordCount != meta.RecordCount {
		cv.Valid = false
		cv.Issues = append(cv.Issues,
			fmt.Sprintf("record count mismatch: metadata says %d, cursor read %d", meta.RecordCount, recordCount))
	}

	if meta.Sealed {
		complete, err := orch.IndexesComplete(vaultID, meta.ID)
		if err != nil {
			cv.Valid = false
			cv.Issues = append(cv.Issues, fmt.Sprintf("index check error: %v", err))
		} else if !complete {
			cv.Valid = false
			cv.Issues = append(cv.Issues, "indexes incomplete for sealed chunk")
		}
	}

	return cv
}

// WatchChunks opens a server-streaming subscription that pushes a typed
// chunk-state event every time a chunk on this node changes. Each event
// carries the vault ID, chunk ID, op, and either a full ChunkMeta (for
// CREATED / SEALED / UPLOADED) or a record count (for PROGRESS). Clients
// patch their local cache directly from the event payload instead of
// refetching ListChunks on every notification — see gastrolog-3pf9w for
// the pre-3pf9w shape and why it was replaced.
//
// Routing: RouteLocal — each node emits events for the vaults it hosts;
// the client maintains one WatchChunks stream per cluster node and merges
// events into per-vault caches. Reconnects detect dropped events by
// comparing the first post-reconnect version against last-seen + 1.
func (s *VaultServer) WatchChunks(
	ctx context.Context,
	_ *connect.Request[apiv1.WatchChunksRequest],
	stream *connect.ServerStream[apiv1.WatchChunksResponse],
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Aggregated event channel. Capacity sized for moderate burstiness;
	// producers drop on full (they'll re-emit on the next progress tick).
	eventCh := make(chan *apiv1.WatchChunksResponse, 256)

	// Local source: subscribe to this node's ChunkBus and forward typed
	// events. Empty node_id signals "from the connected node itself."
	bus := s.orch.ChunkBus()
	subID, events, baseline := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	var wg sync.WaitGroup
	wg.Go(func() {
		s.runLocalChunkEventForwarder(streamCtx, events, eventCh)
	})

	// Peer sources: open one ForwardWatchChunks stream per cluster node
	// the inspector might care about. Lifecycle events (CREATED / SEALED
	// / DELETED / UPLOADED) come through every node's local bus because
	// the vault-ctl FSM apply path fires on every cluster node — the
	// frontend handles the harmless duplicates via per-node version
	// tracking and idempotent setQueryData merges. PROGRESS events are
	// only emitted on the node that hosts the active chunk's leader, so
	// peer streaming is the only way they reach a client connected to a
	// node that doesn't host the vault. See gastrolog-3pf9w.
	if s.remoteChunkWatcher != nil {
		for _, nodeID := range s.peerNodeIDs(streamCtx) {
			nid := nodeID
			wg.Go(func() {
				s.runPeerChunkEventForwarder(streamCtx, nid, eventCh)
			})
		}
	}

	// Heartbeat carries the local baseline. Clients track per-node
	// versions; the empty node_id matches what local-bus events carry.
	if err := stream.Send(&apiv1.WatchChunksResponse{
		Op:      apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_UNSPECIFIED,
		Version: baseline,
	}); err != nil {
		return err
	}

	// Drain aggregated events → user-facing client stream. Producer
	// goroutines exit via streamCtx.Done() when this loop returns.
	defer wg.Wait()
	for {
		select {
		case <-streamCtx.Done():
			return nil
		case msg := <-eventCh:
			s.stampReplicaInfo(streamCtx, msg)
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

// stampReplicaInfo overlays authoritative cluster-wide replica and
// retention state on the outbound event's chunk meta, sourced from the
// vault-ctl FSM. Without this, the client has to reconstruct replica
// counts from per-node event attribution (which drifts — gastrolog-66vmg)
// and never sees retention_pending flip live (ListChunks-only overlay).
//
// Replica fields silently no-op when the FSM doesn't know about the chunk
// (single-node mode, memory vault, or the chunk has been finalized between
// event emit and relay): leaving replica_count zero on the wire signals
// "no authoritative value" to the client. Retention and pending-ack
// fields are stamped whenever the vault is known to the orchestrator.
func (s *VaultServer) stampReplicaInfo(ctx context.Context, msg *apiv1.WatchChunksResponse) {
	if msg == nil || msg.Meta == nil {
		return
	}
	if len(msg.VaultId) == 0 || len(msg.ChunkId) == 0 {
		return
	}
	vid := glid.FromBytes(msg.VaultId)
	if len(msg.ChunkId) != len(chunk.ChunkID{}) {
		return
	}
	var cid chunk.ChunkID
	copy(cid[:], msg.ChunkId)

	if pending := s.orch.RetentionPendingChunks(vid); pending != nil {
		msg.Meta.RetentionPending = pending[cid]
	}
	if acks := s.orch.PendingDeleteAcks(vid); acks != nil {
		if owed := acks[cid]; len(owed) > 0 {
			sortedOwed := append([]string(nil), owed...)
			sort.Strings(sortedOwed)
			msg.Meta.PendingAckNodeIds = sortedOwed
		} else {
			msg.Meta.PendingAckNodeIds = nil
		}
	}

	applyResidencyStamp(msg.Meta, s.orch.ChunkResidency(vid, cid))
}

// applyResidencyStamp stamps holder-receipt residency onto an outbound
// event meta. Empty or nil residency (chunk unknown, or known with zero
// receipts) stamps nothing: replica_count zero already means "no
// authoritative value" on the wire, so mergeMeta keeps the client's
// last authoritative set (or the ListChunks-seeded one). Both sources
// now carry the same holder-receipt semantics, so there is no
// cross-source ping-pong (gastrolog-68wsli).
func applyResidencyStamp(meta *apiv1.ChunkMeta, residency []string) {
	if len(residency) == 0 {
		return
	}
	sort.Strings(residency)
	meta.ReplicaNodeIds = residency
	if len(residency) > math.MaxInt32 {
		meta.ReplicaCount = math.MaxInt32
	} else {
		meta.ReplicaCount = int32(len(residency)) //nolint:gosec // G115: bounded by branch above
	}
}

// runLocalChunkEventForwarder drains the local ChunkBus subscription and
// translates each event into a wire WatchChunksResponse, fed into the
// aggregated event channel. The local node's ID is stamped onto every
// event so subscribers can attribute the source consistently with peer
// events — without it, the client can't tell which node produced a
// CREATED/SEALED/PROGRESS event and replica-count tracking that derives
// from per-node attribution undercounts the connected node. See
// gastrolog-4zy8a.
func (s *VaultServer) runLocalChunkEventForwarder(
	ctx context.Context,
	events <-chan notify.Versioned[orchestrator.ChunkChangeEvent],
	out chan<- *apiv1.WatchChunksResponse,
) {
	localNodeIDBytes := []byte(s.localNodeID)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			msg := chunkChangeEventToProto(ev.Event)
			msg.Version = ev.Version
			msg.NodeId = localNodeIDBytes
			if msg.Meta != nil {
				msg.Meta.VaultType = s.orch.VaultType(ev.Event.VaultID)
			}
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

// runPeerChunkEventForwarder opens a ForwardWatchChunks stream to a
// remote node and forwards each peer event into the aggregated channel,
// tagging it with the peer's node_id so the client can track per-node
// version. On error, retries with exponential backoff bounded by 5 s.
// Returns when ctx is cancelled.
func (s *VaultServer) runPeerChunkEventForwarder(
	ctx context.Context,
	nodeID string,
	out chan<- *apiv1.WatchChunksResponse,
) {
	nodeIDBytes := []byte(nodeID)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := s.remoteChunkWatcher.WatchChunks(ctx, nodeID, func(peerMsg *apiv1.ForwardWatchChunksResponse) error {
			msg := &apiv1.WatchChunksResponse{
				VaultId:     peerMsg.VaultId,
				ChunkId:     peerMsg.ChunkId,
				Op:          peerMsg.Op,
				Meta:        peerMsg.Meta,
				RecordCount: peerMsg.RecordCount,
				Version:     peerMsg.Version,
				NodeId:      nodeIDBytes,
			}
			select {
			case out <- msg:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.logger.Debug("watchchunks: peer stream errored, will retry",
				"node", nodeID, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// peerNodeIDs returns the IDs of all cluster nodes other than this one.
// Sourced from NodeStorageConfigs (every cluster node has one); skipped
// silently if the config store is unavailable.
func (s *VaultServer) peerNodeIDs(ctx context.Context) []string {
	nscs, err := s.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}
	var nodes []string
	for _, nsc := range nscs {
		id := nsc.NodeID
		if id == "" || id == s.localNodeID {
			continue
		}
		nodes = append(nodes, id)
	}
	return nodes
}

// chunkChangeEventToProto converts an orchestrator ChunkChangeEvent into
// the wire WatchChunksResponse. Lays out which fields are present per Op
// so the client merge logic can rely on the same contract.
func chunkChangeEventToProto(ev orchestrator.ChunkChangeEvent) *apiv1.WatchChunksResponse {
	msg := &apiv1.WatchChunksResponse{
		VaultId: ev.VaultID.ToProto(),
		ChunkId: ev.ChunkID[:],
		Op:      chunkOpToProto(ev.Op),
	}
	if ev.Meta != nil {
		// Mirror VaultChunkMetaToProto: the inner ChunkMeta needs
		// vault_id populated so the frontend's per-vault grouping
		// matches against the same vaultId the ListChunks path uses.
		// Bare ChunkMetaToProto leaves that field zero, which sends
		// the chunk into the renderer's "unknown" group and hides it.
		msg.Meta = ChunkMetaToProto(*ev.Meta)
		msg.Meta.VaultId = ev.VaultID.ToProto()
	}
	if ev.Op == orchestrator.ChunkChangeOpProgress {
		msg.RecordCount = ev.RecordCount
	}
	return msg
}

func chunkOpToProto(op orchestrator.ChunkChangeOp) apiv1.ChunkChangeOp {
	switch op {
	case orchestrator.ChunkChangeOpUnspecified:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_UNSPECIFIED
	case orchestrator.ChunkChangeOpCreated:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_CREATED
	case orchestrator.ChunkChangeOpProgress:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_PROGRESS
	case orchestrator.ChunkChangeOpSealed:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_SEALED
	case orchestrator.ChunkChangeOpDeleted:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_DELETED
	case orchestrator.ChunkChangeOpUploaded:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_UPLOADED
	default:
		return apiv1.ChunkChangeOp_CHUNK_CHANGE_OP_UNSPECIFIED
	}
}
