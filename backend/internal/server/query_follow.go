package server

import (
	"context"
	"errors"
	"iter"
	"sync"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// Follow executes a query and streams matching records, continuously polling for new arrivals.
// This is a tail -f style operation that never completes until the client disconnects.
func (s *QueryServer) Follow(
	ctx context.Context,
	req *connect.Request[apiv1.FollowRequest],
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	if s.maxFollowDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.maxFollowDuration)
		defer cancel()
	}

	eng := s.orch.PrimaryTierQueryEngine()

	q, pipeline, err := protoToQuery(req.Msg.Query)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Pipeline queries: allow non-aggregating streaming-compatible operators in
	// follow mode. Reject stats (needs all records), sort and tail (not streaming).
	if pipeline != nil && len(pipeline.Pipes) > 0 {
		for _, pipe := range pipeline.Pipes {
			switch pipe.(type) {
			case *querylang.StatsOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("stats operator is not supported in follow mode"))
			case *querylang.SortOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("sort operator is not supported in follow mode"))
			case *querylang.TailOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("tail operator is not supported in follow mode"))
			case *querylang.SliceOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("slice operator is not supported in follow mode"))
			case *querylang.TimechartOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("timechart operator is not supported in follow mode"))
			case *querylang.BarchartOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("barchart operator is not supported in follow mode"))
			case *querylang.DonutOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("donut operator is not supported in follow mode"))
			case *querylang.HeatmapOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("heatmap operator is not supported in follow mode"))
			case *querylang.MapOp:
				return connect.NewError(connect.CodeInvalidArgument,
					errors.New("map operator is not supported in follow mode"))
			}
		}
	}

	// Start remote follow streams for vaults on other nodes.
	remoteRecords := s.startRemoteFollows(ctx, q)

	// Local follow for vaults on this node.
	localIter := eng.Follow(ctx, q)

	// Merge local and remote records and stream to the client.
	return s.mergeFollowStreams(ctx, localIter, remoteRecords, stream)
}

// startRemoteFollows opens ForwardFollow streams to all remote nodes that own
// vaults matching the query. Returns a channel that carries records from all
// remote streams combined.
func (s *QueryServer) startRemoteFollows(ctx context.Context, q query.Query) <-chan *apiv1.Record {
	if s.remoteSearcher == nil || s.cfgStore == nil {
		return nil
	}

	selectedVaults, _ := query.ExtractVaultFilter(q.Normalize().BoolExpr, nil)
	byNode := s.remoteVaultsByNode(ctx, selectedVaults)
	if len(byNode) == 0 {
		return nil
	}

	queryExpr := q.String()
	merged := make(chan *apiv1.Record, 64)
	var wg sync.WaitGroup

	for nodeID, vaultIDs := range byNode {
		vaultStrs := make([]string, len(vaultIDs))
		for i, v := range vaultIDs {
			vaultStrs[i] = v.String()
		}

		wg.Add(1)
		go func(nodeID string, vaultStrs []string) {
			defer wg.Done()

			recCh, errCh := s.remoteSearcher.Follow(ctx, nodeID, &apiv1.ForwardFollowRequest{
				VaultIds: vaultStrs,
				Query:    queryExpr,
			})

			for {
				select {
				case rec, ok := <-recCh:
					if !ok {
						// Check for error after channel closes.
						if err := <-errCh; err != nil {
							s.logger.Warn("follow: remote stream error", "node", nodeID, "err", err)
						}
						return
					}
					select {
					case merged <- exportToRecord(rec):
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(nodeID, vaultStrs)
	}

	// Close merged channel when all remote streams end.
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// mergeFollowStreams interleaves local follow records with remote records
// and streams them to the client. Records are sent immediately as they arrive
// — there's no ordering guarantee in follow mode (it's real-time tailing).
func (s *QueryServer) mergeFollowStreams(
	ctx context.Context,
	localIter iter.Seq2[chunk.Record, error],
	remoteRecords <-chan *apiv1.Record,
	stream *connect.ServerStream[apiv1.FollowResponse],
) error {
	// If no remote records, just stream local.
	if remoteRecords == nil {
		return streamLocalFollow(localIter, stream)
	}

	// Both local and remote: run local in a goroutine, merge via channel.
	localCh := make(chan localFollowMsg, 64)
	go func() {
		defer close(localCh)
		for rec, err := range localIter {
			select {
			case localCh <- localFollowMsg{rec: rec, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-localCh:
			if !ok {
				return drainRemoteFollow(remoteRecords, stream)
			}
			if err := sendLocalFollowMsg(msg, stream); err != nil {
				return err
			}
		case rec, ok := <-remoteRecords:
			if !ok {
				return drainLocalFollow(localCh, stream)
			}
			if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{rec}}); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

type localFollowMsg struct {
	rec chunk.Record
	err error
}

// streamLocalFollow streams all records from a local follow iterator.
func streamLocalFollow(localIter iter.Seq2[chunk.Record, error], stream *connect.ServerStream[apiv1.FollowResponse]) error {
	for rec, err := range localIter {
		if err != nil {
			return followError(err)
		}
		if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{recordToProto(rec)}}); err != nil {
			return err
		}
	}
	return nil
}

// sendLocalFollowMsg sends a single local follow message to the stream.
func sendLocalFollowMsg(msg localFollowMsg, stream *connect.ServerStream[apiv1.FollowResponse]) error {
	if msg.err != nil {
		return followError(msg.err)
	}
	return stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{recordToProto(msg.rec)}})
}

// drainRemoteFollow streams remaining remote records after local closes.
func drainRemoteFollow(remoteRecords <-chan *apiv1.Record, stream *connect.ServerStream[apiv1.FollowResponse]) error {
	for rec := range remoteRecords {
		if err := stream.Send(&apiv1.FollowResponse{Records: []*apiv1.Record{rec}}); err != nil {
			return err
		}
	}
	return nil
}

// drainLocalFollow streams remaining local records after remote closes.
func drainLocalFollow(localCh <-chan localFollowMsg, stream *connect.ServerStream[apiv1.FollowResponse]) error {
	for msg := range localCh {
		if err := sendLocalFollowMsg(msg, stream); err != nil {
			return err
		}
	}
	return nil
}

// followError returns nil for normal termination (context cancelled/deadline)
// or wraps the error as a connect error.
func followError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return connect.NewError(connect.CodeInternal, err)
}
