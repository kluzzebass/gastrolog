package server

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
)

// ExportToVault materializes search results into a target vault as a
// background job. Returns a job ID for progress tracking.
func (s *QueryServer) ExportToVault(
	ctx context.Context,
	req *connect.Request[apiv1.ExportToVaultRequest],
) (*connect.Response[apiv1.ExportToVaultResponse], error) {
	if req.Msg.Expression == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("expression required"))
	}

	// Resolve target: explicit field takes priority, otherwise extract from | export <target> in expression.
	target := req.Msg.Target
	if target == "" {
		q2, pipeline2, parseErr := parseExpression(req.Msg.Expression)
		if parseErr != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
		}
		_ = q2
		if exportOp, ok := querylang.HasExportOp(pipeline2); ok {
			target = exportOp.Target
		}
	}
	if target == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("target vault required"))
	}

	// Resolve target vault: try UUID first, then name lookup.
	targetVaultID, targetName, err := s.resolveTargetVault(ctx, target)
	if err != nil {
		return nil, err
	}

	// Parse expression and extract pipeline.
	q, pipeline, parseErr := parseExpression(req.Msg.Expression)
	if parseErr != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
	}

	// Strip ExportOp from pipeline if present (the remaining ops transform records).
	if pipeline != nil {
		pipeline.Pipes = stripExportOp(pipeline.Pipes)
		if len(pipeline.Pipes) == 0 {
			pipeline = nil
		}
	}

	// Same-vault guard: exclude target vault from sources.
	q, guardErr := s.excludeTargetVault(q, targetVaultID)
	if guardErr != nil {
		return nil, guardErr
	}

	// Forward to remote node if the target vault isn't local.
	if nodeID := s.remoteNodeForTargetVault(ctx, targetVaultID); nodeID != "" {
		if s.remoteSearcher == nil {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("remote vault forwarding not configured"))
		}
		fwdResp, fwdErr := s.remoteSearcher.ExportToVault(ctx, nodeID, &apiv1.ForwardExportToVaultRequest{
			Expression:    req.Msg.Expression,
			TargetVaultId: targetVaultID.ToProto(),
		})
		if fwdErr != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("forward to %s: %w", nodeID, fwdErr))
		}
		return connect.NewResponse(&apiv1.ExportToVaultResponse{
			JobId: fwdResp.GetJobId(),
		}), nil
	}

	// Submit the export as a background job on this node.
	jobName := "export to " + targetName
	jobID := s.orch.Scheduler().Submit(jobName, func(jobCtx context.Context, job *orchestrator.JobProgress) {
		s.runExportJob(jobCtx, job, q, pipeline, targetVaultID)
	})

	return connect.NewResponse(&apiv1.ExportToVaultResponse{JobId: []byte(jobID)}), nil
}

// RunExportJob is the public entry point for export jobs forwarded from remote
// nodes. It resolves the vault, parses the expression, and submits the job.
func (s *QueryServer) RunExportJob(ctx context.Context, expression string, targetVaultID glid.GLID) (string, error) {
	q, pipeline, err := parseExpression(expression)
	if err != nil {
		return "", err
	}

	// Strip ExportOp from pipeline if present.
	if pipeline != nil {
		pipeline.Pipes = stripExportOp(pipeline.Pipes)
		if len(pipeline.Pipes) == 0 {
			pipeline = nil
		}
	}

	targetName := targetVaultID.String()
	jobName := "export to " + targetName
	jobID := s.orch.Scheduler().Submit(jobName, func(jobCtx context.Context, job *orchestrator.JobProgress) {
		s.runExportJob(jobCtx, job, q, pipeline, targetVaultID)
	})
	return jobID, nil
}

// runExportJob executes the export: searches across all nodes, applies pipeline
// transforms, and appends matching records to the target vault.
func (s *QueryServer) runExportJob(
	ctx context.Context,
	job *orchestrator.JobProgress,
	q query.Query,
	pipeline *querylang.Pipeline,
	targetVaultID glid.GLID,
) {
	eng := s.orch.LeaderVaultQueryEngine()
	if s.lookupResolver != nil {
		eng.SetLookupResolver(s.lookupResolver)
	}

	var records []chunk.Record

	// Drain all remote records by paginating through collectRemote.
	// Each call returns up to one batch per vault; loop until exhausted.
	remoteRecords := s.drainRemoteRecords(ctx, q)

	hasMaterializingPipeline := pipeline != nil && len(pipeline.Pipes) > 0 && !query.CanStreamPipeline(pipeline)
	hasStreamingPipeline := pipeline != nil && len(pipeline.Pipes) > 0 && !hasMaterializingPipeline

	if hasMaterializingPipeline {
		result, err := eng.RunPipelineOnRecords(ctx, q, pipeline, remoteRecords)
		if err != nil {
			job.Fail(s.now(), fmt.Sprintf("pipeline execution: %v", err))
			return
		}
		records = result.Records
	} else {
		localIter, _ := eng.Search(ctx, q, nil)
		for rec, err := range localIter {
			if err != nil {
				job.Fail(s.now(), fmt.Sprintf("search: %v", err))
				return
			}
			records = append(records, rec)
		}
		records = append(records, remoteRecords...)
	}

	if hasStreamingPipeline {
		transform := query.NewRecordTransform(pipeline.Pipes, s.lookupResolver)
		var filtered []chunk.Record
		for _, rec := range records {
			if transformed, ok := transform.Apply(ctx, rec); ok {
				filtered = append(filtered, transformed)
			}
		}
		records = filtered
	}

	s.logger.Info("export job starting append",
		slog.Int("records", len(records)),
		slog.String("target_vault", targetVaultID.String()),
	)

	// Append each record to the target vault.
	for _, rec := range records {
		// Build a clean record for append — preserve timestamps and attrs,
		// but let the target vault assign fresh WriteTS.
		appendRec := chunk.Record{
			Raw:      rec.Raw,
			SourceTS: rec.SourceTS,
			IngestTS: rec.IngestTS,
			EventID:  rec.EventID,
		}
		if len(rec.Attrs) > 0 {
			appendRec.Attrs = make(chunk.Attributes, len(rec.Attrs))
			maps.Copy(appendRec.Attrs, rec.Attrs)
		}

		if _, _, err := s.orch.Append(targetVaultID, appendRec); err != nil {
			job.Fail(s.now(), fmt.Sprintf("append to vault: %v", err))
			return
		}
		job.AddRecords(1)
	}

	job.Complete(s.now())
}

// resolveTargetVault resolves a target string to a vault UUID.
// Tries UUID parse first, then name lookup in system.
func (s *QueryServer) resolveTargetVault(ctx context.Context, target string) (glid.GLID, string, *connect.Error) {
	// Try UUID first.
	if id, err := glid.ParseUUID(target); err == nil {
		return s.resolveVaultByID(ctx, id, target)
	}

	// Name lookup.
	if s.cfgStore == nil {
		return glid.Nil, "", connect.NewError(connect.CodeNotFound, fmt.Errorf("vault %q not found (no config store)", target))
	}
	allVaults, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return glid.Nil, "", connect.NewError(connect.CodeInternal, fmt.Errorf("list vaults: %w", err))
	}

	var matches []system.VaultConfig
	for _, vc := range allVaults {
		if strings.EqualFold(vc.Name, target) {
			matches = append(matches, vc)
		}
	}

	switch len(matches) {
	case 0:
		return glid.Nil, "", connect.NewError(connect.CodeNotFound, fmt.Errorf("vault %q not found", target))
	case 1:
		return matches[0].ID, matches[0].Name, nil
	default:
		return glid.Nil, "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("vault name %q is ambiguous (%d matches), use vault UUID instead", target, len(matches)))
	}
}

func (s *QueryServer) resolveVaultByID(ctx context.Context, id glid.GLID, target string) (glid.GLID, string, *connect.Error) {
	if s.cfgStore != nil {
		allVaults, err := s.cfgStore.ListVaults(ctx)
		if err == nil {
			for _, vc := range allVaults {
				if vc.ID == id {
					return id, vc.Name, nil
				}
			}
		}
	}
	if slices.Contains(s.orch.ListVaults(), id) {
		return id, id.String(), nil
	}
	return glid.Nil, "", connect.NewError(connect.CodeNotFound, fmt.Errorf("vault %s not found", target))
}

// drainRemoteRecords collects all remote records into a slice by draining
// the streaming iterator returned by collectRemote.
func (s *QueryServer) drainRemoteRecords(ctx context.Context, q query.Query) []chunk.Record {
	remoteIter, _, _ := s.collectRemote(ctx, q, nil)
	if remoteIter == nil {
		return nil
	}
	var all []chunk.Record
	for rec, err := range remoteIter {
		if err != nil {
			break
		}
		all = append(all, rec)
	}
	return all
}

// excludeTargetVault ensures the target vault is not searched as a source.
// If the query has no vault filter, it injects NOT vault_id=<target>.
// If the query explicitly includes the target, it rejects with an error.
func (s *QueryServer) excludeTargetVault(q query.Query, targetVaultID glid.GLID) (query.Query, *connect.Error) {
	nq := q.Normalize()
	sourceVaults, _ := query.ExtractVaultFilter(nq.BoolExpr, nil)

	if sourceVaults == nil {
		// No vault filter — inject NOT vault_id=<target> to auto-exclude.
		exclusion := &querylang.NotExpr{
			Term: &querylang.PredicateExpr{
				Kind:  querylang.PredKV,
				Key:   "vault_id",
				Value: targetVaultID.String(),
			},
		}
		if q.BoolExpr != nil {
			q.BoolExpr = &querylang.AndExpr{Terms: []querylang.Expr{q.BoolExpr, exclusion}}
		} else {
			q.BoolExpr = exclusion
		}
		return q, nil
	}

	if slices.Contains(sourceVaults, targetVaultID) {
		return q, connect.NewError(connect.CodeInvalidArgument,
			errors.New("cannot export into a source vault"))
	}
	return q, nil
}

// remoteNodeForTargetVault returns the owning node ID if the vault is remote.
// Returns "" if the vault is local or not found.
func (s *QueryServer) remoteNodeForTargetVault(ctx context.Context, vaultID glid.GLID) string {
	// Check if the vault is local.
	if slices.Contains(s.orch.ListVaults(), vaultID) {
		return ""
	}
	// Look up in config for remote node.
	if s.cfgStore == nil {
		return ""
	}
	allCfg, err := s.cfgStore.ListVaults(ctx)
	if err != nil {
		return ""
	}
	for _, vc := range allCfg {
		if vc.ID == vaultID {
			return "" // vaults no longer have a NodeID
		}
	}
	return ""
}

// stripExportOp removes ExportOp from the pipe list (always the last one).
func stripExportOp(pipes []querylang.PipeOp) []querylang.PipeOp {
	var result []querylang.PipeOp
	for _, op := range pipes {
		if _, ok := op.(*querylang.ExportOp); !ok {
			result = append(result, op)
		}
	}
	return result
}

// now returns the current time (for use in job progress).
func (s *QueryServer) now() time.Time { return time.Now() }

// ExportToVaultFunc returns a function suitable for use as a cluster
// ExportToVaultExecutor, delegating to this QueryServer's export logic.
// The closure captures s (not s.queryServer) so it works even when wired
// before buildMux initializes queryServer.
func (s *Server) ExportToVaultFunc() func(ctx context.Context, expression string, targetVaultID glid.GLID) (string, error) {
	return func(ctx context.Context, expression string, targetVaultID glid.GLID) (string, error) {
		return s.queryServer.RunExportJob(ctx, expression, targetVaultID)
	}
}
