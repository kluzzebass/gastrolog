package orchestrator

import (
	"context"
	"errors"
	"gastrolog/internal/glid"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
	"gastrolog/internal/system"
)

// ejectTarget represents a resolved destination for an ejected record.
type ejectTarget struct {
	vaultID glid.GLID
	nodeID  string // empty = local
}

// resolvedRoute pairs a compiled filter with its resolved destinations.
type resolvedRoute struct {
	filter  *CompiledFilter // nil = no filter (matches nothing)
	targets []ejectTarget
}

// remoteKey identifies a unique (node, vault) destination for remote delivery.
type remoteKey struct {
	nodeID  string
	vaultID glid.GLID
}

// ejectChunk streams a sealed chunk's records through named routes, delivering
// to each route's matching destinations. After all records are delivered, the
// source chunk is deleted.
//
// This is the core retention-eject operation: records are decomposed from the
// sealed chunk, evaluated against each route's filter, and delivered to matching
// destinations (local via Append, remote via TransferRecords).
func (r *retentionRunner) ejectChunk(id chunk.ChunkID, routeIDs []glid.GLID) {
	ctx := context.Background()

	sys, err := r.orch.loadSystem(ctx)
	if err != nil {
		r.logger.Error("retention eject: failed to load config",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}

	routes := r.resolveEjectRoutes(sys, id, routeIDs)
	if routes == nil {
		return // logged inside resolveEjectRoutes
	}

	cursor, err := r.cm.OpenCursor(id)
	if err != nil {
		if errors.Is(err, chunk.ErrChunkSuspect) {
			r.logger.Warn("retention eject: chunk suspect (blob not found), skipping",
				"vault", r.vaultID, "chunk", id.String())
			return
		}
		r.logger.Error("retention eject: failed to open cursor",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}
	defer func() { _ = cursor.Close() }()

	remoteBuffers := make(map[remoteKey][]chunk.Record)
	recordCount, ok := r.deliverEjectRecords(id, cursor, routes, remoteBuffers)
	if !ok {
		return
	}

	if !r.flushRemoteBuffers(ctx, id, remoteBuffers) {
		return
	}

	// All records delivered — delete source chunk.
	r.expireChunk(id)
	r.logger.Info("retention eject: completed",
		"vault", r.vaultID, "chunk", id.String(),
		"records", recordCount, "routes", len(routes))
}

// resolveEjectRoutes compiles each route's filter and resolves destinations.
// Returns nil if resolution fails (errors are logged).
func (r *retentionRunner) resolveEjectRoutes(sys *system.System, id chunk.ChunkID, routeIDs []glid.GLID) []resolvedRoute {
	var routes []resolvedRoute
	for _, routeID := range routeIDs {
		route := findRoute(sys.Config.Routes, routeID)
		if route == nil {
			r.logger.Error("retention eject: route not found",
				"vault", r.vaultID, "chunk", id.String(), "route", routeID)
			return nil
		}
		if !route.Enabled {
			r.logger.Warn("retention eject: skipping disabled route",
				"vault", r.vaultID, "chunk", id.String(), "route", routeID)
			continue
		}

		resolved := r.resolveOneRoute(sys, routeID, route)
		if resolved == nil {
			return nil // compile error, logged inside
		}
		if len(resolved.targets) == 0 {
			r.logger.Warn("retention eject: route has no destinations",
				"vault", r.vaultID, "route", routeID)
			continue
		}
		routes = append(routes, *resolved)
	}

	if len(routes) == 0 {
		r.logger.Warn("retention eject: no valid routes resolved",
			"vault", r.vaultID, "chunk", id.String())
		return nil
	}
	return routes
}

// resolveOneRoute compiles a single route's filter and resolves its destinations.
// Returns nil on compile error (logged).
func (r *retentionRunner) resolveOneRoute(sys *system.System, routeID glid.GLID, route *system.RouteConfig) *resolvedRoute {
	cfg := &sys.Config
	var cf *CompiledFilter
	if route.FilterID != nil {
		filterExpr := resolveFilterExpr(cfg, *route.FilterID)
		compiled, err := CompileFilter(routeID, filterExpr)
		if err != nil {
			r.logger.Error("retention eject: failed to compile filter",
				"vault", r.vaultID, "route", routeID, "error", err)
			return nil
		}
		cf = compiled
	}

	var targets []ejectTarget
	for _, destID := range route.Destinations {
		nodeID := resolveVaultNodeID(sys, destID)
		if nodeID == r.orch.localNodeID {
			nodeID = ""
		}
		targets = append(targets, ejectTarget{vaultID: destID, nodeID: nodeID})
	}

	return &resolvedRoute{filter: cf, targets: targets}
}

// deliverEjectRecords reads all records from the cursor, evaluates route filters,
// and delivers matching records to local destinations (buffering remote ones).
// Returns the record count and true on success, false if a local append failed.
func (r *retentionRunner) deliverEjectRecords(
	id chunk.ChunkID,
	cursor chunk.RecordCursor,
	routes []resolvedRoute,
	remoteBuffers map[remoteKey][]chunk.Record,
) (int64, bool) {
	var recordCount int64
	for {
		rec, _, err := cursor.Next()
		if err != nil {
			break // io.EOF or read error — cursor exhausted
		}
		recordCount++

		for _, route := range routes {
			if !matchesEjectFilter(route.filter, rec.Attrs) {
				continue
			}
			if !r.deliverToTargets(id, rec, route.targets, remoteBuffers) {
				return recordCount, false
			}
		}
	}
	return recordCount, true
}

// deliverToTargets sends a record to all targets of a matched route.
// Local targets are appended immediately; remote targets are buffered.
// Returns false if a local append fails.
func (r *retentionRunner) deliverToTargets(
	id chunk.ChunkID,
	rec chunk.Record,
	targets []ejectTarget,
	remoteBuffers map[remoteKey][]chunk.Record,
) bool {
	for _, target := range targets {
		if target.nodeID == "" {
			if _, _, err := r.orch.Append(target.vaultID, rec); err != nil {
				r.logger.Error("retention eject: local append failed",
					"vault", r.vaultID, "chunk", id.String(),
					"dest", target.vaultID, "error", err)
				return false // Abort eject; chunk remains intact for retry.
			}
		} else {
			key := remoteKey{nodeID: target.nodeID, vaultID: target.vaultID}
			remoteBuffers[key] = append(remoteBuffers[key], rec)
		}
	}
	return true
}

// flushRemoteBuffers sends buffered records to remote nodes via ForwardAppend.
// Uses the unary ForwardRecords RPC so records are appended to the destination
// vault's active chunk (same as live ingestion), not imported as sealed chunks.
// Returns true on success, false if any transfer fails (logged).
func (r *retentionRunner) flushRemoteBuffers(ctx context.Context, id chunk.ChunkID, buffers map[remoteKey][]chunk.Record) bool {
	for key, records := range buffers {
		if r.orch.transferrer == nil {
			r.logger.Error("retention eject: no remote transferrer configured",
				"vault", r.vaultID, "dest_node", key.nodeID, "dest_vault", key.vaultID)
			return false
		}
		if err := r.orch.transferrer.ForwardAppend(ctx, key.nodeID, key.vaultID, records); err != nil {
			r.logger.Error("retention eject: remote append failed",
				"vault", r.vaultID, "chunk", id.String(),
				"dest_node", key.nodeID, "dest_vault", key.vaultID, "error", err)
			return false // Abort; chunk remains intact for retry.
		}
	}
	return true
}

// matchesEjectFilter evaluates a compiled filter against record attributes.
// If the filter is nil (route has no filter), no records match.
func matchesEjectFilter(cf *CompiledFilter, attrs chunk.Attributes) bool {
	if cf == nil {
		return false
	}
	switch cf.Kind {
	case FilterNone:
		return false
	case FilterCatchAll:
		return true
	case FilterExpr:
		return querylang.MatchAttrs(cf.DNF, attrs)
	case FilterCatchRest:
		return true // In eject context, catch-rest acts as catch-all.
	}
	return false
}

// findRoute finds a RouteConfig by ID in a slice.
func findRoute(routes []system.RouteConfig, id glid.GLID) *system.RouteConfig {
	for i := range routes {
		if routes[i].ID == id {
			return &routes[i]
		}
	}
	return nil
}
