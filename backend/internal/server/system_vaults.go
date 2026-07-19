package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gastrolog/internal/glid"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
)

// vaultQuantity describes one operator-authored config quantity on a vault:
// which field it lands in, what it defaults to, and whether it applies to this
// vault's type at all.
type vaultQuantity struct {
	flag    string // operator-facing name, for error messages
	applies bool   // false → this vault type has no such quantity; leave it alone
	in      string // the incoming expression from the wire
	dst     *string
	def     string                          // default expression when unset on create
	prev    func(system.VaultConfig) string // the stored value, for preserve-on-update
}

// resolveVaultQuantities settles every config quantity on a vault from one set
// of rules, rather than a near-identical function per field (gastrolog-etcjdx):
//
//   - set          → stored verbatim, as the operator typed it, after a
//     parse-check so an unparseable expression fails at write rather than
//     surfacing later at use.
//   - "0"          → rejected: an explicit zero means "accept nothing" or "no
//     bound", the two states this model exists to prevent.
//   - unset, create → the default expression.
//   - unset, update → the stored value, so an update that does not mention a
//     quantity never silently re-defaults a chosen one.
func resolveVaultQuantities(p *apiv1.VaultConfig, vaultCfg *system.VaultConfig, existing []system.VaultConfig) *connect.Error {
	for _, q := range []vaultQuantity{
		{
			flag: "cache-budget", applies: vaultCfg.IsCloud(),
			in: p.GetCacheBudget(), dst: &vaultCfg.CacheBudget, def: system.DefaultVaultCacheBudget,
			prev: func(v system.VaultConfig) string { return v.CacheBudget },
		},
		{
			flag: "memory-budget", applies: vaultCfg.Type == system.VaultTypeMemory,
			in: p.GetMemoryBudget(), dst: &vaultCfg.MemoryBudget, def: system.DefaultVaultMemoryBudget,
			prev: func(v system.VaultConfig) string { return v.MemoryBudget },
		},
	} {
		if !q.applies {
			continue
		}
		if connErr := resolveVaultQuantity(q, vaultCfg, existing); connErr != nil {
			return connErr
		}
	}
	return nil
}

func resolveVaultQuantity(q vaultQuantity, vaultCfg *system.VaultConfig, existing []system.VaultConfig) *connect.Error {
	if !system.IsQuantityUnset(q.in) {
		bytes, err := system.ParseSize(q.in)
		if err != nil {
			// Budgets are size-only: a %-of-volume budget does not compose —
			// N vaults at 10% each overcommit the shared volume (see the
			// max-size decision in docs/product-defaults-policy-design.md).
			// Name that reason instead of a bare unknown-unit error.
			if sp, perr := system.ParseSizeOrPercent(q.in); perr == nil && sp.IsPercent() {
				return errInvalidArg(fmt.Errorf(
					"%s %q on vault %q: a percentage of the volume is not allowed here — per-vault shares do not compose across vaults on the same volume; use an absolute size (e.g. %s)",
					q.flag, q.in, vaultCfg.Name, q.def))
			}
			return errInvalidArg(fmt.Errorf("%s %q on vault %q: %w", q.flag, q.in, vaultCfg.Name, err))
		}
		if bytes == 0 {
			return errInvalidArg(fmt.Errorf(
				"%s of %q on vault %q means no bound; omit it for the default (%s) or set a real size",
				q.flag, q.in, vaultCfg.Name, q.def))
		}
		*q.dst = q.in // the operator's expression, verbatim
		return nil
	}
	for i := range existing {
		if existing[i].ID == vaultCfg.ID {
			*q.dst = q.prev(existing[i]) // preserve on update
			return nil
		}
	}
	*q.dst = q.def // default on create
	return nil
}

// validateVaultExpressions parse-checks the quantities that carry no default —
// an empty value is legitimately "inherit" or "off" — so a malformed one is
// caught at write instead of at use (gastrolog-etcjdx).
//
// The disk-free thresholds are the only volume-relative fields: they accept a
// percentage of the volume ("10%") alongside an absolute size, because the
// threshold guards the vault's own volume, so a share composes. An explicit
// zero ("0", "0%") would disable the guard for this vault and is rejected,
// like the explicit-0 budgets.
func validateVaultExpressions(vaultCfg *system.VaultConfig) *connect.Error {
	for _, f := range []struct {
		flag string
		expr string
	}{
		{"disk-free-warn", vaultCfg.DiskFreeWarn},
		{"disk-free-floor", vaultCfg.DiskFreeFloor},
	} {
		if system.IsQuantityUnset(f.expr) {
			continue
		}
		sp, err := system.ParseSizeOrPercent(f.expr)
		if err != nil {
			return errInvalidArg(fmt.Errorf("%s %q on vault %q: %w", f.flag, f.expr, vaultCfg.Name, err))
		}
		if sp.IsZero() {
			return errInvalidArg(fmt.Errorf(
				"%s of %q on vault %q disables the guard; omit it to inherit the node default, or set a real size or percentage",
				f.flag, f.expr, vaultCfg.Name))
		}
	}
	if !system.IsQuantityUnset(vaultCfg.CacheTTL) {
		if _, err := system.ParseDuration(vaultCfg.CacheTTL); err != nil {
			return errInvalidArg(fmt.Errorf("cache-ttl %q on vault %q: %w", vaultCfg.CacheTTL, vaultCfg.Name, err))
		}
	}
	return nil
}

// validateRetentionTransferDisposition enforces the gastrolog-2l918
// transfer-disposition config rules at write time, not at retention-sweep
// time: disposition "transfer" requires a target vault ID; the target must
// not be the source vault (self-transfer is the cascade footgun — a
// transferred chunk would immediately re-qualify for the same rule); and
// per spec decision #4, transfer is file → file only (cloud-backed and
// memory vaults have different at-rest forms and lifecycle machinery, so
// their pairing with transfer is an explicit config error rather than a
// runtime surprise on the first retention sweep). vaults is the
// already-loaded vault list (existing vaults, for the target lookup);
// vaultCfg is the (already resolved) incoming config.
func validateRetentionTransferDisposition(vaultCfg system.VaultConfig, vaults []system.VaultConfig) *connect.Error {
	if vaultCfg.ResolveRetentionDisposition() != system.RetentionDispositionTransfer {
		return nil
	}
	if vaultCfg.RetentionTransferTargetVaultID == nil {
		return errInvalidArg(fmt.Errorf(
			"vault %q: retention_disposition=transfer requires a retention_transfer_target_vault_id", vaultCfg.Name))
	}
	targetID := *vaultCfg.RetentionTransferTargetVaultID
	if targetID == vaultCfg.ID {
		return errInvalidArg(fmt.Errorf(
			"vault %q: retention transfer target cannot be the vault itself (self-transfer is a retention cascade)", vaultCfg.Name))
	}
	var target *system.VaultConfig
	for i := range vaults {
		if vaults[i].ID == targetID {
			target = &vaults[i]
			break
		}
	}
	if target == nil {
		return errInvalidArg(fmt.Errorf(
			"vault %q: retention transfer target %s not found", vaultCfg.Name, targetID))
	}
	if vaultCfg.Type != system.VaultTypeFile || vaultCfg.IsCloud() {
		return errInvalidArg(fmt.Errorf(
			"vault %q: retention_disposition=transfer requires a plain file-typed source vault (got %s, cloud=%t) — cloud-backed and memory vaults have different at-rest forms and lifecycle machinery",
			vaultCfg.Name, vaultCfg.Type, vaultCfg.IsCloud()))
	}
	if target.Type != system.VaultTypeFile || target.IsCloud() {
		return errInvalidArg(fmt.Errorf(
			"vault %q: retention transfer target %q must be a plain file vault (got %s, cloud=%t) — cloud-backed and memory vaults have different at-rest forms and lifecycle machinery",
			vaultCfg.Name, target.Name, target.Type, target.IsCloud()))
	}
	return detectTransferCycle(vaultCfg, vaults)
}

// detectTransferCycle rejects a transfer-target graph that would cycle
// back to the writing vault — A→B→A, or any longer chain A→B→C→A
// (gastrolog-2l918 review finding 3a). Self-transfer (the 1-hop cycle) is
// already rejected above; this generalizes to the multi-hop case, which
// self-transfer's simple equality check cannot catch. The graph is tiny
// (one edge per vault, at most len(vaults) hops), so a plain walk with a
// seen-set is the whole algorithm — no need for anything fancier.
//
// vaults is the existing vault list; vaultCfg is the incoming (not yet
// persisted) config for its own ID, so the walk uses vaultCfg — not
// whatever is currently stored — as the starting point and as the
// resolution for its own ID if it appears again later in the chain (a
// vault cannot cycle back to a stale view of itself).
func detectTransferCycle(vaultCfg system.VaultConfig, vaults []system.VaultConfig) *connect.Error {
	byID := make(map[glid.GLID]system.VaultConfig, len(vaults)+1)
	for _, v := range vaults {
		byID[v.ID] = v
	}
	byID[vaultCfg.ID] = vaultCfg

	chain := []string{vaultCfg.Name}
	seen := map[glid.GLID]bool{vaultCfg.ID: true}
	cur := vaultCfg
	for cur.ResolveRetentionDisposition() == system.RetentionDispositionTransfer && cur.RetentionTransferTargetVaultID != nil {
		nextID := *cur.RetentionTransferTargetVaultID
		next, ok := byID[nextID]
		if !ok {
			return nil // target doesn't exist — the "not found" check above already covers this
		}
		chain = append(chain, next.Name)
		if seen[nextID] {
			return errInvalidArg(fmt.Errorf(
				"vault %q: retention transfer target graph has a cycle: %s", vaultCfg.Name, strings.Join(chain, " -> ")))
		}
		seen[nextID] = true
		cur = next
	}
	return nil
}

// checkVaultShapeImmutable rejects PutVault when an existing vault's shape
// fields (type, cloud_service_id) would change. New vaults pass through —
// the existing-vault lookup returns nil and we have nothing to compare.
// See gastrolog-3ul0s for the failure mode this guards against.
func checkVaultShapeImmutable(ctx context.Context, store system.Store, incoming system.VaultConfig) *connect.Error {
	existing, err := store.GetVault(ctx, incoming.ID)
	if err != nil {
		return errInternal(err)
	}
	if existing == nil {
		return nil // creating a new vault — no shape to preserve
	}
	if existing.Type != incoming.Type {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("vault type is immutable: cannot change %s → %s on existing vault %q (create a new vault and migrate)",
				existing.Type, incoming.Type, incoming.Name))
	}
	if !cloudServiceIDEqual(existing.CloudServiceID, incoming.CloudServiceID) {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("vault cloud_service_id is immutable on existing vault %q: changing it would orphan blobs in the old bucket (create a new vault and migrate)",
				incoming.Name))
	}
	return nil
}

// cloudServiceIDEqual compares two optional *glid.GLID values. Both nil and
// both pointing at the same GLID are equal; everything else is a change.
func cloudServiceIDEqual(a, b *glid.GLID) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// PutVault creates or updates a vault.
func (s *SystemServer) PutVault(
	ctx context.Context,
	req *connect.Request[apiv1.PutVaultRequest],
) (*connect.Response[apiv1.PutVaultResponse], error) {
	if req.Msg.Config == nil {
		return nil, errRequired("config")
	}
	if len(req.Msg.Config.Id) == 0 {
		req.Msg.Config.Id = glid.New().ToProto()
	}
	if req.Msg.Config.Name == "" {
		return nil, errRequired("name")
	}

	vaultCfg, err := protoToVaultConfig(req.Msg.Config)
	if err != nil {
		return nil, errInvalidArg(err)
	}

	// Reject duplicate names.
	vaults, err := s.sysStore.ListVaults(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("vault", vaultCfg.ID, vaultCfg.Name, vaults, func(v system.VaultConfig) (glid.GLID, string) { return v.ID, v.Name }); connErr != nil {
		return nil, connErr
	}

	// Resolve the vault's cache/memory budgets: the wire distinguishes
	// "unset" (absent) from "explicitly 0" (present, zero), and they mean
	// opposite things (gastrolog-1epfgb). This is the single ingress for
	// every surface — CLI create, UI, and config import all call PutVault —
	// so resolving here makes an unbounded vault unrepresentable regardless
	// of who asked.
	if connErr := resolveVaultQuantities(req.Msg.Config, &vaultCfg, vaults); connErr != nil {
		return nil, connErr
	}
	if connErr := validateVaultExpressions(&vaultCfg); connErr != nil {
		return nil, connErr
	}
	if connErr := validateRetentionTransferDisposition(vaultCfg, vaults); connErr != nil {
		return nil, connErr
	}

	// Shape immutability: `type` and `cloud_service_id` determine *where*
	// and *how* this vault's chunks are stored. Changing either on a vault
	// with existing chunks would either reinterpret on-disk layout with
	// the wrong manager (type swap) or orphan blobs in the old cloud
	// bucket while pointing the new manager at an empty one (cloud-service
	// swap). The running orchestrator's applyExistingVaultChanges only
	// reloads filters / rotation / retention, so the change looks like a
	// no-op in dev — until the next restart rebuilds the manager from the
	// updated config. See gastrolog-3ul0s.
	if connErr := checkVaultShapeImmutable(ctx, s.sysStore, vaultCfg); connErr != nil {
		return nil, connErr
	}

	// Note: vault ID validation is intentionally omitted here.
	// RouteLeader RPCs run on any node with Raft writes forwarded to the leader,
	// but reads are local. In a multi-node cluster, vaults created moments before
	// the vault may not have replicated to this node's FSM yet. The orchestrator's
	// buildVaultInstances handles missing vaults gracefully (logs a warning, skips).
	// Referential integrity is enforced on the delete path (DeleteVault rejects
	// if any vault references the vault).

	// Persist to config store. For raft stores, the FSM notification callback
	// handles orchestrator side effects. For non-raft stores, notify() does.
	if err := s.sysStore.PutVault(ctx, vaultCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: vaultCfg.ID})

	// Run placement synchronously so the response includes placements.
	if s.placementReconcile != nil {
		s.placementReconcile(ctx)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutVaultResponse{System: cfg}), nil
}

// DeleteVault removes a vault. If force is false, the vault must be empty.
// If force is true, the vault is removed regardless of content: active chunks are sealed,
// all indexes and chunks are deleted, and for file vaults the vault directory is removed.
func (s *SystemServer) DeleteVault(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteVaultRequest],
) (*connect.Response[apiv1.DeleteVaultResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	// Verify the vault exists in config before touching the orchestrator.
	// The vault may belong to another node (not in local orchestrator) but
	// must exist in the shared config store.
	existing, err := s.sysStore.GetVault(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if existing == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	// Referential integrity: reject if any route references this vault as a destination.
	if routeID, used, err := s.vaultReferencedByRoute(ctx, id); err != nil {
		return nil, errInternal(err)
	} else if used {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("vault %q is referenced as destination in route %q", req.Msg.Id, routeID))
	}

	if req.Msg.Force {
		if err := s.forceDeleteVault(id); err != nil {
			return nil, err
		}
	} else {
		if err := s.removeVault(id); err != nil {
			return nil, err
		}
	}

	if err := s.sysStore.DeleteVault(ctx, id, req.Msg.GetDeleteData()); err != nil {
		return nil, errInternal(err)
	}

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteVaultResponse{System: cfg}), nil
}

func (s *SystemServer) forceDeleteVault(id glid.GLID) error {
	if err := s.orch.ForceRemoveVault(id); err != nil && !errors.Is(err, orchestrator.ErrVaultNotFound) {
		return errInternal(err)
	}
	return nil
}

func (s *SystemServer) removeVault(id glid.GLID) error {
	err := s.orch.RemoveVault(id)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, orchestrator.ErrVaultNotFound):
		// Expected when the vault belongs to another node — the owning
		// node's FSM dispatcher handles its own runtime cleanup.
		return nil
	case errors.Is(err, orchestrator.ErrVaultNotEmpty):
		return errPrecondition(err)
	default:
		return errInternal(err)
	}
}

// PauseVault disables ingestion for a vault.
// It reads the current config, flips Enabled to false, and writes it back.
// The VaultPut FSM notification handles the runtime DisableVault call.
func (s *SystemServer) PauseVault(
	ctx context.Context,
	req *connect.Request[apiv1.PauseVaultRequest],
) (*connect.Response[apiv1.PauseVaultResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	vaultCfg, err := s.sysStore.GetVault(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if vaultCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	vaultCfg.Enabled = false
	if err := s.sysStore.PutVault(ctx, *vaultCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PauseVaultResponse{System: cfg}), nil
}

// ResumeVault enables ingestion for a vault.
// It reads the current config, flips Enabled to true, and writes it back.
// The VaultPut FSM notification handles the runtime EnableVault call.
func (s *SystemServer) ResumeVault(
	ctx context.Context,
	req *connect.Request[apiv1.ResumeVaultRequest],
) (*connect.Response[apiv1.ResumeVaultResponse], error) {
	if len(req.Msg.Id) == 0 {
		return nil, errRequired("id")
	}

	id, connErr := parseProtoID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	vaultCfg, err := s.sysStore.GetVault(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if vaultCfg == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("vault not found"))
	}

	vaultCfg.Enabled = true
	if err := s.sysStore.PutVault(ctx, *vaultCfg); err != nil {
		return nil, errInternal(err)
	}
	s.notify(raftfsm.Notification{Kind: raftfsm.NotifyVaultPut, ID: id})

	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.ResumeVaultResponse{System: cfg}), nil
}

// protoToVaultConfig converts a proto VaultConfig to a system.VaultConfig.
// Delegates to convert.VaultConfigFromProto so the field mapping has one
// source of truth (shared with the FSM command path).
func protoToVaultConfig(p *apiv1.VaultConfig) (system.VaultConfig, error) {
	return convert.VaultConfigFromProto(p)
}

// CloudServiceTester validates connectivity for a cloud storage configuration.
type CloudServiceTester func(ctx context.Context, params map[string]string) (string, error)

// TestCloudService tests connectivity for a cloud storage configuration without saving it.
func (s *SystemServer) TestCloudService(
	ctx context.Context,
	req *connect.Request[apiv1.TestCloudServiceRequest],
) (*connect.Response[apiv1.TestCloudServiceResponse], error) {
	tester := s.cloudTesters[req.Msg.Type]
	if tester == nil {
		return connect.NewResponse(&apiv1.TestCloudServiceResponse{
			Success: false,
			Message: fmt.Sprintf("connection test not supported for cloud service type %q", req.Msg.Type),
		}), nil
	}

	msg, err := tester(ctx, req.Msg.Params)
	if err != nil {
		return connect.NewResponse(&apiv1.TestCloudServiceResponse{ //nolint:nilerr // test failure is reported in the response body, not as an RPC error
			Success: false,
			Message: err.Error(),
		}), nil
	}
	return connect.NewResponse(&apiv1.TestCloudServiceResponse{
		Success: true,
		Message: msg,
	}), nil
}
