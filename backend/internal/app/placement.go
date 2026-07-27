package app

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"strings"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

const (
	// placementReconcileJobName is the operator-visible name for the
	// periodic placement-reconcile fallback shown in the inspector's
	// Scheduled view. Keep stable across releases.
	placementReconcileJobName = "vault-placement-reconcile"

	// placementReconcileSchedule runs every 15 seconds. 6-field cron
	// (with-seconds).
	//
	// Every FSM-visible placement input now wakes the reconciler on its own
	// event (gastrolog-29xpy): vault put, placements set, node-storage-config
	// set, node lifecycle-state change, node-config add/remove, and ingester
	// put/delete all fire pm.Trigger() from the config dispatcher, and
	// leadership transitions run through pm.Run's Raft observer. What remains
	// with NO event is peer LIVENESS EXPIRY: LivePeers() is a freshness test
	// (time.Now vs last Raft contact, or vs last broadcast for peers we share
	// no Raft edge with), so a peer silently dropping out of the alive set is
	// detectable only by re-evaluating on a clock. This tick is
	// that re-evaluation — the honest, irreducible residual, not a catch-all
	// for missed events. (Sustained absence is separately promoted to a
	// NodeStateChanged FSM event by the unreachable sweep, which then triggers
	// a reconcile; this tick covers the pre-promotion window and follower/
	// singleton eligibility against the live-peer set.)
	placementReconcileSchedule = "*/15 * * * * *"

	// Alarm type IDs raised by the placement manager; the instance key is
	// the vault ID. The two unplaced conditions are separate alarm types
	// (split from the old vault-unplaced ID): a selected node missing the
	// required storage class and no eligible node at all have different
	// causes and different operator responses.
	softOfflineAlarmType         = "vault-soft-offline-leader"
	storageClassMissingAlarmType = "vault-storage-class-missing"
	noEligibleNodeAlarmType      = "vault-no-eligible-node"
	homeCannotStoreAlarmType     = "vault-home-cannot-store"
)

// clearUnplacedAlarms clears both unplaced-condition alarms for a vault —
// called when the vault has a valid, eligible leader placement.
func (pm *placementManager) clearUnplacedAlarms(v system.VaultConfig) {
	if pm.alerts == nil {
		return
	}
	pm.alerts.Clear(storageClassMissingAlarmType, v.ID.String())
	pm.alerts.Clear(noEligibleNodeAlarmType, v.ID.String())
}

// placementManager assigns vaults to nodes automatically.
// Runs on every node but only acts when this node is the Raft leader.
// Writes vault assignments via system.Store (Raft-replicated).
type placementManager struct {
	cfgStore    system.Store
	clusterSrv  *cluster.Server
	peerState   *cluster.PeerState
	factories   *orchestrator.Factories
	alerts      alert.Sink
	localNodeID string
	logger      *slog.Logger
	triggerCh   chan struct{} // poked to run reconcile immediately

	// localVaultStorageProtected reports whether THIS node's backing
	// storage for the vault is under disk protect. Peers' protect state
	// arrives via the NodeStats broadcast
	// (PeerState.VaultStorageProtectedNodes); the local node does not
	// appear in its own peer table, so the degraded-home alarm needs this
	// direct orchestrator lookup. Nil in tests that don't exercise the
	// local-degraded case. Renamed from localVaultDiskProtected
	// (gastrolog-9akebz).
	localVaultStorageProtected func(glid.GLID) bool
}

// Run blocks until ctx is cancelled. Handles the two event-driven
// reconcile sources — leadership transitions (via the Raft observer
// channel) and manual triggers (Trigger() / RPC handlers via
// triggerCh). The periodic-fallback cadence is NOT in this loop
// anymore; that piece lives in startPlacementReconcile so it shows
// up in the inspector's Scheduled view (gastrolog-1ia46).
func (pm *placementManager) Run(ctx context.Context) {
	leaderCh := make(chan hraft.Observation, 4)
	pm.clusterSrv.RegisterLeaderObserver(leaderCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-leaderCh:
			if pm.clusterSrv.IsLeader() {
				pm.reconcile(ctx)
			}
		case <-pm.triggerCh:
			if pm.clusterSrv.IsLeader() {
				pm.reconcile(ctx)
			}
		}
	}
}

// Trigger requests an immediate placement reconcile. Non-blocking — if a
// reconcile is already pending, the trigger is dropped.
func (pm *placementManager) Trigger() {
	select {
	case pm.triggerCh <- struct{}{}:
	default:
	}
}

// startPlacementReconcile registers the periodic placement reconcile with the
// supplied scheduler. The scheduled task just pokes pm.Trigger() — the actual
// reconcile work still runs in pm.Run's goroutine via triggerCh, preserving the
// existing serialization. Config-input changes reconcile event-driven from the
// dispatcher; this tick exists solely to re-evaluate peer-heartbeat liveness,
// which is TTL-based and has no event (see placementReconcileSchedule).
func startPlacementReconcile(_ context.Context, scheduler scheduledJobRegistry, pm *placementManager) error {
	task := func() { pm.Trigger() }
	if err := scheduler.AddJob(placementReconcileJobName, placementReconcileSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(placementReconcileJobName,
		"Vault placement reconcile — peer-liveness re-evaluation. Runs on every node every 15 seconds; the task pokes the placement manager's trigger channel, which only acts when this node is the Raft leader. Every FSM-visible placement input (vault/placements/node-storage/node-state/node-config/ingester changes, leadership transitions) reconciles event-driven; this tick covers the one input with no event — peer-heartbeat liveness expiry (LivePeers is a TTL test, detectable only by re-evaluating on a clock).")
	return nil
}

// Reconcile runs placement synchronously. Safe to call from RPC handlers
// (not from FSM callbacks — those would deadlock Raft).
func (pm *placementManager) Reconcile(ctx context.Context) {
	if pm.clusterSrv != nil && pm.clusterSrv.IsLeader() {
		pm.reconcile(ctx)
	}
}

// reconcile evaluates all vaults and active ingesters, assigning them to
// eligible alive nodes. Only writes when the assignment actually changes.
func (pm *placementManager) reconcile(ctx context.Context) {
	vaults, err := pm.cfgStore.ListVaults(ctx)
	if err != nil {
		pm.logger.Error("placement: list vaults", "error", err)
		return
	}
	nscs, err := pm.cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		pm.logger.Error("placement: list node storage configs", "error", err)
		return
	}

	// Build alive-node set: local node + live peers.
	alive := make(map[string]bool)
	alive[pm.localNodeID] = true
	livePeers := pm.peerState.LivePeers()
	for _, id := range livePeers {
		alive[id] = true
	}
	// If no peers have reported yet (cluster just started), treat all Raft
	// members as alive to avoid spurious reassignments during startup.
	peerStatePopulated := len(livePeers) > 0
	if !peerStatePopulated && pm.clusterSrv != nil {
		if servers, err := pm.clusterSrv.Servers(); err == nil {
			for _, srv := range servers {
				alive[srv.ID] = true
			}
		}
	}

	// Build node-state map from FSM authority. placeVault uses this to
	// gate leader rotation: soft-offline (Unreachable, Maintenance) and
	// in-transition (Draining, Decommissioning) states refuse rotation.
	// Only Live state permits the existing rotate-on-unreachable
	// behavior. See docs/node-lifecycle-design.md "Behavior gates by
	// state" and gastrolog-slc6l.
	nodeConfigs, err := pm.cfgStore.ListNodes(ctx)
	if err != nil {
		pm.logger.Error("placement: list nodes", "error", err)
		return
	}
	nodeStates := make(map[string]system.NodeState, len(nodeConfigs))
	for _, n := range nodeConfigs {
		nodeStates[n.ID.String()] = n.EffectiveState()
	}

	// Count current vault assignments per node (for load balancing).
	// Counts both leaders and followers.
	vaultCount := make(map[string]int)
	for _, v := range vaults {
		placements, _ := pm.cfgStore.GetVaultPlacements(ctx, v.ID)
		leaderNodeID := system.LeaderNodeID(placements, nscs)
		if leaderNodeID != "" && alive[leaderNodeID] {
			vaultCount[leaderNodeID]++
		}
		for _, sid := range system.FollowerNodeIDs(placements, nscs) {
			if alive[sid] {
				vaultCount[sid]++
			}
		}
	}

	nodeNames := make(map[string]string, len(nodeConfigs))
	for _, n := range nodeConfigs {
		id := n.ID.String()
		if n.Name != "" {
			nodeNames[id] = n.Name
		} else {
			nodeNames[id] = id
		}
	}

	for _, v := range vaults {
		pm.placeVault(ctx, v, alive, nodeStates, nscs, vaultCount)
		pm.reportDegradedHomes(ctx, v, alive, nscs, nodeNames)
	}

	pm.reconcileSingletonIngesters(ctx, alive)
}

// reportDegradedHomes raises (or clears) the vault-home-cannot-store alarm
// (gastrolog-38bm9t): a placement member whose local backing volume for
// this vault is under disk protect is a degraded holder — it can't take
// collection writes or build GLCBs. selectFollowers backfills a healthy
// eligible replica AUTOMATICALLY (the degraded member stops counting
// toward RF but is retained), so the alarm's job is visibility: name the
// degraded home and say whether the backfill restored RF storable members
// or the topology has no spare and admission is throttling at the source.
// Runs after placeVault, so it reports the post-backfill placement.
func (pm *placementManager) reportDegradedHomes(ctx context.Context, v system.VaultConfig, alive map[string]bool, nscs []system.NodeStorageConfig, nodeNames map[string]string) {
	if pm.alerts == nil {
		return
	}
	placements, _ := pm.cfgStore.GetVaultPlacements(ctx, v.ID)
	homes := make(map[string]bool)
	if leader := system.LeaderNodeID(placements, nscs); leader != "" {
		homes[leader] = true
	}
	for _, nid := range system.FollowerNodeIDs(placements, nscs) {
		if nid != "" {
			homes[nid] = true
		}
	}
	if len(homes) == 0 {
		pm.alerts.Clear(homeCannotStoreAlarmType, v.ID.String())
		return
	}

	protected := pm.vaultStorageProtectedSet(v.ID)
	var degraded []string
	healthy := 0
	for nid := range homes {
		if protected[nid] {
			degraded = append(degraded, nameOrID(nodeNames, nid))
		} else {
			healthy++
		}
	}
	if len(degraded) == 0 {
		pm.alerts.Clear(homeCannotStoreAlarmType, v.ID.String())
		return
	}
	sort.Strings(degraded)

	rf := int(v.ReplicationFactor)
	if rf <= 0 {
		rf = 1
	}
	remedy := fmt.Sprintf("healthy replicas backfilled automatically (%d storable members)", healthy)
	if healthy < rf {
		remedy = fmt.Sprintf("no eligible replacement node — %d of %d storable members; admission for this vault throttles at the source until space frees", healthy, rf)
	}
	pm.alerts.Raise(homeCannotStoreAlarmType, v.ID.String(),
		fmt.Sprintf("Vault %q: home %s cannot store (disk protect) — collection and builds are paused there; %s",
			v.Name, strings.Join(degraded, ", "), remedy))
}

func nameOrID(names map[string]string, id string) string {
	if n, ok := names[id]; ok && n != "" {
		return n
	}
	return id
}

// reconcileSingletonIngesters assigns each singleton ingester to exactly one
// alive node from its allowed set, preferring non-leader nodes. Parallel
// ingesters are skipped — they run on every selected node without central
// coordination. An ingester is singleton when both the registered type has
// SingletonSupported and the instance's Singleton flag is set.
func (pm *placementManager) reconcileSingletonIngesters(ctx context.Context, alive map[string]bool) {
	ingesters, err := pm.cfgStore.ListIngesters(ctx)
	if err != nil {
		pm.logger.Error("placement: list ingesters", "error", err)
		return
	}

	leaderID := ""
	if pm.clusterSrv != nil {
		_, leaderID = pm.clusterSrv.LeaderInfo()
	}

	for _, ing := range ingesters {
		if !ing.Enabled {
			continue
		}
		// Singleton with no eligibility expressed (no AllNodes, no NodeIDs)
		// is operator-error — skip silently.
		if !ing.AllNodes && len(ing.NodeIDs) == 0 {
			continue
		}
		if !pm.isSingletonIngester(ing) {
			continue
		}
		pm.placeSingletonIngester(ctx, ing, alive, leaderID)
	}
}

// eligibleNodes returns the set of node IDs an ingester is allowed to run on.
// AllNodes=true: every alive node in the cluster (membership-aware).
// AllNodes=false: NodeIDs intersected with alive nodes (literal pin).
func eligibleNodes(ing system.IngesterConfig, alive map[string]bool) []string {
	if ing.AllNodes {
		nodes := make([]string, 0, len(alive))
		for nodeID, isAlive := range alive {
			if isAlive {
				nodes = append(nodes, nodeID)
			}
		}
		return nodes
	}
	candidates := make([]string, 0, len(ing.NodeIDs))
	for _, nodeID := range ing.NodeIDs {
		if alive[nodeID] {
			candidates = append(candidates, nodeID)
		}
	}
	return candidates
}

// placeSingletonIngester assigns a single singleton ingester to one alive node.
func (pm *placementManager) placeSingletonIngester(ctx context.Context, ing system.IngesterConfig, alive map[string]bool, leaderID string) {
	current, _ := pm.cfgStore.GetIngesterAssignment(ctx, ing.ID)

	candidates := eligibleNodes(ing, alive)

	// Current assignment still valid? AllNodes accepts any current alive
	// node; NodeIDs requires current to be in the list.
	if current != "" && alive[current] && (ing.AllNodes || slices.Contains(ing.NodeIDs, current)) {
		return
	}

	if len(candidates) == 0 {
		pm.logger.Warn("placement: no alive node for active ingester", "id", ing.ID, "name", ing.Name)
		return
	}

	// Prefer non-leader.
	nonLeader := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c != leaderID {
			nonLeader = append(nonLeader, c)
		}
	}
	if len(nonLeader) > 0 {
		candidates = nonLeader
	}

	best := candidates[rand.Intn(len(candidates))] //nolint:gosec // G404: load balancing, not security
	if best == current {
		return
	}

	_ = pm.cfgStore.SetIngesterAssignment(ctx, ing.ID, best)
	pm.logger.Info("placement: assigned active ingester", "id", ing.ID, "name", ing.Name, "node", best, "prev", current)
}

// isSingletonIngester returns true when the ingester instance should be
// placed via Raft on a single node (failover mode). Requires both the type
// to declare SingletonSupported and the instance to have Singleton=true.
func (pm *placementManager) isSingletonIngester(ing system.IngesterConfig) bool {
	if !ing.Singleton {
		return false
	}
	if pm.factories == nil {
		return false
	}
	reg, ok := pm.factories.IngesterTypes[ing.Type]
	return ok && reg.SingletonSupported
}

// placeVault evaluates a single vault and assigns it to an eligible node if needed.
func (pm *placementManager) placeVault(ctx context.Context, v system.VaultConfig, alive map[string]bool, nodeStates map[string]system.NodeState, nscs []system.NodeStorageConfig, vaultCount map[string]int) {
	currentLeader := system.LeaderNodeID(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}(), nscs)

	// State-driven placement guard (gastrolog-slc6l). Rotation is
	// permitted only when the current leader's node is in the Live
	// state. Any other state refuses rotation:
	//   - Unreachable / Maintenance (soft-offline): retain placement,
	//     raise warning alert. Closes the RF=1 redeploy bug — placement
	//     can't move chunks off a transiently-absent node and orphan
	//     them on its disk.
	//   - Draining / Decommissioning (in-transition): retain placement;
	//     the explicit drain orchestrator / decommission flow is the
	//     authority for moving placements off these nodes, not this
	//     reconcile loop. No alert — these are deliberate operator
	//     states, not unexpected absences.
	// See docs/node-lifecycle-design.md "Behavior gates by state".
	if currentLeader != "" {
		switch nodeStates[currentLeader] {
		case system.NodeStateUnknown, system.NodeStateLive:
			// Live state (and legacy Unknown which maps to Live via
			// EffectiveState during nodeStates construction): fall
			// through to the eligibility check below — unless the
			// heartbeat says the node just went quiet.
			//
			// Two-clock inversion (gastrolog-2d35dc): heartbeat
			// liveness (peer TTL, ~8s) must never move a leader — only
			// the node lifecycle state machine may. A leader that is
			// state-Live but heartbeat-absent is in the pre-Unreachable
			// window (the unreachable sweep flips the state after its
			// 5-minute grace); before this guard, the alive[] check
			// below reassigned leadership ~26s into any blip — exactly
			// the transient absence slc6l's soft-offline gate promised
			// to protect, orphaning the returning node's chunks at
			// RF=1. Same treatment as Unreachable: retain placement,
			// alert, reconcile followers only.
			if !alive[currentLeader] {
				if pm.alerts != nil {
					pm.alerts.Raise(softOfflineAlarmType, v.ID.String(),
						fmt.Sprintf("Vault %q leader heartbeat lost on node %s (state still Live) — placement retained, rotation gated until the node lifecycle state changes",
							v.Name, currentLeader))
				}
				pm.placeFollowers(ctx, &v, alive, nscs, vaultCount)
				return
			}
		case system.NodeStateUnreachable, system.NodeStateMaintenance:
			if pm.alerts != nil {
				pm.alerts.Raise(softOfflineAlarmType, v.ID.String(),
					fmt.Sprintf("Vault %q leader on %s node %s — placement retained, rotation gated",
						v.Name, nodeStates[currentLeader], currentLeader))
			}
			pm.placeFollowers(ctx, &v, alive, nscs, vaultCount)
			return
		case system.NodeStateDraining, system.NodeStateDecommissioning:
			// Drain / decommission flow is the authority; reconcile is a
			// no-op for this vault while the node is in transition.
			pm.placeFollowers(ctx, &v, alive, nscs, vaultCount)
			return
		}
	}
	if pm.alerts != nil {
		pm.alerts.Clear(softOfflineAlarmType, v.ID.String())
	}

	// Current leader assignment still valid — check followers too. The
	// leader is guaranteed heartbeat-alive on this path (the two-clock
	// guard above returned otherwise), so only eligibility (storage
	// config) can invalidate it here.
	if currentLeader != "" && pm.nodeEligible(v, currentLeader, nscs) {
		pm.clearUnplacedAlarms(v)
		pm.placeFollowers(ctx, &v, alive, nscs, vaultCount)
		return
	}

	eligible := pm.eligibleNodes(v, alive, nscs)

	if len(eligible) == 0 {
		pm.handleUnplaceable(ctx, v, nscs, vaultCount)
		return
	}

	best := pm.selectNode(eligible, vaultCount)
	if best == currentLeader {
		return
	}

	old := currentLeader
	// Replace the leader placement. StorageIDForNode is strict: "" means the
	// selected node lost its matching storage class since the eligibility
	// check — refuse the placement loudly rather than land the leader on the
	// wrong disk class (gastrolog-2bv1x).
	storageID := system.StorageIDForNode(best, v, nscs)
	if storageID == "" {
		pm.logger.Error("placement: no storage of required class on selected node; refusing leader placement",
			"vault", v.ID, "name", v.Name, "node", best, "class", v.StorageClass)
		if pm.alerts != nil {
			pm.alerts.Clear(noEligibleNodeAlarmType, v.ID.String())
			pm.alerts.Raise(storageClassMissingAlarmType, v.ID.String(),
				fmt.Sprintf("Vault %q: selected node %s has no storage of class %d", v.Name, best, v.StorageClass))
		}
		return
	}
	oldP, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	newP := replaceLeaderPlacement(oldP, storageID)
	if err := pm.cfgStore.SetVaultPlacements(ctx, v.ID, newP); err != nil {
		pm.logger.Error("placement: assign vault", "vault", v.ID, "name", v.Name, "node", best, "error", err)
		return
	}

	if old != "" {
		vaultCount[old]--
	}
	vaultCount[best]++

	pm.clearUnplacedAlarms(v)

	if old == "" {
		pm.logger.Info("placement: vault assigned", "vault", v.ID, "name", v.Name, "node", best)
	} else {
		pm.logger.Info("placement: vault reassigned", "vault", v.ID, "name", v.Name, "from", old, "to", best)
	}

	// Place followers if replication is configured.
	pm.placeFollowers(ctx, &v, alive, nscs, vaultCount)
}

// replaceLeaderPlacement returns a new Placements slice with the leader set to storageID.
func replaceLeaderPlacement(placements []system.VaultPlacement, storageID string) []system.VaultPlacement {
	var result []system.VaultPlacement
	for _, p := range placements {
		if !p.Leader {
			result = append(result, p)
		}
	}
	return append([]system.VaultPlacement{{StorageID: storageID, Leader: true}}, result...)
}

// placeFollowers assigns follower file storages for a vault based on its ReplicationFactor.
// Prefers storages on different nodes (availability), falls back to different storages on
// the same node (redundancy). Never places two replicas on the same file storage.
func (pm *placementManager) placeFollowers(ctx context.Context, v *system.VaultConfig, alive map[string]bool, nscs []system.NodeStorageConfig, vaultCount map[string]int) {
	leaderStorageID := system.LeaderStorageID(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}())
	leaderNodeID := system.NodeIDForStorage(leaderStorageID, nscs)

	// Degraded-home backfill (gastrolog-38bm9t): members whose local
	// volume for this vault is under disk protect stop counting toward
	// RF; the healthy-follower target grows so an eligible node becomes
	// a replica automatically. A degraded LEADER raises the target even
	// for RF=1 vaults — the state guards retain its leadership, but the
	// vault still needs one member that can actually store.
	degraded := pm.vaultStorageProtectedSet(v.ID)
	rf := int(v.ReplicationFactor)
	if rf <= 0 {
		rf = 1 // unset RF means a single copy
	}
	target := rf - 1
	if degraded[leaderNodeID] {
		target++
	}
	if target <= 0 {
		pm.clearStaleFollowers(ctx, v, nscs, vaultCount)
		return
	}

	candidates := pm.followerCandidates(*v, leaderStorageID, leaderNodeID, alive, nscs, vaultCount)
	kept, healthy := pm.selectFollowers(v, target, leaderStorageID, leaderNodeID, degraded, candidates, nscs, alive, vaultCount)

	// Build new placements.
	newPlacements := []system.VaultPlacement{{StorageID: leaderStorageID, Leader: true}}
	newPlacements = append(newPlacements, kept...)

	if !placementsEqual(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}(), newPlacements) {
		if err := pm.cfgStore.SetVaultPlacements(ctx, v.ID, newPlacements); err != nil {
			pm.logger.Error("placement: assign followers", "vault", v.ID, "error", err)
			return
		}
		pm.logger.Info("placement: followers updated",
			"vault", v.ID, "name", v.Name, "placements", len(newPlacements))
	}

	pm.alertReplication(v, healthy, target)
}

// clearStaleFollowers removes leftover follower placements when RF <= 1.
func (pm *placementManager) clearStaleFollowers(ctx context.Context, v *system.VaultConfig, nscs []system.NodeStorageConfig, vaultCount map[string]int) {
	currentFollowers := system.FollowerStorageIDs(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}())
	if len(currentFollowers) == 0 {
		return
	}
	for _, sID := range currentFollowers {
		if nid := system.NodeIDForStorage(sID, nscs); nid != "" {
			vaultCount[nid]--
		}
	}
	cp, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	if err := pm.cfgStore.SetVaultPlacements(ctx, v.ID, clearFollowerPlacements(cp)); err != nil {
		pm.logger.Error("placement: clear stale followers", "vault", v.ID, "error", err)
	}
}

// followerCandidates returns eligible storages excluding the leader, sorted
// by preference: cross-node first (availability), then same-node (redundancy),
// then least-loaded.
func (pm *placementManager) followerCandidates(v system.VaultConfig, leaderStorageID, leaderNodeID string, alive map[string]bool, nscs []system.NodeStorageConfig, vaultCount map[string]int) []eligibleStorage {
	all := pm.eligibleStorages(v, alive, nscs)
	var candidates []eligibleStorage
	for _, ea := range all {
		if ea.storageID != leaderStorageID {
			candidates = append(candidates, ea)
		}
	}
	slices.SortFunc(candidates, func(a, b eligibleStorage) int {
		aRemote := a.nodeID != leaderNodeID
		bRemote := b.nodeID != leaderNodeID
		if aRemote != bRemote {
			if aRemote {
				return -1
			}
			return 1
		}
		return vaultCount[a.nodeID] - vaultCount[b.nodeID]
	})
	return candidates
}

// selectFollowers picks follower placements: retains existing valid ones first,
// then fills from sorted candidates.
// selectFollowers picks the follower placements: existing healthy members
// up to target (the cap trims surplus after recovery or an RF decrease),
// degraded members unconditionally retained, and healthy eligible
// candidates backfilled until target healthy followers exist. Returns the
// placements and the healthy-follower count.
//
// Degraded-home backfill (gastrolog-38bm9t): a placement member whose
// local volume for this vault is under disk protect cannot take
// collection writes or build GLCBs, so it stops COUNTING toward the
// replication factor — but it is never dropped here (its bytes may
// recover when space frees; removal is the operator's call). Healthy
// eligible candidates backfill automatically so the vault keeps RF
// storable members and the release gate keeps moving. When the degraded
// member recovers it counts again, and the healthy cap trims the
// placement back on a later pass.
func (pm *placementManager) selectFollowers(v *system.VaultConfig, target int, leaderStorageID, leaderNodeID string, degraded map[string]bool, candidates []eligibleStorage, nscs []system.NodeStorageConfig, alive map[string]bool, vaultCount map[string]int) ([]system.VaultPlacement, int) {
	var kept []system.VaultPlacement
	usedStorages := map[string]bool{leaderStorageID: true}
	usedNodes := map[string]bool{leaderNodeID: true} // 1:1:1: one store per vault per node

	healthy := 0
	// Keep existing valid follower placements: healthy ones up to the
	// target (the cap is what trims surplus after recovery or an RF
	// decrease), degraded ones unconditionally.
	current, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	for _, p := range current {
		if p.Leader {
			continue
		}
		nid := system.NodeIDForStorage(p.StorageID, nscs)
		if nid == "" || !alive[nid] || usedStorages[p.StorageID] || usedNodes[nid] || !pm.storageEligible(p.StorageID, *v, nscs) {
			continue
		}
		if degraded[nid] {
			kept = append(kept, p)
			usedStorages[p.StorageID] = true
			usedNodes[nid] = true
			continue
		}
		if healthy >= target {
			continue
		}
		kept = append(kept, p)
		usedStorages[p.StorageID] = true
		usedNodes[nid] = true
		healthy++
	}

	// Fill remaining from candidates, preferring cross-node. Degraded
	// nodes are not candidates — adding a member that cannot store
	// defeats the backfill.
	for _, ea := range candidates {
		if healthy >= target {
			break
		}
		if usedStorages[ea.storageID] || usedNodes[ea.nodeID] || degraded[ea.nodeID] {
			continue
		}
		kept = append(kept, system.VaultPlacement{StorageID: ea.storageID, Leader: false})
		usedStorages[ea.storageID] = true
		usedNodes[ea.nodeID] = true
		vaultCount[ea.nodeID]++
		healthy++
	}
	return kept, healthy
}

// vaultStorageProtectedSet returns the node IDs currently reporting a
// storage backing this vault under disk protect: live peers via the
// NodeStats broadcast, the local node via the orchestrator lookup (it is
// absent from its own peer table). Renamed from vaultDiskProtectedSet
// (gastrolog-9akebz).
func (pm *placementManager) vaultStorageProtectedSet(vaultID glid.GLID) map[string]bool {
	protected := make(map[string]bool)
	if pm.peerState != nil {
		for _, nid := range pm.peerState.VaultStorageProtectedNodes(vaultID) {
			protected[nid] = true
		}
	}
	if pm.localVaultStorageProtected != nil && pm.localVaultStorageProtected(vaultID) {
		protected[pm.localNodeID] = true
	}
	return protected
}

// alertReplication raises or clears the under-replicated vault alarm.
func (pm *placementManager) alertReplication(v *system.VaultConfig, placed, desired int) {
	if pm.alerts == nil {
		return
	}
	if placed < desired {
		pm.alerts.Raise("vault-underreplicated", v.ID.String(),
			fmt.Sprintf("Vault %q: only %d of %d desired replicas (insufficient eligible file storages)", v.Name, placed+1, int(v.ReplicationFactor)))
	} else {
		pm.alerts.Clear("vault-underreplicated", v.ID.String())
	}
}

type eligibleStorage struct {
	storageID string
	nodeID    string
}

// eligibleStorages returns all storages across all alive nodes that can host a replica.
// For memory vaults: one synthetic storage per alive node (no file storage needed).
// For file/cloud vaults: all file storages matching the required class.
func (pm *placementManager) eligibleStorages(v system.VaultConfig, alive map[string]bool, nscs []system.NodeStorageConfig) []eligibleStorage {
	var result []eligibleStorage

	if v.Type == system.VaultTypeMemory {
		for nodeID := range alive {
			result = append(result, eligibleStorage{
				storageID: system.SyntheticStorageID(nodeID),
				nodeID:    nodeID,
			})
		}
		return result
	}

	sc := v.StorageClass
	for _, nsc := range nscs {
		if !alive[nsc.NodeID] {
			continue
		}
		for _, fs := range nsc.FileStorages {
			if fs.StorageClass == sc {
				result = append(result, eligibleStorage{storageID: fs.ID.String(), nodeID: nsc.NodeID})
			}
		}
	}
	return result
}

// storageEligible checks if a specific storage still matches the vault's requirements.
func (pm *placementManager) storageEligible(storageID string, v system.VaultConfig, nscs []system.NodeStorageConfig) bool {
	if v.Type == system.VaultTypeMemory {
		return strings.HasPrefix(storageID, system.SyntheticStoragePrefix)
	}
	sc := v.StorageClass
	for _, nsc := range nscs {
		for _, fs := range nsc.FileStorages {
			if fs.ID.String() == storageID && fs.StorageClass == sc {
				return true
			}
		}
	}
	return false
}

// clearFollowerPlacements removes all non-leader placements.
func clearFollowerPlacements(placements []system.VaultPlacement) []system.VaultPlacement {
	var result []system.VaultPlacement
	for _, p := range placements {
		if p.Leader {
			result = append(result, p)
		}
	}
	return result
}

func placementsEqual(a, b []system.VaultPlacement) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].StorageID != b[i].StorageID || a[i].Leader != b[i].Leader {
			return false
		}
	}
	return true
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// handleUnplaceable clears a vault's assignment when no eligible node exists.
func (pm *placementManager) handleUnplaceable(_ context.Context, v system.VaultConfig, nscs []system.NodeStorageConfig, _ map[string]int) {
	// "Zero eligible nodes" is almost always transient — peer state
	// hasn't broadcast in yet, the FSM snapshot is still being replayed,
	// or the cluster just bootstrapped and NSCs haven't propagated to
	// the placement view. Wiping placements on every such tick would
	// destroy valid leader+follower assignments that already exist on
	// disk and never recover them once the cluster stabilizes — the
	// next tick sees no current leader, has to re-elect from scratch,
	// and loses any follower history. See gastrolog-2yeie: before
	// yield-leadership preStop preserved cluster state across pod
	// restart, demote-self was wiping state on every restart anyway so
	// the destructive branch here was a no-op. Now that state survives,
	// the wipe is visible.
	//
	// Keep current placements intact; raise the alert so the operator
	// sees the degraded condition; let the next reconcile tick promote
	// peers back into the alive set and extend followers naturally.
	currentLeader := system.LeaderNodeID(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}(), nscs)
	pm.logger.Warn("placement: vault has no currently-eligible node, retaining existing placements",
		"vault", v.ID, "name", v.Name, "current_leader", currentLeader)
	if pm.alerts != nil {
		pm.alerts.Clear(storageClassMissingAlarmType, v.ID.String())
		pm.alerts.Raise(noEligibleNodeAlarmType, v.ID.String(),
			fmt.Sprintf("Vault %q has no eligible node", v.Name))
	}
}

// nodeEligibleForVault reports whether a node can host a placement for
// a vault, given the cluster's storage configs and the vault's current
// placements. This is THE definition of placement eligibility: the
// placement manager asks it when choosing leaders and followers, and
// the RF-preservation removal gate (gastrolog-3vyex) asks the same
// question about re-placement candidates. Do not fork a second copy —
// a gate that disagrees with the placer is worse than no gate.
//
// Liveness and lifecycle state are the CALLER's filter (the placement
// manager passes its alive set; the removal gate passes nodes in
// NodeStateLive). This function answers only "can this node hold this
// vault's data".
func nodeEligibleForVault(v system.VaultConfig, nodeID string, nscs []system.NodeStorageConfig, placements []system.VaultPlacement) bool {
	switch v.Type {
	case system.VaultTypeMemory:
		return true // any node can serve memory vaults
	case system.VaultTypeFile:
		return nodeHasStorageClass(nscs, nodeID, v.StorageClass)
	case system.VaultTypeJSONL:
		// JSONL vaults have explicit node assignment via Path — only the
		// node already holding the leader placement is eligible.
		return system.LeaderNodeID(placements, nscs) == nodeID
	default:
		return false
	}
}

// nodeEligible checks whether a specific node can serve a vault.
func (pm *placementManager) nodeEligible(v system.VaultConfig, nodeID string, nscs []system.NodeStorageConfig) bool {
	placements, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	return nodeEligibleForVault(v, nodeID, nscs, placements)
}

// eligibleNodes returns all alive nodes that can serve a vault.
func (pm *placementManager) eligibleNodes(v system.VaultConfig, alive map[string]bool, nscs []system.NodeStorageConfig) []string {
	placements, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	var result []string
	for nodeID := range alive {
		if nodeEligibleForVault(v, nodeID, nscs, placements) {
			result = append(result, nodeID)
		}
	}
	return result
}

// selectNode picks the node with the fewest assigned vaults.
// Ties are broken randomly to spread vaults evenly across nodes.
func (pm *placementManager) selectNode(eligible []string, vaultCount map[string]int) string {
	// Find the minimum vault count.
	minCount := vaultCount[eligible[0]]
	for _, id := range eligible[1:] {
		if c := vaultCount[id]; c < minCount {
			minCount = c
		}
	}
	// Collect all candidates at the minimum count.
	var candidates []string
	for _, id := range eligible {
		if vaultCount[id] == minCount {
			candidates = append(candidates, id)
		}
	}
	return candidates[rand.Intn(len(candidates))] //nolint:gosec // G404: load balancing, not security
}

// nodeHasStorageClass checks if a node has a file storage with the given class.
func nodeHasStorageClass(nscs []system.NodeStorageConfig, nodeID string, storageClass uint32) bool {
	if storageClass == 0 {
		return false
	}
	idx := slices.IndexFunc(nscs, func(n system.NodeStorageConfig) bool { return n.NodeID == nodeID })
	if idx < 0 {
		return false
	}
	return slices.ContainsFunc(nscs[idx].FileStorages, func(a system.FileStorage) bool { return a.StorageClass == storageClass })
}
