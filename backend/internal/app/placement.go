package app

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

const placementInterval = 15 * time.Second

// placementManager assigns vaults to nodes automatically.
// Runs on every node but only acts when this node is the Raft leader.
// Writes vault assignments via system.Store (Raft-replicated).
type placementManager struct {
	cfgStore    system.Store
	clusterSrv  *cluster.Server
	peerState   *cluster.PeerState
	factories   *orchestrator.Factories
	alerts      orchestrator.AlertCollector
	localNodeID string
	logger      *slog.Logger
	triggerCh   chan struct{} // poked to run reconcile immediately
}

// Run blocks until ctx is cancelled. When this node is leader, it runs
// reconcile periodically and on leadership transitions.
func (pm *placementManager) Run(ctx context.Context) {
	leaderCh := make(chan hraft.Observation, 4)
	pm.clusterSrv.RegisterLeaderObserver(leaderCh)

	ticker := time.NewTicker(placementInterval)
	defer ticker.Stop()

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
		case <-ticker.C:
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

	for _, v := range vaults {
		pm.placeVault(ctx, v, alive, nscs, vaultCount)
	}

	pm.reconcileSingletonIngesters(ctx, alive)
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
func (pm *placementManager) placeVault(ctx context.Context, v system.VaultConfig, alive map[string]bool, nscs []system.NodeStorageConfig, vaultCount map[string]int) {
	alertKey := fmt.Sprintf("vault-unplaced:%s", v.ID)

	currentLeader := system.LeaderNodeID(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}(), nscs)

	// Current leader assignment still valid — check followers too.
	if currentLeader != "" && alive[currentLeader] && pm.nodeEligible(v, currentLeader, nscs) {
		if pm.alerts != nil {
			pm.alerts.Clear(alertKey)
		}
		pm.placeFollowers(ctx, &v, alive, nscs, vaultCount)
		return
	}

	eligible := pm.eligibleNodes(v, alive, nscs)

	if len(eligible) == 0 {
		pm.handleUnplaceable(ctx, v, alertKey, nscs, vaultCount)
		return
	}

	best := pm.selectNode(eligible, vaultCount)
	if best == currentLeader {
		return
	}

	old := currentLeader
	// Replace the leader placement.
	oldP, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	newP := replaceLeaderPlacement(oldP, system.StorageIDForNode(best, v, nscs))
	if err := pm.cfgStore.SetVaultPlacements(ctx, v.ID, newP); err != nil {
		pm.logger.Error("placement: assign vault", "vault", v.ID, "name", v.Name, "node", best, "error", err)
		return
	}

	if old != "" {
		vaultCount[old]--
	}
	vaultCount[best]++

	if pm.alerts != nil {
		pm.alerts.Clear(alertKey)
	}

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
	desired := int(v.ReplicationFactor) - 1
	if desired <= 0 {
		pm.clearStaleFollowers(ctx, v, nscs, vaultCount)
		return
	}

	leaderStorageID := system.LeaderStorageID(func() []system.VaultPlacement {
		p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
		return p
	}())
	leaderNodeID := system.NodeIDForStorage(leaderStorageID, nscs)
	candidates := pm.followerCandidates(*v, leaderStorageID, leaderNodeID, alive, nscs, vaultCount)
	kept := pm.selectFollowers(v, desired, leaderStorageID, leaderNodeID, candidates, nscs, alive, vaultCount)

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

	pm.alertReplication(v, len(kept), desired)
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
func (pm *placementManager) selectFollowers(v *system.VaultConfig, desired int, leaderStorageID, leaderNodeID string, candidates []eligibleStorage, nscs []system.NodeStorageConfig, alive map[string]bool, vaultCount map[string]int) []system.VaultPlacement {
	var kept []system.VaultPlacement
	usedStorages := map[string]bool{leaderStorageID: true}
	usedNodes := map[string]bool{leaderNodeID: true} // 1:1:1: one store per vault per node

	// Keep existing valid follower placements.
	current, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
	for _, p := range current {
		if p.Leader || len(kept) >= desired {
			continue
		}
		nid := system.NodeIDForStorage(p.StorageID, nscs)
		if nid != "" && alive[nid] && !usedStorages[p.StorageID] && !usedNodes[nid] && pm.storageEligible(p.StorageID, *v, nscs) {
			kept = append(kept, p)
			usedStorages[p.StorageID] = true
			usedNodes[nid] = true
		}
	}

	// Fill remaining from candidates, preferring cross-node.
	for _, ea := range candidates {
		if len(kept) >= desired {
			break
		}
		if usedStorages[ea.storageID] || usedNodes[ea.nodeID] {
			continue
		}
		kept = append(kept, system.VaultPlacement{StorageID: ea.storageID, Leader: false})
		usedStorages[ea.storageID] = true
		usedNodes[ea.nodeID] = true
		vaultCount[ea.nodeID]++
	}
	return kept
}

// alertReplication sets or clears the under-replicated vault alert.
func (pm *placementManager) alertReplication(v *system.VaultConfig, placed, desired int) {
	if pm.alerts == nil {
		return
	}
	alertKey := fmt.Sprintf("vault-underreplicated:%s", v.ID)
	if placed < desired {
		pm.alerts.Set(alertKey, alert.Warning, "placement",
			fmt.Sprintf("Vault %q: only %d of %d desired replicas (insufficient eligible file storages)", v.Name, placed+1, int(v.ReplicationFactor)))
	} else {
		pm.alerts.Clear(alertKey)
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
func (pm *placementManager) handleUnplaceable(_ context.Context, v system.VaultConfig, alertKey string, nscs []system.NodeStorageConfig, _ map[string]int) {
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
		pm.alerts.Set(alertKey, alert.Warning, "placement",
			fmt.Sprintf("Vault %q has no eligible node", v.Name))
	}
}

// nodeEligible checks whether a specific node can serve a vault.
func (pm *placementManager) nodeEligible(v system.VaultConfig, nodeID string, nscs []system.NodeStorageConfig) bool {
	switch v.Type {
	case system.VaultTypeMemory:
		return true // any node can serve memory vaults
	case system.VaultTypeFile:
		return nodeHasStorageClass(nscs, nodeID, v.StorageClass)
	case system.VaultTypeJSONL:
		// JSONL vaults have explicit node assignment via Path.
		leaderNodeID := system.LeaderNodeID(func() []system.VaultPlacement {
			p, _ := pm.cfgStore.GetVaultPlacements(context.Background(), v.ID)
			return p
		}(), nscs)
		return leaderNodeID == nodeID
	default:
		return false
	}
}

// eligibleNodes returns all alive nodes that can serve a vault.
func (pm *placementManager) eligibleNodes(v system.VaultConfig, alive map[string]bool, nscs []system.NodeStorageConfig) []string {
	var result []string
	for nodeID := range alive {
		if pm.nodeEligible(v, nodeID, nscs) {
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
