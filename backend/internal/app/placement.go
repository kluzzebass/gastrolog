package app

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
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

// placementManager assigns tiers to nodes automatically.
// Runs on every node but only acts when this node is the Raft leader.
// Writes tier assignments via system.Store (Raft-replicated).
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

// reconcile evaluates all tiers and active ingesters, assigning them to
// eligible alive nodes. Only writes when the assignment actually changes.
func (pm *placementManager) reconcile(ctx context.Context) {
	tiers, err := pm.cfgStore.ListTiers(ctx)
	if err != nil {
		pm.logger.Error("placement: list tiers", "error", err)
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

	// Build set of tier IDs actually referenced by vaults (tiers with a VaultID).
	referencedTiers := make(map[string]bool)
	for _, t := range tiers {
		if t.VaultID != (glid.GLID{}) {
			referencedTiers[t.ID.String()] = true
		}
	}

	// Count current tier assignments per node (for load balancing).
	// Counts both leaders and followers.
	tierCount := make(map[string]int)
	for _, t := range tiers {
		leaderNodeID := system.LeaderNodeID(func() []system.TierPlacement {
			p, _ := pm.cfgStore.GetTierPlacements(context.Background(), t.ID)
			return p
		}(), nscs)
		if leaderNodeID != "" && alive[leaderNodeID] {
			tierCount[leaderNodeID]++
		}
		for _, sid := range system.FollowerNodeIDs(func() []system.TierPlacement {
			p, _ := pm.cfgStore.GetTierPlacements(context.Background(), t.ID)
			return p
		}(), nscs) {
			if alive[sid] {
				tierCount[sid]++
			}
		}
	}

	for _, tier := range tiers {
		if !referencedTiers[tier.ID.String()] {
			continue
		}
		pm.placeTier(ctx, tier, alive, nscs, tierCount)
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
		if !ing.Enabled || len(ing.NodeIDs) == 0 {
			continue
		}
		if !pm.isSingletonIngester(ing) {
			continue
		}
		pm.placeSingletonIngester(ctx, ing, alive, leaderID)
	}
}

// placeSingletonIngester assigns a single singleton ingester to one alive node.
func (pm *placementManager) placeSingletonIngester(ctx context.Context, ing system.IngesterConfig, alive map[string]bool, leaderID string) {
	current, _ := pm.cfgStore.GetIngesterAssignment(ctx, ing.ID)

	// Current assignment still valid?
	if current != "" && alive[current] && slices.Contains(ing.NodeIDs, current) {
		return
	}

	// Pick an eligible alive node, preferring non-leader.
	var candidates []string
	for _, nodeID := range ing.NodeIDs {
		if alive[nodeID] {
			candidates = append(candidates, nodeID)
		}
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

// placeTier evaluates a single tier and assigns it to an eligible node if needed.
func (pm *placementManager) placeTier(ctx context.Context, tier system.TierConfig, alive map[string]bool, nscs []system.NodeStorageConfig, tierCount map[string]int) {
	alertKey := fmt.Sprintf("tier-unplaced:%s", tier.ID)

	currentLeader := system.LeaderNodeID(func() []system.TierPlacement {
		p, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
		return p
	}(), nscs)

	// Current leader assignment still valid — check followers too.
	if currentLeader != "" && alive[currentLeader] && pm.nodeEligible(tier, currentLeader, nscs) {
		if pm.alerts != nil {
			pm.alerts.Clear(alertKey)
		}
		pm.placeFollowers(ctx, &tier, alive, nscs, tierCount)
		return
	}

	eligible := pm.eligibleNodes(tier, alive, nscs)

	if len(eligible) == 0 {
		pm.handleUnplaceable(ctx, tier, alertKey, nscs, tierCount)
		return
	}

	best := pm.selectNode(eligible, tierCount)
	if best == currentLeader {
		return
	}

	old := currentLeader
	// Replace the leader placement.
	oldP, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
	newP := replaceLeaderPlacement(oldP, system.StorageIDForNode(best, tier, nscs))
	_ = pm.cfgStore.SetTierPlacements(ctx, tier.ID, newP)
	if err := pm.cfgStore.PutTier(ctx, tier); err != nil {
		pm.logger.Error("placement: assign tier", "tier", tier.ID, "name", tier.Name, "node", best, "error", err)
		return
	}

	if old != "" {
		tierCount[old]--
	}
	tierCount[best]++

	if pm.alerts != nil {
		pm.alerts.Clear(alertKey)
	}

	if old == "" {
		pm.logger.Info("placement: tier assigned", "tier", tier.ID, "name", tier.Name, "node", best)
	} else {
		pm.logger.Info("placement: tier reassigned", "tier", tier.ID, "name", tier.Name, "from", old, "to", best)
	}

	// Place followers if replication is configured.
	pm.placeFollowers(ctx, &tier, alive, nscs, tierCount)
}

// replaceLeaderPlacement returns a new Placements slice with the leader set to storageID.
func replaceLeaderPlacement(placements []system.TierPlacement, storageID string) []system.TierPlacement {
	var result []system.TierPlacement
	for _, p := range placements {
		if !p.Leader {
			result = append(result, p)
		}
	}
	return append([]system.TierPlacement{{StorageID: storageID, Leader: true}}, result...)
}

// placeFollowers assigns follower file storages for a tier based on its ReplicationFactor.
// Prefers storages on different nodes (availability), falls back to different storages on
// the same node (redundancy). Never places two replicas on the same file storage.
func (pm *placementManager) placeFollowers(ctx context.Context, tier *system.TierConfig, alive map[string]bool, nscs []system.NodeStorageConfig, tierCount map[string]int) {
	desired := int(tier.ReplicationFactor) - 1
	if desired <= 0 {
		pm.clearStaleFollowers(ctx, tier, nscs, tierCount)
		return
	}

	leaderStorageID := system.LeaderStorageID(func() []system.TierPlacement {
		p, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
		return p
	}())
	leaderNodeID := system.NodeIDForStorage(leaderStorageID, nscs)
	candidates := pm.followerCandidates(*tier, leaderStorageID, leaderNodeID, alive, nscs, tierCount)
	kept := pm.selectFollowers(tier, desired, leaderStorageID, leaderNodeID, candidates, nscs, alive, tierCount)

	// Build new placements.
	newPlacements := []system.TierPlacement{{StorageID: leaderStorageID, Leader: true}}
	newPlacements = append(newPlacements, kept...)

	if !placementsEqual(func() []system.TierPlacement {
		p, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
		return p
	}(), newPlacements) {
		_ = pm.cfgStore.SetTierPlacements(ctx, tier.ID, newPlacements)
		if err := pm.cfgStore.PutTier(ctx, *tier); err != nil {
			pm.logger.Error("placement: assign followers", "tier", tier.ID, "error", err)
			return
		}
		pm.logger.Info("placement: followers updated",
			"tier", tier.ID, "name", tier.Name, "placements", len(newPlacements))
	}

	pm.alertReplication(tier, len(kept), desired)
}

// clearStaleFollowers removes leftover follower placements when RF <= 1.
func (pm *placementManager) clearStaleFollowers(ctx context.Context, tier *system.TierConfig, nscs []system.NodeStorageConfig, tierCount map[string]int) {
	currentFollowers := system.FollowerStorageIDs(func() []system.TierPlacement {
		p, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
		return p
	}())
	if len(currentFollowers) == 0 {
		return
	}
	for _, sID := range currentFollowers {
		if nid := system.NodeIDForStorage(sID, nscs); nid != "" {
			tierCount[nid]--
		}
	}
	cp, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
	_ = pm.cfgStore.SetTierPlacements(ctx, tier.ID, clearFollowerPlacements(cp))
	if err := pm.cfgStore.PutTier(ctx, *tier); err != nil {
		pm.logger.Error("placement: clear stale followers", "tier", tier.ID, "error", err)
	}
}

// followerCandidates returns eligible storages excluding the leader, sorted
// by preference: cross-node first (availability), then same-node (redundancy),
// then least-loaded.
func (pm *placementManager) followerCandidates(tier system.TierConfig, leaderStorageID, leaderNodeID string, alive map[string]bool, nscs []system.NodeStorageConfig, tierCount map[string]int) []eligibleStorage {
	all := pm.eligibleStorages(tier, alive, nscs)
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
		return tierCount[a.nodeID] - tierCount[b.nodeID]
	})
	return candidates
}

// selectFollowers picks follower placements: retains existing valid ones first,
// then fills from sorted candidates.
func (pm *placementManager) selectFollowers(tier *system.TierConfig, desired int, leaderStorageID, leaderNodeID string, candidates []eligibleStorage, nscs []system.NodeStorageConfig, alive map[string]bool, tierCount map[string]int) []system.TierPlacement {
	var kept []system.TierPlacement
	usedStorages := map[string]bool{leaderStorageID: true}
	usedNodes := map[string]bool{leaderNodeID: true} // 1:1:1: one store per tier per node

	// Keep existing valid follower placements.
	tierPs, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
	for _, p := range tierPs {
		if p.Leader || len(kept) >= desired {
			continue
		}
		nid := system.NodeIDForStorage(p.StorageID, nscs)
		if nid != "" && alive[nid] && !usedStorages[p.StorageID] && !usedNodes[nid] && pm.storageEligible(p.StorageID, *tier, nscs) {
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
		kept = append(kept, system.TierPlacement{StorageID: ea.storageID, Leader: false})
		usedStorages[ea.storageID] = true
		usedNodes[ea.nodeID] = true
		tierCount[ea.nodeID]++
	}
	return kept
}

// alertReplication sets or clears the under-replicated tier alert.
func (pm *placementManager) alertReplication(tier *system.TierConfig, placed, desired int) {
	if pm.alerts == nil {
		return
	}
	alertKey := fmt.Sprintf("tier-underreplicated:%s", tier.ID)
	if placed < desired {
		pm.alerts.Set(alertKey, alert.Warning, "placement",
			fmt.Sprintf("Tier %q: only %d of %d desired replicas (insufficient eligible file storages)", tier.Name, placed+1, int(tier.ReplicationFactor)))
	} else {
		pm.alerts.Clear(alertKey)
	}
}

type eligibleStorage struct {
	storageID string
	nodeID    string
}

// eligibleStorages returns all storages across all alive nodes that can host a replica.
// For memory tiers: one synthetic storage per alive node (no file storage needed).
// For file/cloud tiers: all file storages matching the required class.
func (pm *placementManager) eligibleStorages(tier system.TierConfig, alive map[string]bool, nscs []system.NodeStorageConfig) []eligibleStorage {
	var result []eligibleStorage

	if tier.Type == system.VaultTypeMemory {
		for nodeID := range alive {
			result = append(result, eligibleStorage{
				storageID: system.SyntheticStorageID(nodeID),
				nodeID:    nodeID,
			})
		}
		return result
	}

	sc := tier.StorageClass
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

// storageEligible checks if a specific storage still matches the tier's requirements.
func (pm *placementManager) storageEligible(storageID string, tier system.TierConfig, nscs []system.NodeStorageConfig) bool {
	if tier.Type == system.VaultTypeMemory {
		return strings.HasPrefix(storageID, system.SyntheticStoragePrefix)
	}
	sc := tier.StorageClass
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
func clearFollowerPlacements(placements []system.TierPlacement) []system.TierPlacement {
	var result []system.TierPlacement
	for _, p := range placements {
		if p.Leader {
			result = append(result, p)
		}
	}
	return result
}

func placementsEqual(a, b []system.TierPlacement) bool {
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

// handleUnplaceable clears a tier's assignment when no eligible node exists.
func (pm *placementManager) handleUnplaceable(ctx context.Context, tier system.TierConfig, alertKey string, nscs []system.NodeStorageConfig, tierCount map[string]int) {
	currentLeader := system.LeaderNodeID(func() []system.TierPlacement {
		p, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
		return p
	}(), nscs)
	if currentLeader != "" {
		old := currentLeader
		_ = pm.cfgStore.SetTierPlacements(ctx, tier.ID, nil)
		if err := pm.cfgStore.PutTier(ctx, tier); err != nil {
			pm.logger.Error("placement: clear tier assignment", "tier", tier.ID, "name", tier.Name, "error", err)
		} else {
			pm.logger.Warn("placement: tier unplaced, no eligible nodes", "tier", tier.ID, "name", tier.Name)
		}
		tierCount[old]--
	}
	if pm.alerts != nil {
		pm.alerts.Set(alertKey, alert.Warning, "placement",
			fmt.Sprintf("Tier %q has no eligible node", tier.Name))
	}
}

// nodeEligible checks whether a specific node can serve a tier.
func (pm *placementManager) nodeEligible(tier system.TierConfig, nodeID string, nscs []system.NodeStorageConfig) bool {
	switch tier.Type {
	case system.VaultTypeMemory:
		return true // any node can serve memory tiers
	case system.VaultTypeFile:
		return nodeHasStorageClass(nscs, nodeID, tier.StorageClass)
	case system.VaultTypeJSONL:
		// JSONL tiers have explicit node assignment via Path.
		leaderNodeID := system.LeaderNodeID(func() []system.TierPlacement {
			p, _ := pm.cfgStore.GetTierPlacements(context.Background(), tier.ID)
			return p
		}(), nscs)
		return leaderNodeID == nodeID
	default:
		return false
	}
}

// eligibleNodes returns all alive nodes that can serve a tier.
func (pm *placementManager) eligibleNodes(tier system.TierConfig, alive map[string]bool, nscs []system.NodeStorageConfig) []string {
	var result []string
	for nodeID := range alive {
		if pm.nodeEligible(tier, nodeID, nscs) {
			result = append(result, nodeID)
		}
	}
	return result
}

// selectNode picks the node with the fewest assigned tiers.
// Ties are broken randomly to spread tiers evenly across nodes.
func (pm *placementManager) selectNode(eligible []string, tierCount map[string]int) string {
	// Find the minimum tier count.
	minCount := tierCount[eligible[0]]
	for _, id := range eligible[1:] {
		if c := tierCount[id]; c < minCount {
			minCount = c
		}
	}
	// Collect all candidates at the minimum count.
	var candidates []string
	for _, id := range eligible {
		if tierCount[id] == minCount {
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
