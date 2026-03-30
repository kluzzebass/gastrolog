package app

import (
	"strings"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/orchestrator"

	hraft "github.com/hashicorp/raft"
)

const placementInterval = 15 * time.Second

// placementManager assigns tiers to nodes automatically.
// Runs on every node but only acts when this node is the Raft leader.
// Writes tier assignments via config.Store (Raft-replicated).
type placementManager struct {
	cfgStore    config.Store
	clusterSrv  *cluster.Server
	peerState   *cluster.PeerState
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

// reconcile evaluates all tiers and assigns them to eligible, alive nodes.
// Only writes PutTier when the assignment actually changes.
func (pm *placementManager) reconcile(ctx context.Context) {
	tiers, err := pm.cfgStore.ListTiers(ctx)
	if err != nil {
		pm.logger.Error("placement: list tiers", "error", err)
		return
	}
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

	// Build set of tier IDs actually referenced by vaults.
	referencedTiers := make(map[string]bool)
	for _, v := range vaults {
		for _, tid := range v.TierIDs {
			referencedTiers[tid.String()] = true
		}
	}

	// Count current tier assignments per node (for load balancing).
	// Counts both primaries and secondaries.
	tierCount := make(map[string]int)
	for _, t := range tiers {
		primaryNodeID := t.PrimaryNodeID(nscs)
		if primaryNodeID != "" && alive[primaryNodeID] {
			tierCount[primaryNodeID]++
		}
		for _, sid := range t.SecondaryNodeIDs(nscs) {
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
}

// placeTier evaluates a single tier and assigns it to an eligible node if needed.
func (pm *placementManager) placeTier(ctx context.Context, tier config.TierConfig, alive map[string]bool, nscs []config.NodeStorageConfig, tierCount map[string]int) {
	alertKey := fmt.Sprintf("tier-unplaced:%s", tier.ID)

	currentPrimary := tier.PrimaryNodeID(nscs)

	// Current primary assignment still valid — check secondaries too.
	if currentPrimary != "" && alive[currentPrimary] && pm.nodeEligible(tier, currentPrimary, nscs) {
		if pm.alerts != nil {
			pm.alerts.Clear(alertKey)
		}
		pm.placeSecondaries(ctx, &tier, alive, nscs, tierCount)
		return
	}

	eligible := pm.eligibleNodes(tier, alive, nscs)

	if len(eligible) == 0 {
		pm.handleUnplaceable(ctx, tier, alertKey, nscs, tierCount)
		return
	}

	best := pm.selectNode(eligible, tierCount)
	if best == currentPrimary {
		return
	}

	old := currentPrimary
	// Replace the primary placement.
	tier.Placements = replacePrimaryPlacement(tier.Placements, config.StorageIDForNode(best, tier, nscs))
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

	// Place secondaries if replication is configured.
	pm.placeSecondaries(ctx, &tier, alive, nscs, tierCount)
}

// replacePrimaryPlacement returns a new Placements slice with the primary set to storageID.
func replacePrimaryPlacement(placements []config.TierPlacement, storageID string) []config.TierPlacement {
	var result []config.TierPlacement
	for _, p := range placements {
		if !p.Primary {
			result = append(result, p)
		}
	}
	return append([]config.TierPlacement{{StorageID: storageID, Primary: true}}, result...)
}

// placeSecondaries assigns secondary file storages for a tier based on its ReplicationFactor.
// Prefers storages on different nodes (availability), falls back to different storages on
// the same node (redundancy). Never places two replicas on the same file storage.
func (pm *placementManager) placeSecondaries(ctx context.Context, tier *config.TierConfig, alive map[string]bool, nscs []config.NodeStorageConfig, tierCount map[string]int) {
	desired := int(tier.ReplicationFactor) - 1
	if desired <= 0 {
		pm.clearStaleSecondaries(ctx, tier, nscs, tierCount)
		return
	}

	primaryStorageID := tier.PrimaryStorageID()
	primaryNodeID := config.NodeIDForStorage(primaryStorageID, nscs)
	candidates := pm.secondaryCandidates(*tier, primaryStorageID, primaryNodeID, alive, nscs, tierCount)
	kept := pm.selectSecondaries(tier, desired, primaryStorageID, primaryNodeID, candidates, nscs, alive, tierCount)

	// Build new placements.
	newPlacements := []config.TierPlacement{{StorageID: primaryStorageID, Primary: true}}
	newPlacements = append(newPlacements, kept...)

	if !placementsEqual(tier.Placements, newPlacements) {
		tier.Placements = newPlacements
		if err := pm.cfgStore.PutTier(ctx, *tier); err != nil {
			pm.logger.Error("placement: assign secondaries", "tier", tier.ID, "error", err)
			return
		}
		pm.logger.Info("placement: secondaries updated",
			"tier", tier.ID, "name", tier.Name, "placements", len(newPlacements))
	}

	pm.alertReplication(tier, len(kept), desired)
}

// clearStaleSecondaries removes leftover secondary placements when RF <= 1.
func (pm *placementManager) clearStaleSecondaries(ctx context.Context, tier *config.TierConfig, nscs []config.NodeStorageConfig, tierCount map[string]int) {
	currentSecondaries := tier.SecondaryStorageIDs()
	if len(currentSecondaries) == 0 {
		return
	}
	for _, sID := range currentSecondaries {
		if nid := config.NodeIDForStorage(sID, nscs); nid != "" {
			tierCount[nid]--
		}
	}
	tier.Placements = clearSecondaryPlacements(tier.Placements)
	if err := pm.cfgStore.PutTier(ctx, *tier); err != nil {
		pm.logger.Error("placement: clear stale secondaries", "tier", tier.ID, "error", err)
	}
}

// secondaryCandidates returns eligible storages excluding the primary, sorted
// by preference: cross-node first (availability), then same-node (redundancy),
// then least-loaded.
func (pm *placementManager) secondaryCandidates(tier config.TierConfig, primaryStorageID, primaryNodeID string, alive map[string]bool, nscs []config.NodeStorageConfig, tierCount map[string]int) []eligibleStorage {
	all := pm.eligibleStorages(tier, alive, nscs)
	var candidates []eligibleStorage
	for _, ea := range all {
		if ea.storageID != primaryStorageID {
			candidates = append(candidates, ea)
		}
	}
	slices.SortFunc(candidates, func(a, b eligibleStorage) int {
		aRemote := a.nodeID != primaryNodeID
		bRemote := b.nodeID != primaryNodeID
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

// selectSecondaries picks secondary placements: retains existing valid ones first,
// then fills from sorted candidates.
func (pm *placementManager) selectSecondaries(tier *config.TierConfig, desired int, primaryStorageID, primaryNodeID string, candidates []eligibleStorage, nscs []config.NodeStorageConfig, alive map[string]bool, tierCount map[string]int) []config.TierPlacement {
	var kept []config.TierPlacement
	usedStorages := map[string]bool{primaryStorageID: true}
	usedNodes := map[string]bool{primaryNodeID: true} // 1:1:1: one store per tier per node

	// Keep existing valid secondary placements.
	for _, p := range tier.Placements {
		if p.Primary || len(kept) >= desired {
			continue
		}
		nid := config.NodeIDForStorage(p.StorageID, nscs)
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
		kept = append(kept, config.TierPlacement{StorageID: ea.storageID, Primary: false})
		usedStorages[ea.storageID] = true
		usedNodes[ea.nodeID] = true
		tierCount[ea.nodeID]++
	}
	return kept
}

// alertReplication sets or clears the under-replicated tier alert.
func (pm *placementManager) alertReplication(tier *config.TierConfig, placed, desired int) {
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
	nodeID string
}

// eligibleStorages returns all storages across all alive nodes that can host a replica.
// For memory tiers: one synthetic storage per alive node (no file storage needed).
// For file/cloud tiers: all file storages matching the required class.
func (pm *placementManager) eligibleStorages(tier config.TierConfig, alive map[string]bool, nscs []config.NodeStorageConfig) []eligibleStorage {
	var result []eligibleStorage

	if tier.Type == config.TierTypeMemory {
		for nodeID := range alive {
			result = append(result, eligibleStorage{
				storageID: config.SyntheticStorageID(nodeID),
				nodeID:    nodeID,
			})
		}
		return result
	}

	sc := tier.StorageClass
	if tier.Type == config.TierTypeCloud {
		sc = tier.ActiveChunkClass
	}
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
func (pm *placementManager) storageEligible(storageID string, tier config.TierConfig, nscs []config.NodeStorageConfig) bool {
	if tier.Type == config.TierTypeMemory {
		return strings.HasPrefix(storageID, config.SyntheticStoragePrefix)
	}
	sc := tier.StorageClass
	if tier.Type == config.TierTypeCloud {
		sc = tier.ActiveChunkClass
	}
	for _, nsc := range nscs {
		for _, fs := range nsc.FileStorages {
			if fs.ID.String() == storageID && fs.StorageClass == sc {
				return true
			}
		}
	}
	return false
}

// clearSecondaryPlacements removes all non-primary placements.
func clearSecondaryPlacements(placements []config.TierPlacement) []config.TierPlacement {
	var result []config.TierPlacement
	for _, p := range placements {
		if p.Primary {
			result = append(result, p)
		}
	}
	return result
}

func placementsEqual(a, b []config.TierPlacement) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].StorageID != b[i].StorageID || a[i].Primary != b[i].Primary {
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
func (pm *placementManager) handleUnplaceable(ctx context.Context, tier config.TierConfig, alertKey string, nscs []config.NodeStorageConfig, tierCount map[string]int) {
	currentPrimary := tier.PrimaryNodeID(nscs)
	if currentPrimary != "" {
		old := currentPrimary
		tier.Placements = nil
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
func (pm *placementManager) nodeEligible(tier config.TierConfig, nodeID string, nscs []config.NodeStorageConfig) bool {
	switch tier.Type {
	case config.TierTypeMemory:
		return true // any node can serve memory tiers
	case config.TierTypeFile:
		return nodeHasStorageClass(nscs, nodeID, tier.StorageClass)
	case config.TierTypeCloud:
		return nodeHasStorageClass(nscs, nodeID, tier.ActiveChunkClass)
	case config.TierTypeJSONL:
		// JSONL tiers have explicit node assignment via Path.
		primaryNodeID := tier.PrimaryNodeID(nscs)
		return primaryNodeID == nodeID
	default:
		return false
	}
}

// eligibleNodes returns all alive nodes that can serve a tier.
func (pm *placementManager) eligibleNodes(tier config.TierConfig, alive map[string]bool, nscs []config.NodeStorageConfig) []string {
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
func nodeHasStorageClass(nscs []config.NodeStorageConfig, nodeID string, storageClass uint32) bool {
	if storageClass == 0 {
		return false
	}
	idx := slices.IndexFunc(nscs, func(n config.NodeStorageConfig) bool { return n.NodeID == nodeID })
	if idx < 0 {
		return false
	}
	return slices.ContainsFunc(nscs[idx].FileStorages, func(a config.FileStorage) bool { return a.StorageClass == storageClass })
}
