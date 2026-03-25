package app

import (
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
		case <-ticker.C:
			if pm.clusterSrv.IsLeader() {
				pm.reconcile(ctx)
			}
		}
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
	tierCount := make(map[string]int)
	for _, t := range tiers {
		if t.NodeID != "" && alive[t.NodeID] {
			tierCount[t.NodeID]++
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

	// Current assignment still valid — nothing to do.
	if tier.NodeID != "" && alive[tier.NodeID] && pm.nodeEligible(tier, tier.NodeID, nscs) {
		if pm.alerts != nil {
			pm.alerts.Clear(alertKey)
		}
		return
	}

	eligible := pm.eligibleNodes(tier, alive, nscs)

	if len(eligible) == 0 {
		pm.handleUnplaceable(ctx, tier, alertKey, tierCount)
		return
	}

	best := pm.selectNode(eligible, tierCount)
	if best == tier.NodeID {
		return
	}

	old := tier.NodeID
	tier.NodeID = best
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
}

// handleUnplaceable clears a tier's assignment when no eligible node exists.
func (pm *placementManager) handleUnplaceable(ctx context.Context, tier config.TierConfig, alertKey string, tierCount map[string]int) {
	if tier.NodeID != "" {
		old := tier.NodeID
		tier.NodeID = ""
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

// nodeHasStorageClass checks if a node has a storage area with the given class.
func nodeHasStorageClass(nscs []config.NodeStorageConfig, nodeID string, storageClass uint32) bool {
	if storageClass == 0 {
		return false
	}
	idx := slices.IndexFunc(nscs, func(n config.NodeStorageConfig) bool { return n.NodeID == nodeID })
	if idx < 0 {
		return false
	}
	return slices.ContainsFunc(nscs[idx].Areas, func(a config.StorageArea) bool { return a.StorageClass == storageClass })
}
