package chunking

import (
	"time"

	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// RegistryPlanningStats summarizes the vault-ctl completed-segment registry
// for operator visibility and catch-up budgeting.
type RegistryPlanningStats struct {
	TotalSegments    int
	EligibleSegments int
	RegistryRecords  uint64
	// OldestLastIngest is the minimum LastIngestTS among segments still
	// eligible for planning; zero when none remain.
	OldestLastIngest time.Time
}

// RegistryPlanningStats reads registry totals from the vault-ctl FSM using
// the same eligibility rules as the chunking planner.
func RegistryPlanningStatsFromFSM(fsm *vaultctlfsm.FSM) RegistryPlanningStats {
	if fsm == nil {
		return RegistryPlanningStats{}
	}
	entries := fsm.ListCompletedSegments()
	var stats RegistryPlanningStats
	stats.TotalSegments = len(entries)
	for _, entry := range entries {
		stats.RegistryRecords += uint64(entry.RecordCount)
		if segmentExhaustedForPlanning(fsm, entry) {
			continue
		}
		stats.EligibleSegments++
		if !entry.LastIngestTS.IsZero() &&
			(stats.OldestLastIngest.IsZero() || entry.LastIngestTS.Before(stats.OldestLastIngest)) {
			stats.OldestLastIngest = entry.LastIngestTS
		}
	}
	return stats
}
