package repl

import (
	"fmt"
	"slices"
	"strings"
)

// cmdStats shows overall system statistics.
func (r *REPL) cmdStats(out *strings.Builder) {
	stores := r.orch.ChunkManagers()

	var totalChunks, totalSealed, totalActive int
	var totalRecords int64

	for _, store := range stores {
		cm := r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}

		chunks, err := cm.List()
		if err != nil {
			continue
		}

		active := cm.Active()

		for _, meta := range chunks {
			totalChunks++
			totalRecords += meta.RecordCount

			if active != nil && meta.ID == active.ID {
				totalActive++
			} else if meta.Sealed {
				totalSealed++
			}
		}
	}

	fmt.Fprintf(out, "System Statistics:\n")
	fmt.Fprintf(out, "  Stores:      %d\n", len(stores))
	fmt.Fprintf(out, "  Chunks:      %d total (%d sealed, %d active)\n",
		totalChunks, totalSealed, totalActive)
	fmt.Fprintf(out, "  Records:     %d\n", totalRecords)

	// Receiver stats
	receivers := r.orch.Receivers()
	fmt.Fprintf(out, "  Receivers:   %d\n", len(receivers))
}

// cmdStatus shows the live system state.
func (r *REPL) cmdStatus(out *strings.Builder) {
	fmt.Fprintf(out, "System Status:\n")

	// Orchestrator running status
	if r.orch.Running() {
		fmt.Fprintf(out, "  Orchestrator: running\n")
	} else {
		fmt.Fprintf(out, "  Orchestrator: stopped\n")
	}

	// Receivers
	receivers := r.orch.Receivers()
	if len(receivers) > 0 {
		slices.Sort(receivers)
		fmt.Fprintf(out, "  Receivers:    %s\n", strings.Join(receivers, ", "))
	} else {
		fmt.Fprintf(out, "  Receivers:    none\n")
	}

	// Stores and their active chunks
	stores := r.orch.ChunkManagers()
	slices.Sort(stores)

	fmt.Fprintf(out, "  Stores:\n")
	for _, store := range stores {
		cm := r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}

		active := cm.Active()
		if active != nil {
			fmt.Fprintf(out, "    [%s] active chunk: %s\n",
				store, active.ID.String())
		} else {
			fmt.Fprintf(out, "    [%s] no active chunk\n", store)
		}

		// Check for pending indexes on sealed chunks
		im := r.orch.IndexManager(store)
		if im != nil {
			chunks, err := cm.List()
			if err == nil {
				pendingIndexes := 0
				for _, meta := range chunks {
					if meta.Sealed {
						if complete, err := im.IndexesComplete(meta.ID); err == nil && !complete {
							pendingIndexes++
						}
					}
				}
				if pendingIndexes > 0 {
					fmt.Fprintf(out, "    [%s] pending indexes: %d chunks\n", store, pendingIndexes)
				}
			}
		}
	}
}
