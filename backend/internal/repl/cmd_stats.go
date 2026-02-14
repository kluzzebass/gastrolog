package repl

import (
	"fmt"
	"slices"
	"strings"
)

// cmdStats shows overall system statistics.
func (r *REPL) cmdStats(out *strings.Builder) {
	stores := r.client.ListStores()

	var totalChunks, totalSealed, totalActive int
	var totalRecords int64

	for _, store := range stores {
		cm := r.client.ChunkManager(store.ID)
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
}

// cmdStatus shows the live system state.
func (r *REPL) cmdStatus(out *strings.Builder) {
	fmt.Fprintf(out, "System Status:\n")

	// Orchestrator running status
	if r.client.IsRunning() {
		fmt.Fprintf(out, "  Server: running\n")
	} else {
		fmt.Fprintf(out, "  Server: stopped\n")
	}

	// Stores and their active chunks
	stores := r.client.ListStores()
	slices.SortFunc(stores, func(a, b StoreInfo) int {
		return strings.Compare(a.DisplayName(), b.DisplayName())
	})

	fmt.Fprintf(out, "  Stores:\n")
	for _, store := range stores {
		cm := r.client.ChunkManager(store.ID)
		if cm == nil {
			continue
		}

		active := cm.Active()
		if active != nil {
			fmt.Fprintf(out, "    [%s] active chunk: %s\n",
				store.DisplayName(), active.ID.String())
		} else {
			fmt.Fprintf(out, "    [%s] no active chunk\n", store.DisplayName())
		}

		// Check for pending indexes on sealed chunks
		im := r.client.IndexManager(store.ID)
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
					fmt.Fprintf(out, "    [%s] pending indexes: %d chunks\n", store.DisplayName(), pendingIndexes)
				}
			}
		}
	}
}
