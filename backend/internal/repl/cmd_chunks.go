package repl

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
)

// cmdChunks lists all chunks across all stores with their metadata.
func (r *REPL) cmdChunks(out *strings.Builder) {
	stores := r.client.ListStores()
	if len(stores) == 0 {
		out.WriteString("No chunk managers registered.\n")
		return
	}

	slices.SortFunc(stores, func(a, b StoreInfo) int {
		return strings.Compare(a.DisplayName(), b.DisplayName())
	})

	for _, store := range stores {
		cm := r.client.ChunkManager(store.ID)
		if cm == nil {
			continue
		}

		chunks, err := cm.List()
		if err != nil {
			fmt.Fprintf(out, "[%s] Error listing chunks: %v\n", store.DisplayName(), err)
			continue
		}

		if len(chunks) == 0 {
			fmt.Fprintf(out, "[%s] No chunks\n", store.DisplayName())
			continue
		}

		// Sort by StartTS
		slices.SortFunc(chunks, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS)
		})

		fmt.Fprintf(out, "[%s] %d chunks:\n", store.DisplayName(), len(chunks))

		// Check which is the active chunk
		active := cm.Active()
		var activeID chunk.ChunkID
		if active != nil {
			activeID = active.ID
		}

		for _, meta := range chunks {
			status := "sealed"
			if meta.ID == activeID {
				status = "active"
			} else if !meta.Sealed {
				status = "open"
			}

			// Format time range
			timeRange := fmt.Sprintf("%s - %s",
				meta.StartTS.Format("2006-01-02 15:04:05"),
				meta.EndTS.Format("2006-01-02 15:04:05"))

			fmt.Fprintf(out, "  %s  %s  %d records  [%s]\n",
				meta.ID.String(), timeRange, meta.RecordCount, status)
		}
	}
}

// cmdChunk shows detailed information about a specific chunk.
func (r *REPL) cmdChunk(out *strings.Builder, args []string) {
	if len(args) == 0 {
		out.WriteString("Usage: chunk <chunk-id>\n")
		return
	}

	chunkID, err := chunk.ParseChunkID(args[0])
	if err != nil {
		fmt.Fprintf(out, "Invalid chunk ID: %v\n", err)
		return
	}

	// Find the chunk across all stores
	stores := r.client.ListStores()
	var foundStore StoreInfo
	var meta chunk.ChunkMeta
	var cm ChunkReader

	for _, store := range stores {
		cm = r.client.ChunkManager(store.ID)
		if cm == nil {
			continue
		}
		m, err := cm.Meta(chunkID)
		if err == nil {
			foundStore = store
			meta = m
			break
		}
	}

	if foundStore.ID == "" {
		fmt.Fprintf(out, "Chunk not found: %s\n", args[0])
		return
	}

	// Determine status
	status := "sealed"
	if active := cm.Active(); active != nil && active.ID == chunkID {
		status = "active"
	} else if !meta.Sealed {
		status = "open"
	}

	fmt.Fprintf(out, "Chunk: %s\n", meta.ID.String())
	fmt.Fprintf(out, "  Store:    %s\n", foundStore.DisplayName())
	fmt.Fprintf(out, "  Status:   %s\n", status)
	fmt.Fprintf(out, "  StartTS:  %s\n", meta.StartTS.Format(time.RFC3339Nano))
	fmt.Fprintf(out, "  EndTS:    %s\n", meta.EndTS.Format(time.RFC3339Nano))
	fmt.Fprintf(out, "  Records:  %d\n", meta.RecordCount)

	// Show index status if sealed
	if meta.Sealed {
		im := r.client.IndexManager(foundStore.ID)
		if im != nil {
			complete, err := im.IndexesComplete(chunkID)
			if err != nil {
				fmt.Fprintf(out, "  Indexes:  error checking: %v\n", err)
			} else if complete {
				fmt.Fprintf(out, "  Indexes:  complete\n")
			} else {
				fmt.Fprintf(out, "  Indexes:  incomplete\n")
			}
		}
	} else {
		fmt.Fprintf(out, "  Indexes:  n/a (not sealed)\n")
	}
}
