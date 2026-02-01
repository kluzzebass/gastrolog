package repl

import (
	"fmt"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// cmdIndexes shows index details for a specific chunk.
func (r *REPL) cmdIndexes(out *strings.Builder, args []string) {
	if len(args) == 0 {
		out.WriteString("Usage: indexes <chunk-id>\n")
		return
	}

	chunkID, err := chunk.ParseChunkID(args[0])
	if err != nil {
		fmt.Fprintf(out, "Invalid chunk ID: %v\n", err)
		return
	}

	// Find the chunk and its index manager
	stores := r.orch.ChunkManagers()
	var foundStore string
	var cm chunk.ChunkManager
	var im index.IndexManager

	for _, store := range stores {
		cm = r.orch.ChunkManager(store)
		if cm == nil {
			continue
		}
		if _, err := cm.Meta(chunkID); err == nil {
			foundStore = store
			im = r.orch.IndexManager(store)
			break
		}
	}

	if foundStore == "" {
		fmt.Fprintf(out, "Chunk not found: %s\n", args[0])
		return
	}

	if im == nil {
		fmt.Fprintf(out, "No index manager for store: %s\n", foundStore)
		return
	}

	fmt.Fprintf(out, "Indexes for chunk %s:\n", chunkID.String())

	// Time index
	if timeIdx, err := im.OpenTimeIndex(chunkID); err != nil {
		fmt.Fprintf(out, "  time:   not available (%v)\n", err)
	} else {
		entries := timeIdx.Entries()
		if len(entries) > 0 {
			fmt.Fprintf(out, "  time:   %d entries (sparsity ~%d)\n",
				len(entries), estimateSparsity(entries))
		} else {
			fmt.Fprintf(out, "  time:   0 entries\n")
		}
	}

	// Token index
	if tokIdx, err := im.OpenTokenIndex(chunkID); err != nil {
		fmt.Fprintf(out, "  token:  not available (%v)\n", err)
	} else {
		entries := tokIdx.Entries()
		totalPositions := 0
		for _, e := range entries {
			totalPositions += len(e.Positions)
		}
		fmt.Fprintf(out, "  token:  %d tokens, %d positions\n", len(entries), totalPositions)
	}
}

// estimateSparsity estimates the sparsity factor from time index entries.
func estimateSparsity(entries []index.TimeIndexEntry) int {
	if len(entries) < 2 {
		return 1
	}
	// Look at position gaps between entries
	totalGap := uint64(0)
	for i := 1; i < len(entries); i++ {
		gap := entries[i].RecordPos - entries[i-1].RecordPos
		totalGap += gap
	}
	avgGap := totalGap / uint64(len(entries)-1)
	if avgGap < 1 {
		return 1
	}
	return int(avgGap)
}
