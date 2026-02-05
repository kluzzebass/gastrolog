package repl

import (
	"fmt"
	"strings"

	"gastrolog/internal/chunk"
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
	stores := r.client.ListStores()
	var foundStore string
	var cm ChunkReader
	var im IndexReader

	for _, store := range stores {
		cm = r.client.ChunkManager(store)
		if cm == nil {
			continue
		}
		if _, err := cm.Meta(chunkID); err == nil {
			foundStore = store
			im = r.client.IndexManager(store)
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
