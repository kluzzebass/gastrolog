package repl

import (
	"fmt"
	"slices"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index/analyzer"
)

// cmdAnalyze runs the index analyzer for one or all chunks.
func (r *REPL) cmdAnalyze(out *strings.Builder, args []string) {
	// If a chunk ID is given, analyze just that chunk
	if len(args) > 0 {
		chunkID, err := chunk.ParseChunkID(args[0])
		if err != nil {
			fmt.Fprintf(out, "Invalid chunk ID: %v\n", err)
			return
		}
		r.analyzeChunk(out, chunkID)
		return
	}

	// Analyze all chunks across all stores
	stores := r.client.ListStores()
	if len(stores) == 0 {
		out.WriteString("No stores registered.\n")
		return
	}

	slices.SortFunc(stores, func(a, b StoreInfo) int {
		return strings.Compare(a.DisplayName(), b.DisplayName())
	})

	for _, store := range stores {
		a := r.client.Analyzer(store.ID)
		if a == nil {
			continue
		}

		agg, err := a.AnalyzeAll()
		if err != nil {
			fmt.Fprintf(out, "[%s] Error: %v\n", store.DisplayName(), err)
			continue
		}

		fmt.Fprintf(out, "[%s] Index Analysis (%d chunks):\n", store.DisplayName(), agg.ChunksAnalyzed)

		// Summary by index type
		out.WriteString("\n  Bytes by Index Type:\n")
		for _, typ := range []analyzer.IndexType{analyzer.IndexTypeToken, analyzer.IndexTypeAttrKV, analyzer.IndexTypeKV} {
			bytes := agg.BytesByIndexType[typ]
			fmt.Fprintf(out, "    %-10s %s\n", typ, formatBytes(bytes))
		}

		// Problem counts
		if agg.ChunksWithPartialIndexes > 0 || agg.ChunksWithBudgetExhaustion > 0 ||
			agg.ChunksWithMissingIndexes > 0 || agg.ChunksWithErrors > 0 {
			out.WriteString("\n  Issues:\n")
			if agg.ChunksWithPartialIndexes > 0 {
				fmt.Fprintf(out, "    Partial indexes:    %d chunks\n", agg.ChunksWithPartialIndexes)
			}
			if agg.ChunksWithBudgetExhaustion > 0 {
				fmt.Fprintf(out, "    Budget exhaustion:  %d chunks\n", agg.ChunksWithBudgetExhaustion)
			}
			if agg.ChunksWithMissingIndexes > 0 {
				fmt.Fprintf(out, "    Missing indexes:    %d chunks\n", agg.ChunksWithMissingIndexes)
			}
			if agg.ChunksWithErrors > 0 {
				fmt.Fprintf(out, "    Errors:             %d chunks\n", agg.ChunksWithErrors)
			}
		}

		// Per-chunk details (brief)
		out.WriteString("\n  Per-Chunk Summary:\n")
		for _, ca := range agg.Chunks {
			status := "ok"
			var errorDetail string
			for _, s := range ca.Summaries {
				if s.Status == analyzer.StatusPartial {
					status = "partial"
					break
				}
				if s.Status == analyzer.StatusDisabled {
					status = "missing"
					break
				}
				if s.Status == analyzer.StatusError {
					status = "error"
					if s.Error != "" {
						errorDetail = s.Error
					}
					break
				}
			}
			timeRange := fmt.Sprintf("%s - %s",
				ca.ChunkStartTS.Format("2006-01-02 15:04:05"),
				ca.ChunkEndTS.Format("2006-01-02 15:04:05"))
			if errorDetail != "" {
				fmt.Fprintf(out, "    %s  %s  %d records  %s  [%s: %s]\n",
					ca.ChunkID.String(), timeRange, ca.ChunkRecords, formatBytes(totalIndexBytes(ca)), status, errorDetail)
			} else {
				fmt.Fprintf(out, "    %s  %s  %d records  %s  [%s]\n",
					ca.ChunkID.String(), timeRange, ca.ChunkRecords, formatBytes(totalIndexBytes(ca)), status)
			}
		}
		out.WriteByte('\n')
	}
}

// analyzeChunk analyzes a single chunk and prints detailed results.
func (r *REPL) analyzeChunk(out *strings.Builder, chunkID chunk.ChunkID) {
	// Find the chunk and its analyzer
	stores := r.client.ListStores()
	var a AnalyzerClient
	var foundStore StoreInfo

	for _, store := range stores {
		cm := r.client.ChunkManager(store.ID)
		if cm == nil {
			continue
		}
		if _, err := cm.Meta(chunkID); err == nil {
			foundStore = store
			a = r.client.Analyzer(store.ID)
			break
		}
	}

	if foundStore.ID == "" {
		fmt.Fprintf(out, "Chunk not found: %s\n", chunkID.String())
		return
	}

	if a == nil {
		fmt.Fprintf(out, "No analyzer for store: %s\n", foundStore.DisplayName())
		return
	}

	ca, err := a.AnalyzeChunk(chunkID)
	if err != nil {
		fmt.Fprintf(out, "Error analyzing chunk: %v\n", err)
		return
	}

	fmt.Fprintf(out, "Index Analysis for %s:\n", chunkID.String())
	fmt.Fprintf(out, "  Store:    %s\n", foundStore.DisplayName())
	fmt.Fprintf(out, "  Records:  %d\n", ca.ChunkRecords)
	fmt.Fprintf(out, "  Raw Size: %s\n", formatBytes(ca.ChunkBytes))
	fmt.Fprintf(out, "  Sealed:   %v\n", ca.Sealed)

	// Index summaries
	out.WriteString("\n  Index Summary:\n")
	for _, s := range ca.Summaries {
		statusStr := string(s.Status)
		if s.Reason != "" {
			statusStr += " (" + string(s.Reason) + ")"
		}
		fmt.Fprintf(out, "    %-10s %s  %.1f%% of chunk  [%s]\n",
			s.IndexType, formatBytes(s.BytesUsed), s.PercentOfChunk*100, statusStr)
	}

	// Token index details
	if ca.TokenStats != nil {
		ts := ca.TokenStats
		out.WriteString("\n  Token Index:\n")
		fmt.Fprintf(out, "    Unique tokens:  %d\n", ts.UniqueTokens)
		fmt.Fprintf(out, "    Total positions:%d\n", ts.TotalTokenOccurrences)
		fmt.Fprintf(out, "    Avg pos/token:  %.1f\n", ts.AvgPositionsPerToken)
		fmt.Fprintf(out, "    Frequency:      p50=%d  p95=%d  max=%d\n",
			ts.P50TokenFrequency, ts.P95TokenFrequency, ts.MaxTokenFrequency)
		if len(ts.TopTokensByFrequency) > 0 {
			out.WriteString("    Top tokens:     ")
			for i, tf := range ts.TopTokensByFrequency {
				if i > 0 {
					out.WriteString(", ")
				}
				if i >= 5 {
					out.WriteString("...")
					break
				}
				fmt.Fprintf(out, "%s(%d)", tf.Token, tf.Frequency)
			}
			out.WriteByte('\n')
		}
	}

	// Attr KV index details
	if ca.AttrKVStats != nil {
		as := ca.AttrKVStats
		out.WriteString("\n  Attribute Index:\n")
		fmt.Fprintf(out, "    Unique keys:    %d\n", as.UniqueKeys)
		fmt.Fprintf(out, "    Unique values:  %d\n", as.UniqueValues)
		fmt.Fprintf(out, "    Key-value pairs:%d\n", as.UniqueKeyValuePairs)
		fmt.Fprintf(out, "    Total positions:%d\n", as.TotalOccurrences)
		fmt.Fprintf(out, "    Coverage:       %.1f%% of records\n", as.PercentRecordsCovered)
		if len(as.TopKeysByOccurrences) > 0 {
			out.WriteString("    Top keys:       ")
			for i, ks := range as.TopKeysByOccurrences {
				if i > 0 {
					out.WriteString(", ")
				}
				if i >= 5 {
					out.WriteString("...")
					break
				}
				fmt.Fprintf(out, "%s(%d)", ks.Key, ks.TotalOccurrences)
			}
			out.WriteByte('\n')
		}
	}

	// KV index details
	if ca.KVStats != nil {
		ks := ca.KVStats
		out.WriteString("\n  Message KV Index:\n")
		fmt.Fprintf(out, "    Keys indexed:   %d\n", ks.KeysIndexed)
		fmt.Fprintf(out, "    Values indexed: %d\n", ks.ValuesIndexed)
		fmt.Fprintf(out, "    Pairs indexed:  %d\n", ks.PairsIndexed)
		if ks.BudgetExhausted {
			out.WriteString("    ⚠ Budget exhausted (index is capped)\n")
		}
		if len(ks.TopKeysByFrequency) > 0 {
			out.WriteString("    Top keys:       ")
			for i, tf := range ks.TopKeysByFrequency {
				if i > 0 {
					out.WriteString(", ")
				}
				if i >= 5 {
					out.WriteString("...")
					break
				}
				fmt.Fprintf(out, "%s(%d)", tf.Token, tf.Frequency)
			}
			out.WriteByte('\n')
		}
	}
}

// totalIndexBytes sums the bytes from all index summaries.
func totalIndexBytes(ca analyzer.ChunkAnalysis) int64 {
	var total int64
	for _, s := range ca.Summaries {
		total += s.BytesUsed
	}
	return total
}

// formatBytes formats a byte count in human-readable form.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
