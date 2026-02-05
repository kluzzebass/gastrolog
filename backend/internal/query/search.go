package query

import (
	"container/heap"
	"context"
	"iter"
	"math"
	"slices"
	"time"

	"gastrolog/internal/chunk"
)

// positionExhausted is a sentinel value indicating a chunk has been fully consumed.
const positionExhausted = math.MaxUint64

// Search returns an iterator over records matching the query, ordered by ingest timestamp.
// The iterator yields (record, nil) for each match, or (zero, err) on error.
// After yielding an error, iteration stops.
//
// For multi-store engines, this searches across all stores (or stores matching
// store=X predicates in the query) and merge-sorts results by IngestTS.
//
// The resume parameter allows continuing from a previous search. Pass nil to start fresh.
// The returned nextToken function returns a ResumeToken if iteration stopped early
// (limit reached, caller break, error, or context cancellation), or nil if all
// matching records were returned.
func (e *Engine) Search(ctx context.Context, q Query, resume *ResumeToken) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	// Normalize query to ensure BoolExpr is set (converts legacy Tokens/KV if needed).
	q = q.Normalize()

	// Extract store predicates and get remaining query expression.
	allStores := e.listStores()
	selectedStores, remainingExpr := ExtractStoreFilter(q.BoolExpr, allStores)

	// Normalize resume token to new format.
	// For single-store mode, use the first selected store as default.
	defaultStoreID := "default"
	if len(selectedStores) > 0 {
		defaultStoreID = selectedStores[0]
	} else if len(allStores) > 0 {
		defaultStoreID = allStores[0]
	}
	resume = resume.Normalize(defaultStoreID)
	if selectedStores == nil {
		selectedStores = allStores // no store filter means all stores
	}

	// Update query to use remaining expression (without store predicates).
	q.BoolExpr = remainingExpr

	// Track state for resume token generation.
	var lastRefs []MultiStorePosition
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		// Collect all chunks from selected stores with time overlap.
		type storeChunk struct {
			storeID string
			meta    chunk.ChunkMeta
		}
		var allChunks []storeChunk

		for _, storeID := range selectedStores {
			cm, _ := e.getStoreManagers(storeID)
			if cm == nil {
				continue
			}

			metas, err := cm.List()
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}

			candidates := e.selectChunks(metas, q)
			for _, meta := range candidates {
				allChunks = append(allChunks, storeChunk{storeID: storeID, meta: meta})
			}
		}

		if len(allChunks) == 0 {
			completed = true
			return
		}

		// Validate resume token: all referenced chunks must exist.
		if resume != nil && len(resume.Positions) > 0 {
			// Build set of available chunk IDs.
			availableChunks := make(map[chunk.ChunkID]bool)
			for _, sc := range allChunks {
				availableChunks[sc.meta.ID] = true
			}

			// Check each resume position references an existing chunk.
			for _, pos := range resume.Positions {
				// Skip exhausted markers - they don't need to exist anymore.
				if pos.Position == positionExhausted {
					continue
				}
				if !availableChunks[pos.ChunkID] {
					yield(chunk.Record{}, ErrInvalidResumeToken)
					return
				}
			}
		}

		// For single chunk, use simple iteration (no heap needed).
		if len(allChunks) == 1 {
			sc := allChunks[0]
			var startPos *uint64
			if resume != nil {
				for _, pos := range resume.Positions {
					if pos.StoreID == sc.storeID && pos.ChunkID == sc.meta.ID {
						startPos = &pos.Position
						break
					}
				}
			}

			count := 0
			for rr, err := range e.searchChunkWithRef(ctx, q, sc.storeID, sc.meta, startPos) {
				if err != nil {
					lastRefs = []MultiStorePosition{{StoreID: rr.StoreID, ChunkID: rr.Ref.ChunkID, Position: rr.Ref.Pos}}
					yield(chunk.Record{}, err)
					return
				}

				lastRefs = []MultiStorePosition{{StoreID: rr.StoreID, ChunkID: rr.Ref.ChunkID, Position: rr.Ref.Pos}}

				if !yield(rr.Record, nil) {
					return
				}

				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}
			}
			completed = true
			return
		}

		// Multiple chunks: use heap-based merge sort.
		// Build resume position map for quick lookup.
		resumePositions := make(map[string]map[chunk.ChunkID]uint64)
		if resume != nil {
			for _, pos := range resume.Positions {
				if resumePositions[pos.StoreID] == nil {
					resumePositions[pos.StoreID] = make(map[chunk.ChunkID]uint64)
				}
				resumePositions[pos.StoreID][pos.ChunkID] = pos.Position
			}
		}

		// Track current position for ALL chunks (for resume token).
		// Position is updated as records are yielded, or set to positionExhausted when done.
		type chunkKey struct {
			storeID string
			chunkID chunk.ChunkID
		}
		chunkPositions := make(map[chunkKey]uint64)

		// Initialize heap with first record from each chunk.
		var h heap.Interface
		if q.Reverse() {
			rh := make(mergeHeapReverse, 0, len(allChunks))
			h = &rh
		} else {
			fh := make(mergeHeap, 0, len(allChunks))
			h = &fh
		}

		// Track active cursors for cleanup.
		type activeScanner struct {
			storeID string
			chunkID chunk.ChunkID
			iter    func() (recordWithRef, error, bool)
			stop    func()
		}
		var activeScanners []activeScanner
		defer func() {
			for _, s := range activeScanners {
				if s.stop != nil {
					s.stop()
				}
			}
		}()

		// Open iterators for each chunk and prime the heap.
		for _, sc := range allChunks {
			key := chunkKey{storeID: sc.storeID, chunkID: sc.meta.ID}

			// Check if this chunk was already exhausted in a previous iteration.
			if storePositions, ok := resumePositions[sc.storeID]; ok {
				if pos, ok := storePositions[sc.meta.ID]; ok && pos == positionExhausted {
					// Chunk was exhausted, mark it and skip.
					chunkPositions[key] = positionExhausted
					continue
				}
			}

			var startPos *uint64
			if storePositions, ok := resumePositions[sc.storeID]; ok {
				if pos, ok := storePositions[sc.meta.ID]; ok {
					startPos = &pos
				}
			}

			// Create iterator for this chunk.
			iterSeq := e.searchChunkWithRef(ctx, q, sc.storeID, sc.meta, startPos)
			next, stop := iter.Pull2(iterSeq)

			// Get first record.
			rr, err, ok := next()
			if !ok {
				stop()
				// Chunk is exhausted from the start.
				chunkPositions[key] = positionExhausted
				continue
			}
			if err != nil {
				stop()
				yield(chunk.Record{}, err)
				return
			}

			// Note: we don't initialize chunkPositions here because the record
			// hasn't been yielded yet. Position is only set when we yield.

			entry := &cursorEntry{
				storeID: sc.storeID,
				chunkID: sc.meta.ID,
				rec:     rr.Record,
				ref:     rr.Ref,
			}
			heap.Push(h, entry)

			activeScanners = append(activeScanners, activeScanner{
				storeID: sc.storeID,
				chunkID: sc.meta.ID,
				iter:    next,
				stop:    stop,
			})
		}

		// Helper to build resume token from current positions.
		buildLastRefs := func() {
			lastRefs = nil
			for key, pos := range chunkPositions {
				lastRefs = append(lastRefs, MultiStorePosition{
					StoreID:  key.storeID,
					ChunkID:  key.chunkID,
					Position: pos,
				})
			}
		}

		// Find scanner by storeID and chunkID.
		findScanner := func(storeID string, chunkID chunk.ChunkID) *activeScanner {
			for i := range activeScanners {
				if activeScanners[i].storeID == storeID && activeScanners[i].chunkID == chunkID {
					return &activeScanners[i]
				}
			}
			return nil
		}

		// Merge loop.
		count := 0
		for h.Len() > 0 {
			if err := ctx.Err(); err != nil {
				buildLastRefs()
				yield(chunk.Record{}, err)
				return
			}

			entry := heap.Pop(h).(*cursorEntry)
			key := chunkKey{storeID: entry.storeID, chunkID: entry.chunkID}

			// Update position for this chunk.
			chunkPositions[key] = entry.ref.Pos

			if !yield(entry.rec, nil) {
				buildLastRefs()
				return
			}

			count++
			if q.Limit > 0 && count >= q.Limit {
				buildLastRefs()
				return
			}

			// Advance this scanner.
			scanner := findScanner(entry.storeID, entry.chunkID)
			if scanner == nil || scanner.iter == nil {
				continue
			}

			rr, err, ok := scanner.iter()
			if !ok {
				scanner.stop()
				scanner.iter = nil
				scanner.stop = nil
				// Mark chunk as exhausted.
				chunkPositions[key] = positionExhausted
				continue
			}
			if err != nil {
				buildLastRefs()
				yield(chunk.Record{}, err)
				return
			}

			entry.rec = rr.Record
			entry.ref = rr.Ref
			heap.Push(h, entry)
		}

		completed = true
	}

	nextToken := func() *ResumeToken {
		if completed || len(lastRefs) == 0 {
			return nil
		}
		return &ResumeToken{Positions: lastRefs}
	}

	return seq, nextToken
}

// SearchThenFollow finds the first record matching the query, then yields all
// subsequent records (ignoring source and token filters) until End, limit, or EOF.
//
// This is useful for "find error, then show everything after" use cases.
// The source and token filters only apply to finding the first match.
// Time bounds and limit still apply to all yielded records.
//
// For multi-store engines, this searches across all stores (or stores matching
// store=X predicates in the query) and merge-sorts results by IngestTS.
func (e *Engine) SearchThenFollow(ctx context.Context, q Query, resume *ResumeToken) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	// Normalize query to ensure BoolExpr is set (converts legacy Tokens/KV if needed).
	q = q.Normalize()

	// Extract store predicates and get remaining query expression.
	allStores := e.listStores()
	selectedStores, remainingExpr := ExtractStoreFilter(q.BoolExpr, allStores)
	if selectedStores == nil {
		selectedStores = allStores
	}

	// Update query to use remaining expression (without store predicates).
	q.BoolExpr = remainingExpr

	var lastRefs []MultiStorePosition
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		// Collect all chunks from selected stores with time overlap.
		type storeChunk struct {
			storeID string
			meta    chunk.ChunkMeta
		}
		var allChunks []storeChunk

		for _, storeID := range selectedStores {
			cm, _ := e.getStoreManagers(storeID)
			if cm == nil {
				continue
			}

			metas, err := cm.List()
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}

			candidates := e.selectChunks(metas, q)
			for _, meta := range candidates {
				allChunks = append(allChunks, storeChunk{storeID: storeID, meta: meta})
			}
		}

		if len(allChunks) == 0 {
			completed = true
			return
		}

		// Create a follow query that removes all filters.
		followQuery := q
		followQuery.BoolExpr = nil

		// Track current position for ALL chunks (for resume token).
		type chunkKey struct {
			storeID string
			chunkID chunk.ChunkID
		}
		chunkPositions := make(map[chunkKey]uint64)

		// Helper to build resume token from current positions.
		buildLastRefs := func() {
			lastRefs = nil
			for key, pos := range chunkPositions {
				lastRefs = append(lastRefs, MultiStorePosition{
					StoreID:  key.storeID,
					ChunkID:  key.chunkID,
					Position: pos,
				})
			}
		}

		// Track active cursors for cleanup.
		type activeScanner struct {
			storeID string
			chunkID chunk.ChunkID
			iter    func() (recordWithRef, error, bool)
			stop    func()
		}
		var activeScanners []activeScanner
		defer func() {
			for _, s := range activeScanners {
				if s.stop != nil {
					s.stop()
				}
			}
		}()

		// Find scanner by storeID and chunkID.
		findScanner := func(storeID string, chunkID chunk.ChunkID) *activeScanner {
			for i := range activeScanners {
				if activeScanners[i].storeID == storeID && activeScanners[i].chunkID == chunkID {
					return &activeScanners[i]
				}
			}
			return nil
		}

		// Initialize heap with first record from each chunk (using filtered query).
		var h heap.Interface
		if q.Reverse() {
			rh := make(mergeHeapReverse, 0, len(allChunks))
			h = &rh
		} else {
			fh := make(mergeHeap, 0, len(allChunks))
			h = &fh
		}

		// Open iterators for each chunk and prime the heap.
		for _, sc := range allChunks {
			iterSeq := e.searchChunkWithRef(ctx, q, sc.storeID, sc.meta, nil)
			next, stop := iter.Pull2(iterSeq)

			rr, err, ok := next()
			if !ok {
				stop()
				continue
			}
			if err != nil {
				stop()
				yield(chunk.Record{}, err)
				return
			}

			entry := &cursorEntry{
				storeID: sc.storeID,
				chunkID: sc.meta.ID,
				rec:     rr.Record,
				ref:     rr.Ref,
			}
			heap.Push(h, entry)

			activeScanners = append(activeScanners, activeScanner{
				storeID: sc.storeID,
				chunkID: sc.meta.ID,
				iter:    next,
				stop:    stop,
			})
		}

		if h.Len() == 0 {
			// No matches found.
			completed = true
			return
		}

		// Phase 1: Pop the first match from heap (oldest/newest depending on direction).
		firstMatch := heap.Pop(h).(*cursorEntry)
		key := chunkKey{storeID: firstMatch.storeID, chunkID: firstMatch.chunkID}
		chunkPositions[key] = firstMatch.ref.Pos

		if !yield(firstMatch.rec, nil) {
			buildLastRefs()
			return
		}

		count := 1
		if q.Limit > 0 && count >= q.Limit {
			buildLastRefs()
			return
		}

		// Phase 2: Follow mode - switch all scanners to unfiltered query and continue merge.
		// Close all existing scanners.
		for _, s := range activeScanners {
			if s.stop != nil {
				s.stop()
			}
		}
		activeScanners = nil

		// Clear heap.
		for h.Len() > 0 {
			heap.Pop(h)
		}

		// For follow mode, we need to continue from the first match's position.
		// - For the chunk containing the first match: start from position+1
		// - For other chunks: they need to start from a position with IngestTS > firstMatch.IngestTS
		//   We use time-based filtering by adjusting followQuery.Start
		firstMatchTS := firstMatch.rec.IngestTS

		// Reopen iterators for ALL chunks with follow query (no filters), starting appropriately.
		for _, sc := range allChunks {
			key := chunkKey{storeID: sc.storeID, chunkID: sc.meta.ID}

			var startPos *uint64
			if key.storeID == firstMatch.storeID && key.chunkID == firstMatch.chunkID {
				// This chunk had the first match - start from the match position.
				// searchChunkWithRef will skip this position (since startPos means "already returned"),
				// so we pass the match position itself, not position+1.
				startPos = &firstMatch.ref.Pos
			}
			// For other chunks, we'll rely on the heap to filter by timestamp.
			// All chunks are iterated from start, but only records after firstMatchTS will be yielded.

			iterSeq := e.searchChunkWithRef(ctx, followQuery, sc.storeID, sc.meta, startPos)
			next, stop := iter.Pull2(iterSeq)

			// Skip records until we find one with IngestTS > firstMatchTS (for non-match chunks).
			var rr recordWithRef
			var err error
			var ok bool
			for {
				rr, err, ok = next()
				if !ok {
					stop()
					chunkPositions[key] = positionExhausted
					break
				}
				if err != nil {
					stop()
					buildLastRefs()
					yield(chunk.Record{}, err)
					return
				}
				// For the first-match chunk, we already skipped past the match.
				// For other chunks, skip records at or before firstMatchTS.
				if key.storeID == firstMatch.storeID && key.chunkID == firstMatch.chunkID {
					break // Don't skip - we're already positioned correctly.
				}
				if rr.Record.IngestTS.After(firstMatchTS) {
					break // Found a record after the first match.
				}
				// Continue to next record.
			}

			if !ok {
				continue
			}

			entry := &cursorEntry{
				storeID: sc.storeID,
				chunkID: sc.meta.ID,
				rec:     rr.Record,
				ref:     rr.Ref,
			}
			heap.Push(h, entry)

			activeScanners = append(activeScanners, activeScanner{
				storeID: sc.storeID,
				chunkID: sc.meta.ID,
				iter:    next,
				stop:    stop,
			})
		}

		// Merge loop for follow phase.
		for h.Len() > 0 {
			if err := ctx.Err(); err != nil {
				buildLastRefs()
				yield(chunk.Record{}, err)
				return
			}

			entry := heap.Pop(h).(*cursorEntry)
			key := chunkKey{storeID: entry.storeID, chunkID: entry.chunkID}
			chunkPositions[key] = entry.ref.Pos

			if !yield(entry.rec, nil) {
				buildLastRefs()
				return
			}

			count++
			if q.Limit > 0 && count >= q.Limit {
				buildLastRefs()
				return
			}

			// Advance this scanner.
			scanner := findScanner(entry.storeID, entry.chunkID)
			if scanner == nil || scanner.iter == nil {
				continue
			}

			rr, err, ok := scanner.iter()
			if !ok {
				scanner.stop()
				scanner.iter = nil
				scanner.stop = nil
				chunkPositions[key] = positionExhausted
				continue
			}
			if err != nil {
				buildLastRefs()
				yield(chunk.Record{}, err)
				return
			}

			entry.rec = rr.Record
			entry.ref = rr.Ref
			heap.Push(h, entry)
		}

		completed = true
	}

	nextToken := func() *ResumeToken {
		if completed || len(lastRefs) == 0 {
			return nil
		}
		return &ResumeToken{Positions: lastRefs}
	}

	return seq, nextToken
}

// Follow tails records from all stores, waiting for new arrivals.
// It first yields any existing records matching the query (optionally filtered),
// then continuously polls for new records until the context is cancelled.
//
// Unlike SearchThenFollow, this method never completes on its own - it keeps
// polling for new records until ctx is cancelled.
//
// For multi-store engines, records are merged by IngestTS across all stores.
func (e *Engine) Follow(ctx context.Context, q Query) iter.Seq2[chunk.Record, error] {
	// Normalize query to ensure BoolExpr is set.
	q = q.Normalize()

	// Extract store predicates.
	allStores := e.listStores()
	selectedStores, remainingExpr := ExtractStoreFilter(q.BoolExpr, allStores)
	if selectedStores == nil {
		selectedStores = allStores
	}
	q.BoolExpr = remainingExpr

	return func(yield func(chunk.Record, error) bool) {
		// Track last seen position per store+chunk.
		type chunkKey struct {
			storeID string
			chunkID chunk.ChunkID
		}
		lastPositions := make(map[chunkKey]uint64)

		// Poll interval for new records.
		const pollInterval = 100 * time.Millisecond

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Collect records from all stores since last positions.
			type pendingRecord struct {
				storeID string
				rec     chunk.Record
				ref     chunk.RecordRef
			}
			var pending []pendingRecord

			for _, storeID := range selectedStores {
				cm, _ := e.getStoreManagers(storeID)
				if cm == nil {
					continue
				}

				// Get all chunks (including active).
				metas, err := cm.List()
				if err != nil {
					if !yield(chunk.Record{}, err) {
						return
					}
					continue
				}

				for _, meta := range metas {
					key := chunkKey{storeID: storeID, chunkID: meta.ID}

					cursor, err := cm.OpenCursor(meta.ID)
					if err != nil {
						continue
					}

					// Seek past already-seen records.
					if lastPos, ok := lastPositions[key]; ok {
						if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: lastPos}); err != nil {
							cursor.Close()
							continue
						}
						// Skip the record at lastPos (already yielded).
						if _, _, err := cursor.Next(); err != nil {
							cursor.Close()
							continue
						}
					}

					// Read new records.
					for {
						rec, ref, err := cursor.Next()
						if err != nil {
							break // ErrNoMoreRecords or other error
						}

						// Apply query filter if present.
						if q.BoolExpr != nil && !e.matchesFilter(rec, q) {
							lastPositions[key] = ref.Pos
							continue
						}

						pending = append(pending, pendingRecord{
							storeID: storeID,
							rec:     rec,
							ref:     ref,
						})
						lastPositions[key] = ref.Pos
					}
					cursor.Close()
				}
			}

			// Sort pending records by IngestTS.
			slices.SortFunc(pending, func(a, b pendingRecord) int {
				return a.rec.IngestTS.Compare(b.rec.IngestTS)
			})

			// Yield sorted records.
			for _, p := range pending {
				if !yield(p.rec, nil) {
					return
				}
			}

			// Wait before polling again.
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}
}

// matchesFilter checks if a record matches the query's boolean expression.
func (e *Engine) matchesFilter(rec chunk.Record, q Query) bool {
	if q.BoolExpr == nil {
		return true
	}
	// Use the scanner's filter logic - simplified version here.
	// For now, just return true and let the caller handle filtering.
	// TODO: implement proper filter matching.
	return true
}

// SearchWithContext finds records matching the query and includes surrounding
// context records. For each match, it yields ContextBefore records before the
// match, the match itself, and ContextAfter records after the match.
//
// Context records are yielded in timestamp order (oldest first for forward,
// newest first for reverse). Context gathering may cross chunk boundaries.
//
// Note: This method buffers context records in memory. For large context windows,
// consider using SearchThenFollow or manual cursor operations instead.
func (e *Engine) SearchWithContext(ctx context.Context, q Query) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	// Multi-store mode not yet supported for SearchWithContext.
	if e.isMultiStore() {
		return func(yield func(chunk.Record, error) bool) {
			yield(chunk.Record{}, ErrMultiStoreNotSupported)
		}, func() *ResumeToken { return nil }
	}

	// Normalize query to ensure BoolExpr is set (converts legacy Tokens/KV if needed).
	q = q.Normalize()

	var nextRef *chunk.RecordRef
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		metas, err := e.chunks.List()
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}

		// Sort all chunks by StartTS for consistent ordering.
		allChunks := e.selectChunks(metas, q)

		// Also need all chunks sorted ascending for context gathering.
		allChunksAsc := make([]chunk.ChunkMeta, len(metas))
		copy(allChunksAsc, metas)
		slices.SortFunc(allChunksAsc, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS)
		})

		count := 0
		for _, meta := range allChunks {
			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			for rr, err := range e.searchChunkWithRef(ctx, q, "", meta, nil) {
				if err != nil {
					nextRef = &rr.Ref
					yield(chunk.Record{}, err)
					return
				}

				// Gather context before (in iteration order).
				// For forward: before = chronologically older records
				// For reverse: before = chronologically newer records
				if q.ContextBefore > 0 {
					var beforeRecs []chunk.Record
					var err error
					if q.Reverse() {
						// In reverse mode, "before" in iteration order means chronologically after.
						beforeRecs, err = e.gatherContextAfter(ctx, allChunksAsc, rr.Ref, q.ContextBefore, true)
					} else {
						beforeRecs, err = e.gatherContextBefore(ctx, allChunksAsc, rr.Ref, q.ContextBefore, false)
					}
					if err != nil {
						yield(chunk.Record{}, err)
						return
					}
					for _, rec := range beforeRecs {
						if !yield(rec, nil) {
							nextRef = &rr.Ref
							return
						}
						count++
						if q.Limit > 0 && count >= q.Limit {
							nextRef = &rr.Ref
							return
						}
					}
				}

				// Yield the match.
				nextRef = &rr.Ref
				if !yield(rr.Record, nil) {
					return
				}
				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}

				// Gather context after (in iteration order).
				// For forward: after = chronologically newer records
				// For reverse: after = chronologically older records
				if q.ContextAfter > 0 {
					var afterRecs []chunk.Record
					var err error
					if q.Reverse() {
						// In reverse mode, "after" in iteration order means chronologically before.
						afterRecs, err = e.gatherContextBefore(ctx, allChunksAsc, rr.Ref, q.ContextAfter, true)
					} else {
						afterRecs, err = e.gatherContextAfter(ctx, allChunksAsc, rr.Ref, q.ContextAfter, false)
					}
					if err != nil {
						yield(chunk.Record{}, err)
						return
					}
					for _, rec := range afterRecs {
						if !yield(rec, nil) {
							return
						}
						count++
						if q.Limit > 0 && count >= q.Limit {
							return
						}
					}
				}
			}
		}

		completed = true
	}

	nextToken := func() *ResumeToken {
		if completed || nextRef == nil {
			return nil
		}
		return &ResumeToken{Next: *nextRef}
	}

	return seq, nextToken
}
