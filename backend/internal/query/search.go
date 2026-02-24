package query

import (
	"container/heap"
	"context"
	"iter"
	"math"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

// positionExhausted is a sentinel value indicating a chunk has been fully consumed.
const positionExhausted = math.MaxUint64

// storeChunk pairs a store ID with its chunk metadata.
type storeChunk struct {
	storeID uuid.UUID
	meta    chunk.ChunkMeta
}

// mergeKey uniquely identifies a chunk within a store.
type mergeKey struct {
	storeID uuid.UUID
	chunkID chunk.ChunkID
}

// activeScanner tracks an open iterator for a chunk during merge operations.
type activeScanner struct {
	storeID uuid.UUID
	chunkID chunk.ChunkID
	iter    func() (recordWithRef, error, bool)
	stop    func()
}

// pendingRecord holds a record collected during Follow polling.
type pendingRecord struct {
	storeID uuid.UUID
	rec     chunk.Record
	ref     chunk.RecordRef
}

// mergeState holds shared mutable state for heap-based merge operations.
// It is used by Search, SearchThenFollow, and their helper methods.
type mergeState struct {
	h              heap.Interface
	scanners       []activeScanner
	chunkPositions map[mergeKey]uint64
	lastRefs       *[]MultiStorePosition
}

// cleanup stops all active scanners.
func (ms *mergeState) cleanup() {
	for _, s := range ms.scanners {
		if s.stop != nil {
			s.stop()
		}
	}
}

// buildLastRefs populates lastRefs from chunkPositions.
func (ms *mergeState) buildLastRefs() {
	refs := make([]MultiStorePosition, 0, len(ms.chunkPositions))
	for key, pos := range ms.chunkPositions {
		refs = append(refs, MultiStorePosition{
			StoreID:  key.storeID,
			ChunkID:  key.chunkID,
			Position: pos,
		})
	}
	*ms.lastRefs = refs
}

// findScanner looks up a scanner by store and chunk ID.
func (ms *mergeState) findScanner(storeID uuid.UUID, chunkID chunk.ChunkID) *activeScanner {
	for i := range ms.scanners {
		if ms.scanners[i].storeID == storeID && ms.scanners[i].chunkID == chunkID {
			return &ms.scanners[i]
		}
	}
	return nil
}

// advanceScanner advances the scanner for the given entry, pushing the next
// record onto the heap or marking the chunk as exhausted.
// Returns (error, false) if advancing produced an error; (nil, true) otherwise.
func (ms *mergeState) advanceScanner(entry *cursorEntry) (error, bool) {
	key := mergeKey{storeID: entry.storeID, chunkID: entry.chunkID}
	scanner := ms.findScanner(entry.storeID, entry.chunkID)
	if scanner == nil || scanner.iter == nil {
		return nil, true
	}

	rr, err, ok := scanner.iter()
	if !ok {
		scanner.stop()
		scanner.iter = nil
		scanner.stop = nil
		ms.chunkPositions[key] = positionExhausted
		return nil, true
	}
	if err != nil {
		return err, false
	}

	entry.rec = rr.Record
	entry.ref = rr.Ref
	heap.Push(ms.h, entry)
	return nil, true
}

// collectStoreChunks gathers chunks from selected stores that overlap the query.
func (e *Engine) collectStoreChunks(
	selectedStores []uuid.UUID,
	q Query,
	chunkIDs []chunk.ChunkID,
) ([]storeChunk, error) {
	var allChunks []storeChunk
	for _, storeID := range selectedStores {
		cm, _ := e.getStoreManagers(storeID)
		if cm == nil {
			continue
		}

		metas, err := cm.List()
		if err != nil {
			return nil, err
		}

		candidates := e.selectChunks(metas, q, chunkIDs)
		for _, meta := range candidates {
			allChunks = append(allChunks, storeChunk{storeID: storeID, meta: meta})
		}
	}
	return allChunks, nil
}

// validateResumeToken checks that all non-exhausted positions in the resume
// token reference chunks that still exist.
func validateResumeToken(resume *ResumeToken, allChunks []storeChunk) error {
	if resume == nil || len(resume.Positions) == 0 {
		return nil
	}

	available := make(map[chunk.ChunkID]bool, len(allChunks))
	for _, sc := range allChunks {
		available[sc.meta.ID] = true
	}

	for _, pos := range resume.Positions {
		if pos.Position == positionExhausted {
			continue
		}
		if !available[pos.ChunkID] {
			return ErrInvalidResumeToken
		}
	}
	return nil
}

// buildResumePositionMap converts a resume token into a nested map for
// efficient lookup during heap initialization.
func buildResumePositionMap(resume *ResumeToken) map[uuid.UUID]map[chunk.ChunkID]uint64 {
	m := make(map[uuid.UUID]map[chunk.ChunkID]uint64)
	if resume == nil {
		return m
	}
	for _, pos := range resume.Positions {
		if m[pos.StoreID] == nil {
			m[pos.StoreID] = make(map[chunk.ChunkID]uint64)
		}
		m[pos.StoreID][pos.ChunkID] = pos.Position
	}
	return m
}

// newMergeHeap creates a heap.Interface appropriate for the query direction.
func newMergeHeap(reverse bool, capacity int) heap.Interface {
	if reverse {
		rh := make(mergeHeapReverse, 0, capacity)
		return &rh
	}
	fh := make(mergeHeap, 0, capacity)
	return &fh
}

// lookupResumePosition returns the resume start position for a chunk, if any.
// Returns nil if no resume position exists. Sets the chunk as exhausted in
// chunkPositions and returns (nil, true) if the chunk was already exhausted.
func lookupResumePosition(
	resumePositions map[uuid.UUID]map[chunk.ChunkID]uint64,
	sc storeChunk,
	chunkPositions map[mergeKey]uint64,
) (startPos *uint64, exhausted bool) {
	storePositions, ok := resumePositions[sc.storeID]
	if !ok {
		return nil, false
	}
	pos, ok := storePositions[sc.meta.ID]
	if !ok {
		return nil, false
	}
	if pos == positionExhausted {
		chunkPositions[mergeKey{storeID: sc.storeID, chunkID: sc.meta.ID}] = positionExhausted
		return nil, true
	}
	return &pos, false
}

// resolveStartPosition finds the resume start position for a single chunk
// from a resume token. Returns nil if no position is found.
func resolveStartPosition(resume *ResumeToken, storeID uuid.UUID, chunkID chunk.ChunkID) *uint64 {
	if resume == nil {
		return nil
	}
	for _, pos := range resume.Positions {
		if pos.StoreID == storeID && pos.ChunkID == chunkID {
			return &pos.Position
		}
	}
	return nil
}

// searchSingleChunk handles the fast path when only one chunk matches.
// It returns (completed, shouldReturn). When shouldReturn is true the
// caller should return from the yield function.
func (e *Engine) searchSingleChunk(
	ctx context.Context,
	q Query,
	sc storeChunk,
	resume *ResumeToken,
	lastRefs *[]MultiStorePosition,
	yield func(chunk.Record, error) bool,
) (completed bool) {
	startPos := resolveStartPosition(resume, sc.storeID, sc.meta.ID)

	count := 0
	for rr, err := range e.searchChunkWithRef(ctx, q, sc.storeID, sc.meta, startPos) {
		if err != nil {
			*lastRefs = []MultiStorePosition{{StoreID: rr.StoreID, ChunkID: rr.Ref.ChunkID, Position: rr.Ref.Pos}}
			yield(chunk.Record{}, err)
			return false
		}

		*lastRefs = []MultiStorePosition{{StoreID: rr.StoreID, ChunkID: rr.Ref.ChunkID, Position: rr.Ref.Pos}}

		if !yield(rr.record(), nil) {
			return false
		}

		count++
		if q.Limit > 0 && count >= q.Limit {
			return false
		}
	}
	return true
}

// primeHeapWithResume opens iterators for each chunk, respecting resume
// positions, and pushes the first record from each onto the heap.
// Returns a non-nil error if any iterator fails on its first record.
func (e *Engine) primeHeapWithResume(
	ctx context.Context,
	q Query,
	allChunks []storeChunk,
	resumePositions map[uuid.UUID]map[chunk.ChunkID]uint64,
	ms *mergeState,
) error {
	for _, sc := range allChunks {
		startPos, exhausted := lookupResumePosition(resumePositions, sc, ms.chunkPositions)
		if exhausted {
			continue
		}

		if err := e.openAndPrimeScanner(ctx, q, sc, startPos, ms); err != nil {
			return err
		}
	}
	return nil
}

// primeHeap opens iterators for each chunk (no resume) and pushes the first
// record from each onto the heap. Returns a non-nil error if any iterator
// fails on its first record.
func (e *Engine) primeHeap(
	ctx context.Context,
	q Query,
	allChunks []storeChunk,
	ms *mergeState,
) error {
	for _, sc := range allChunks {
		if err := e.openAndPrimeScanner(ctx, q, sc, nil, ms); err != nil {
			return err
		}
	}
	return nil
}

// openAndPrimeScanner opens a single chunk iterator and pushes its first
// record onto the merge heap. If the chunk is immediately exhausted, it
// marks it in chunkPositions. Returns error if the first Next() call fails.
func (e *Engine) openAndPrimeScanner(
	ctx context.Context,
	q Query,
	sc storeChunk,
	startPos *uint64,
	ms *mergeState,
) error {
	iterSeq := e.searchChunkWithRef(ctx, q, sc.storeID, sc.meta, startPos)
	next, stop := iter.Pull2(iterSeq)

	rr, err, ok := next()
	if !ok {
		stop()
		ms.chunkPositions[mergeKey{storeID: sc.storeID, chunkID: sc.meta.ID}] = positionExhausted
		return nil
	}
	if err != nil {
		stop()
		return err
	}

	entry := &cursorEntry{
		storeID: sc.storeID,
		chunkID: sc.meta.ID,
		rec:     rr.Record,
		ref:     rr.Ref,
	}
	heap.Push(ms.h, entry)

	ms.scanners = append(ms.scanners, activeScanner{
		storeID: sc.storeID,
		chunkID: sc.meta.ID,
		iter:    next,
		stop:    stop,
	})
	return nil
}

// mergeLoopResult indicates why the merge loop exited.
type mergeLoopResult int

const (
	mergeCompleted   mergeLoopResult = iota // all records consumed
	mergeStopped                            // yield returned false or limit hit
	mergeError                              // context error or iterator error
)

// runMergeLoop pops entries from the heap, yields them, and advances scanners.
// count is the initial record count (for limit tracking).
// Returns the final count and the reason the loop exited.
func runMergeLoop(
	ctx context.Context,
	q Query,
	ms *mergeState,
	count int,
	yield func(chunk.Record, error) bool,
) (int, mergeLoopResult) {
	for ms.h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			ms.buildLastRefs()
			yield(chunk.Record{}, err)
			return count, mergeError
		}

		entry := heap.Pop(ms.h).(*cursorEntry)
		key := mergeKey{storeID: entry.storeID, chunkID: entry.chunkID}
		ms.chunkPositions[key] = entry.ref.Pos

		entry.rec.Ref = entry.ref
		entry.rec.StoreID = entry.storeID
		if !yield(entry.rec, nil) {
			ms.buildLastRefs()
			return count, mergeStopped
		}

		count++
		if q.Limit > 0 && count >= q.Limit {
			ms.buildLastRefs()
			return count, mergeStopped
		}

		if err, ok := ms.advanceScanner(entry); !ok {
			ms.buildLastRefs()
			yield(chunk.Record{}, err)
			return count, mergeError
		}
	}
	return count, mergeCompleted
}

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

	// Extract chunk predicates.
	chunkIDs, remainingExpr := ExtractChunkFilter(remainingExpr)

	// Normalize resume token to new format.
	// For single-store mode, use the first selected store as default.
	var defaultStoreID uuid.UUID
	if len(selectedStores) > 0 {
		defaultStoreID = selectedStores[0]
	} else if len(allStores) > 0 {
		defaultStoreID = allStores[0]
	}
	resume = resume.Normalize(defaultStoreID)
	if selectedStores == nil {
		selectedStores = allStores // no store filter means all stores
	}

	// Update query to use remaining expression (without store/chunk predicates).
	q.BoolExpr = remainingExpr

	// Track state for resume token generation.
	var lastRefs []MultiStorePosition
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		allChunks, err := e.collectStoreChunks(selectedStores, q, chunkIDs)
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}

		if len(allChunks) == 0 {
			completed = true
			return
		}

		// Validate resume token: all referenced chunks must exist.
		if err := validateResumeToken(resume, allChunks); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		// For single chunk, use simple iteration (no heap needed).
		if len(allChunks) == 1 {
			completed = e.searchSingleChunk(ctx, q, allChunks[0], resume, &lastRefs, yield)
			return
		}

		// Multiple chunks: use heap-based merge sort.
		resumePositions := buildResumePositionMap(resume)

		ms := &mergeState{
			h:              newMergeHeap(q.Reverse(), len(allChunks)),
			chunkPositions: make(map[mergeKey]uint64),
			lastRefs:       &lastRefs,
		}
		defer ms.cleanup()

		if err := e.primeHeapWithResume(ctx, q, allChunks, resumePositions, ms); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		_, result := runMergeLoop(ctx, q, ms, 0, yield)
		if result == mergeCompleted {
			completed = true
		}
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

	// Extract chunk predicates.
	chunkIDs, remainingExpr := ExtractChunkFilter(remainingExpr)

	// Update query to use remaining expression (without store/chunk predicates).
	q.BoolExpr = remainingExpr

	var lastRefs []MultiStorePosition
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		allChunks, err := e.collectStoreChunks(selectedStores, q, chunkIDs)
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}

		if len(allChunks) == 0 {
			completed = true
			return
		}

		// Create a follow query that removes all filters.
		followQuery := q
		followQuery.BoolExpr = nil

		ms := &mergeState{
			h:              newMergeHeap(q.Reverse(), len(allChunks)),
			chunkPositions: make(map[mergeKey]uint64),
			lastRefs:       &lastRefs,
		}
		defer ms.cleanup()

		// Phase 1: Find the first match using filtered query.
		if err := e.primeHeap(ctx, q, allChunks, ms); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		if ms.h.Len() == 0 {
			// No matches found.
			completed = true
			return
		}

		// Pop the first match from heap (oldest/newest depending on direction).
		firstMatch := heap.Pop(ms.h).(*cursorEntry)
		key := mergeKey{storeID: firstMatch.storeID, chunkID: firstMatch.chunkID}
		ms.chunkPositions[key] = firstMatch.ref.Pos

		if !yield(firstMatch.rec, nil) {
			ms.buildLastRefs()
			return
		}

		count := 1
		if q.Limit > 0 && count >= q.Limit {
			ms.buildLastRefs()
			return
		}

		// Phase 2: Follow mode - switch all scanners to unfiltered query.
		if err := e.reopenFollowScanners(ctx, followQuery, allChunks, firstMatch, ms); err != nil {
			ms.buildLastRefs()
			yield(chunk.Record{}, err)
			return
		}

		// Merge loop for follow phase.
		_, result := runMergeLoop(ctx, q, ms, count, yield)
		if result == mergeCompleted {
			completed = true
		}
	}

	nextToken := func() *ResumeToken {
		if completed || len(lastRefs) == 0 {
			return nil
		}
		return &ResumeToken{Positions: lastRefs}
	}

	return seq, nextToken
}

// reopenFollowScanners closes all existing scanners and reopens them with
// an unfiltered query, positioned after the first match. Used by SearchThenFollow
// to transition from the search phase to the follow phase.
func (e *Engine) reopenFollowScanners(
	ctx context.Context,
	followQuery Query,
	allChunks []storeChunk,
	firstMatch *cursorEntry,
	ms *mergeState,
) error {
	// Close all existing scanners.
	ms.cleanup()
	ms.scanners = nil

	// Clear heap.
	for ms.h.Len() > 0 {
		heap.Pop(ms.h)
	}

	firstMatchTS := firstMatch.rec.IngestTS

	for _, sc := range allChunks {
		key := mergeKey{storeID: sc.storeID, chunkID: sc.meta.ID}
		isFirstMatchChunk := key.storeID == firstMatch.storeID && key.chunkID == firstMatch.chunkID

		var startPos *uint64
		if isFirstMatchChunk {
			// This chunk had the first match - start from the match position.
			// searchChunkWithRef will skip this position (since startPos means "already returned"),
			// so we pass the match position itself, not position+1.
			startPos = &firstMatch.ref.Pos
		}

		rr, next, stop, ok, err := e.seekFollowPosition(ctx, followQuery, sc, startPos, isFirstMatchChunk, firstMatchTS)
		if err != nil {
			return err
		}
		if !ok {
			ms.chunkPositions[key] = positionExhausted
			continue
		}

		entry := &cursorEntry{
			storeID: sc.storeID,
			chunkID: sc.meta.ID,
			rec:     rr.Record,
			ref:     rr.Ref,
		}
		heap.Push(ms.h, entry)

		ms.scanners = append(ms.scanners, activeScanner{
			storeID: sc.storeID,
			chunkID: sc.meta.ID,
			iter:    next,
			stop:    stop,
		})
	}
	return nil
}

// seekFollowPosition opens an iterator for a chunk in follow mode and
// advances it past records at or before firstMatchTS (for non-first-match chunks).
// Returns the first valid record, the pull iterator functions, and whether
// a valid record was found.
func (e *Engine) seekFollowPosition(
	ctx context.Context,
	followQuery Query,
	sc storeChunk,
	startPos *uint64,
	isFirstMatchChunk bool,
	firstMatchTS time.Time,
) (recordWithRef, func() (recordWithRef, error, bool), func(), bool, error) {
	iterSeq := e.searchChunkWithRef(ctx, followQuery, sc.storeID, sc.meta, startPos)
	next, stop := iter.Pull2(iterSeq)

	for {
		rr, err, ok := next()
		if !ok {
			stop()
			return recordWithRef{}, nil, nil, false, nil
		}
		if err != nil {
			stop()
			return recordWithRef{}, nil, nil, false, err
		}

		// For the first-match chunk, we're already positioned correctly.
		if isFirstMatchChunk {
			return rr, next, stop, true, nil
		}

		// For other chunks, skip records at or before firstMatchTS.
		if rr.Record.IngestTS.After(firstMatchTS) {
			return rr, next, stop, true, nil
		}
		// Continue to next record.
	}
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

	// Extract store predicates once (the expression doesn't change).
	// storeFilter is nil when the query has no store= predicate (follow all).
	storeFilter, remainingExpr := ExtractStoreFilter(q.BoolExpr, nil)
	q.BoolExpr = remainingExpr

	return func(yield func(chunk.Record, error) bool) {
		fs := &followState{
			engine:        e,
			q:             q,
			storeFilter:   storeFilter,
			lastPositions: make(map[mergeKey]uint64),
			knownStores:   make(map[uuid.UUID]bool),
		}

		// Initialize positions for stores that exist right now.
		fs.resolveStores()

		fs.pollLoop(ctx, yield)
	}
}

// followState holds mutable state for the Follow polling loop.
type followState struct {
	engine        *Engine
	q             Query
	storeFilter   []uuid.UUID
	lastPositions map[mergeKey]uint64
	knownStores   map[uuid.UUID]bool
}

// initStorePositions marks all existing chunks in a store as seen,
// so Follow only yields records that arrive after this point.
func (fs *followState) initStorePositions(storeID uuid.UUID) {
	cm, _ := fs.engine.getStoreManagers(storeID)
	if cm == nil {
		return
	}
	metas, err := cm.List()
	if err != nil {
		return
	}
	for _, meta := range metas {
		key := mergeKey{storeID: storeID, chunkID: meta.ID}
		if meta.Sealed {
			fs.lastPositions[key] = positionExhausted
			continue
		}
		fs.initActiveChunkPosition(cm, storeID, meta)
	}
	fs.knownStores[storeID] = true
}

// initActiveChunkPosition scans an active (unsealed) chunk to find the last
// record position, so Follow starts from after existing records.
func (fs *followState) initActiveChunkPosition(cm chunk.ChunkManager, storeID uuid.UUID, meta chunk.ChunkMeta) {
	cursor, err := cm.OpenCursor(meta.ID)
	if err != nil {
		return
	}
	defer func() { _ = cursor.Close() }()

	hasRecords := false
	var lastPos uint64
	for {
		_, ref, err := cursor.Next()
		if err != nil {
			break
		}
		hasRecords = true
		lastPos = ref.Pos
	}
	if hasRecords {
		fs.lastPositions[mergeKey{storeID: storeID, chunkID: meta.ID}] = lastPos
	}
}

// resolveStores returns the stores to poll this iteration.
// When no store= predicate exists, it re-evaluates the live store
// list each call, initializing positions for any newly discovered store.
func (fs *followState) resolveStores() []uuid.UUID {
	stores := fs.storeFilter
	if stores == nil {
		stores = fs.engine.listStores()
	}
	for _, id := range stores {
		if !fs.knownStores[id] {
			fs.initStorePositions(id)
		}
	}
	return stores
}

// pollLoop is the main Follow polling loop. It repeatedly collects new
// records from all stores, sorts them by timestamp, and yields them.
func (fs *followState) pollLoop(ctx context.Context, yield func(chunk.Record, error) bool) {
	const pollInterval = 100 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		selectedStores := fs.resolveStores()

		pending := fs.collectNewRecords(selectedStores, yield)
		if pending == nil {
			// yield returned false during error handling; caller wants to stop.
			return
		}

		slices.SortFunc(pending, func(a, b pendingRecord) int {
			return a.rec.IngestTS.Compare(b.rec.IngestTS)
		})

		if !fs.yieldPending(pending, yield) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

// collectNewRecords scans all selected stores for records newer than
// the last seen positions. Returns nil if yield returned false (caller stop).
func (fs *followState) collectNewRecords(
	selectedStores []uuid.UUID,
	yield func(chunk.Record, error) bool,
) []pendingRecord {
	var pending []pendingRecord

	for _, storeID := range selectedStores {
		cm, _ := fs.engine.getStoreManagers(storeID)
		if cm == nil {
			continue
		}

		metas, err := cm.List()
		if err != nil {
			if !yield(chunk.Record{}, err) {
				return nil
			}
			continue
		}

		for _, meta := range metas {
			fs.collectChunkRecords(cm, storeID, meta, &pending)
		}
	}
	return pending
}

// collectChunkRecords reads new records from a single chunk, appending
// them to pending. Records already seen (based on lastPositions) are skipped.
func (fs *followState) collectChunkRecords(
	cm chunk.ChunkManager,
	storeID uuid.UUID,
	meta chunk.ChunkMeta,
	pending *[]pendingRecord,
) {
	key := mergeKey{storeID: storeID, chunkID: meta.ID}

	if lastPos, ok := fs.lastPositions[key]; ok && lastPos == positionExhausted {
		return
	}

	cursor, err := cm.OpenCursor(meta.ID)
	if err != nil {
		return
	}
	defer func() { _ = cursor.Close() }()

	if !fs.seekPastSeen(cursor, key, meta.ID) {
		return
	}

	for {
		rec, ref, err := cursor.Next()
		if err != nil {
			break
		}

		if fs.q.BoolExpr != nil && !fs.engine.matchesFilter(rec, fs.q) {
			fs.lastPositions[key] = ref.Pos
			continue
		}

		*pending = append(*pending, pendingRecord{
			storeID: storeID,
			rec:     rec,
			ref:     ref,
		})
		fs.lastPositions[key] = ref.Pos
	}
}

// seekPastSeen positions the cursor past already-seen records.
// Returns false if the cursor could not be positioned (should skip this chunk).
func (fs *followState) seekPastSeen(cursor chunk.RecordCursor, key mergeKey, chunkID chunk.ChunkID) bool {
	lastPos, ok := fs.lastPositions[key]
	if !ok {
		return true
	}
	if err := cursor.Seek(chunk.RecordRef{ChunkID: chunkID, Pos: lastPos}); err != nil {
		return false
	}
	// Skip the record at lastPos (already yielded).
	if _, _, err := cursor.Next(); err != nil {
		return false
	}
	return true
}

// yieldPending yields sorted pending records. Returns false if yield
// returned false (caller wants to stop).
func (fs *followState) yieldPending(pending []pendingRecord, yield func(chunk.Record, error) bool) bool {
	for _, p := range pending {
		p.rec.Ref = p.ref
		p.rec.StoreID = p.storeID
		if !yield(p.rec, nil) {
			return false
		}
	}
	return true
}

// matchesFilter checks if a record matches the query's boolean expression.
func (e *Engine) matchesFilter(rec chunk.Record, q Query) bool {
	if q.BoolExpr == nil {
		return true
	}
	dnf := querylang.ToDNF(q.BoolExpr)
	return dnfFilter(&dnf)(rec)
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

	// Extract chunk predicates.
	chunkIDs, remainingExpr := ExtractChunkFilter(q.BoolExpr)
	q.BoolExpr = remainingExpr

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
		allChunks := e.selectChunks(metas, q, chunkIDs)

		// Also need all chunks sorted ascending for context gathering.
		allChunksAsc := make([]chunk.ChunkMeta, len(metas))
		copy(allChunksAsc, metas)
		slices.SortFunc(allChunksAsc, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS)
		})

		cs := &contextSearchState{
			engine:       e,
			ctx:          ctx,
			q:            q,
			allChunksAsc: allChunksAsc,
			nextRef:      &nextRef,
		}

		for _, meta := range allChunks {
			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			if !cs.processChunk(meta, yield) {
				return
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

// contextSearchState holds mutable state for SearchWithContext's inner loop.
type contextSearchState struct {
	engine       *Engine
	ctx          context.Context
	q            Query
	allChunksAsc []chunk.ChunkMeta
	nextRef      **chunk.RecordRef
	count        int
}

// processChunk iterates over matches in a single chunk, yielding each match
// with its surrounding context records. Returns false if iteration should stop.
func (cs *contextSearchState) processChunk(meta chunk.ChunkMeta, yield func(chunk.Record, error) bool) bool {
	for rr, err := range cs.engine.searchChunkWithRef(cs.ctx, cs.q, uuid.UUID{}, meta, nil) {
		if err != nil {
			*cs.nextRef = &rr.Ref
			yield(chunk.Record{}, err)
			return false
		}

		n, stopped := cs.engine.yieldContextBefore(cs.ctx, cs.q, cs.allChunksAsc, rr, cs.count, cs.nextRef, yield)
		if stopped {
			return false
		}
		cs.count = n

		*cs.nextRef = &rr.Ref
		if !yield(rr.record(), nil) {
			return false
		}
		cs.count++
		if cs.q.Limit > 0 && cs.count >= cs.q.Limit {
			return false
		}

		n, stopped = cs.engine.yieldContextAfter(cs.ctx, cs.q, cs.allChunksAsc, rr, cs.count, yield)
		if stopped {
			return false
		}
		cs.count = n
	}
	return true
}

// yieldContextBefore gathers and yields context records before a match.
// Returns the updated count and whether iteration should stop.
func (e *Engine) yieldContextBefore(
	ctx context.Context,
	q Query,
	allChunksAsc []chunk.ChunkMeta,
	rr recordWithRef,
	count int,
	nextRef **chunk.RecordRef,
	yield func(chunk.Record, error) bool,
) (int, bool) {
	if q.ContextBefore <= 0 {
		return count, false
	}

	beforeRecs, err := e.gatherContextRecords(ctx, allChunksAsc, rr.Ref, q.ContextBefore, q.Reverse(), true)
	if err != nil {
		yield(chunk.Record{}, err)
		return count, true
	}

	for _, rec := range beforeRecs {
		if !yield(rec, nil) {
			ref := rr.Ref
			*nextRef = &ref
			return count, true
		}
		count++
		if q.Limit > 0 && count >= q.Limit {
			ref := rr.Ref
			*nextRef = &ref
			return count, true
		}
	}
	return count, false
}

// yieldContextAfter gathers and yields context records after a match.
// Returns the updated count and whether iteration should stop.
func (e *Engine) yieldContextAfter(
	ctx context.Context,
	q Query,
	allChunksAsc []chunk.ChunkMeta,
	rr recordWithRef,
	count int,
	yield func(chunk.Record, error) bool,
) (int, bool) {
	if q.ContextAfter <= 0 {
		return count, false
	}

	afterRecs, err := e.gatherContextRecords(ctx, allChunksAsc, rr.Ref, q.ContextAfter, q.Reverse(), false)
	if err != nil {
		yield(chunk.Record{}, err)
		return count, true
	}

	for _, rec := range afterRecs {
		if !yield(rec, nil) {
			return count, true
		}
		count++
		if q.Limit > 0 && count >= q.Limit {
			return count, true
		}
	}
	return count, false
}

// gatherContextRecords gathers context records either before or after an anchor.
// For "before" context (isBefore=true):
//   - forward mode: gathers chronologically older records
//   - reverse mode: gathers chronologically newer records ("before" in iteration order)
//
// For "after" context (isBefore=false):
//   - forward mode: gathers chronologically newer records
//   - reverse mode: gathers chronologically older records ("after" in iteration order)
func (e *Engine) gatherContextRecords(
	ctx context.Context,
	chunksAsc []chunk.ChunkMeta,
	anchor chunk.RecordRef,
	n int,
	reverse bool,
	isBefore bool,
) ([]chunk.Record, error) {
	// In reverse mode, the direction is inverted:
	// "before" in iteration order = chronologically after
	// "after" in iteration order = chronologically before
	if isBefore != reverse {
		return e.gatherContextBefore(ctx, chunksAsc, anchor, n, reverse)
	}
	return e.gatherContextAfter(ctx, chunksAsc, anchor, n, reverse)
}
