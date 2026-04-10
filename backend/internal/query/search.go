package query

import (
	"container/heap"
	"context"
	"errors"
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

// vaultChunk pairs a vault ID with its chunk metadata.
type vaultChunk struct {
	vaultID uuid.UUID
	meta    chunk.ChunkMeta
}

// mergeKey uniquely identifies a chunk within a vault.
type mergeKey struct {
	vaultID uuid.UUID
	chunkID chunk.ChunkID
}

// activeScanner tracks an open iterator for a chunk during merge operations.
type activeScanner struct {
	vaultID uuid.UUID
	chunkID chunk.ChunkID
	iter    func() (recordWithRef, error, bool)
	stop    func()
}

// pendingRecord holds a record collected during Follow polling.
type pendingRecord struct {
	vaultID uuid.UUID
	rec     chunk.Record
	ref     chunk.RecordRef
}

// mergeState holds shared mutable state for heap-based merge operations.
// It is used by Search, SearchThenFollow, and their helper methods.
type mergeState struct {
	h              heap.Interface
	scanners       []activeScanner
	chunkPositions map[mergeKey]uint64
	chunkResumeTS  map[mergeKey]time.Time // IngestTS-based resume for reordered chunks
	lastRefs       *[]MultiVaultPosition
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
	refs := make([]MultiVaultPosition, 0, len(ms.chunkPositions))
	for key, pos := range ms.chunkPositions {
		mvp := MultiVaultPosition{
			VaultID:  key.vaultID,
			ChunkID:  key.chunkID,
			Position: pos,
		}
		if ts, ok := ms.chunkResumeTS[key]; ok {
			mvp.ResumeTS = ts
		}
		refs = append(refs, mvp)
	}
	*ms.lastRefs = refs
}

// findScanner looks up a scanner by vault and chunk ID.
func (ms *mergeState) findScanner(vaultID uuid.UUID, chunkID chunk.ChunkID) *activeScanner {
	for i := range ms.scanners {
		if ms.scanners[i].vaultID == vaultID && ms.scanners[i].chunkID == chunkID {
			return &ms.scanners[i]
		}
	}
	return nil
}

// advanceScanner advances the scanner for the given entry, pushing the next
// record onto the heap or marking the chunk as exhausted.
// Returns (error, false) if advancing produced an error; (nil, true) otherwise.
func (ms *mergeState) advanceScanner(entry *cursorEntry) (error, bool) {
	key := mergeKey{vaultID: entry.vaultID, chunkID: entry.chunkID}
	scanner := ms.findScanner(entry.vaultID, entry.chunkID)
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
	entry.reordered = rr.Reordered
	heap.Push(ms.h, entry)
	return nil, true
}

// collectVaultChunks gathers chunks from selected vaults that overlap the query.
// Returns the matching chunks and the count of archived chunks that were skipped.
func (e *Engine) collectVaultChunks(
	selectedVaults []uuid.UUID,
	q Query,
	chunkIDs []chunk.ChunkID,
) ([]vaultChunk, int32, error) {
	var allChunks []vaultChunk
	var archivedCount int32
	for _, vaultID := range selectedVaults {
		cm, _ := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}

		metas, err := cm.List()
		if err != nil {
			return nil, 0, err
		}

		for _, meta := range metas {
			if meta.Archived {
				archivedCount++
			}
		}

		candidates := e.selectChunks(metas, q, chunkIDs)
		for _, meta := range candidates {
			allChunks = append(allChunks, vaultChunk{vaultID: vaultID, meta: meta})
		}
	}
	return allChunks, archivedCount, nil
}

// validateResumeToken checks that all non-exhausted positions in the resume
// token reference chunks that still exist.
func validateResumeToken(resume *ResumeToken, allChunks []vaultChunk) error {
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
type resumeInfo struct {
	Position uint64
	ResumeTS time.Time // non-zero for reordered chunks
}

func buildResumeMap(resume *ResumeToken) map[uuid.UUID]map[chunk.ChunkID]resumeInfo {
	m := make(map[uuid.UUID]map[chunk.ChunkID]resumeInfo)
	if resume == nil {
		return m
	}
	for _, pos := range resume.Positions {
		if m[pos.VaultID] == nil {
			m[pos.VaultID] = make(map[chunk.ChunkID]resumeInfo)
		}
		m[pos.VaultID][pos.ChunkID] = resumeInfo{
			Position: pos.Position,
			ResumeTS: pos.ResumeTS,
		}
	}
	return m
}

// newMergeHeap creates a heap.Interface appropriate for the query direction and ordering.
func newMergeHeap(q Query, capacity int) heap.Interface {
	return newTSHeap(q.OrderBy, q.Reverse(), capacity)
}

// lookupResumeInfo returns the resume info for a chunk, if any.
// Returns nil if no resume info exists. Sets the chunk as exhausted in
// chunkPositions and returns (nil, true) if the chunk was already exhausted.
func lookupResumeInfo(
	resumeMap map[uuid.UUID]map[chunk.ChunkID]resumeInfo,
	sc vaultChunk,
	chunkPositions map[mergeKey]uint64,
) (info *resumeInfo, exhausted bool) {
	vaultEntries, ok := resumeMap[sc.vaultID]
	if !ok {
		return nil, false
	}
	ri, ok := vaultEntries[sc.meta.ID]
	if !ok {
		return nil, false
	}
	if ri.Position == positionExhausted {
		chunkPositions[mergeKey{vaultID: sc.vaultID, chunkID: sc.meta.ID}] = positionExhausted
		return nil, true
	}
	return &ri, false
}

// resolveStartPosition finds the resume start position for a single chunk
// from a resume token. Returns nil if no position is found.
func resolveStartPosition(resume *ResumeToken, vaultID uuid.UUID, chunkID chunk.ChunkID) *uint64 {
	if resume == nil {
		return nil
	}
	for _, pos := range resume.Positions {
		if pos.VaultID == vaultID && pos.ChunkID == chunkID {
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
	sc vaultChunk,
	resume *ResumeToken,
	lastRefs *[]MultiVaultPosition,
	yield func(chunk.Record, error) bool,
) (completed bool) {
	startPos := resolveStartPosition(resume, sc.vaultID, sc.meta.ID)

	count := 0
	for rr, err := range e.searchChunkWithRef(ctx, q, sc.vaultID, sc.meta, startPos) {
		if err != nil {
			*lastRefs = []MultiVaultPosition{{VaultID: rr.VaultID, ChunkID: rr.Ref.ChunkID, Position: rr.Ref.Pos}}
			yield(chunk.Record{}, err)
			return false
		}

		*lastRefs = []MultiVaultPosition{{VaultID: rr.VaultID, ChunkID: rr.Ref.ChunkID, Position: rr.Ref.Pos}}

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
	allChunks []vaultChunk,
	resumeMap map[uuid.UUID]map[chunk.ChunkID]resumeInfo,
	ms *mergeState,
) error {
	for _, sc := range allChunks {
		ri, exhausted := lookupResumeInfo(resumeMap, sc, ms.chunkPositions)
		if exhausted {
			continue
		}
		err := e.primeChunkWithResume(ctx, q, sc, ri, ms)
		if err == nil {
			continue
		}
		// Cloud chunks may be unreadable (corrupt blob, S3 error).
		// Skip them with a warning rather than aborting the entire search.
		if sc.meta.CloudBacked {
			if err := e.handleCloudPrimeError(ctx, err, sc, ms); err != nil {
				return err
			}
			continue
		}
		return err
	}
	return nil
}

// primeChunkWithResume opens a scanner for a single chunk, handling resume
// position or ResumeTS if present.
func (e *Engine) primeChunkWithResume(ctx context.Context, q Query, sc vaultChunk, ri *resumeInfo, ms *mergeState) error {
	if ri != nil && !ri.ResumeTS.IsZero() {
		resumeQ := q
		resumeQ.ResumeTS = ri.ResumeTS
		return e.openAndPrimeScanner(ctx, resumeQ, sc, nil, ms)
	}
	var startPos *uint64
	if ri != nil {
		startPos = &ri.Position
	}
	return e.openAndPrimeScanner(ctx, q, sc, startPos, ms)
}

// primeHeap opens iterators for each chunk (no resume) and pushes the first
// record from each onto the heap. Returns a non-nil error if any iterator
// fails on its first record.
func (e *Engine) primeHeap(
	ctx context.Context,
	q Query,
	allChunks []vaultChunk,
	ms *mergeState,
) error {
	for _, sc := range allChunks {
		if sc.meta.CloudBacked {
			if err := e.primeCloudChunk(ctx, q, sc, ms); err != nil {
				return err
			}
			continue
		}
		if err := e.openAndPrimeScanner(ctx, q, sc, nil, ms); err != nil {
			return err
		}
	}
	return nil
}

// primeCloudChunk primes a cloud-backed chunk, skipping it with a warning
// if the S3 blob is unreadable (corrupt, truncated, etc.). Context errors
// still propagate.
func (e *Engine) primeCloudChunk(ctx context.Context, q Query, sc vaultChunk, ms *mergeState) error {
	if err := e.openAndPrimeScanner(ctx, q, sc, nil, ms); err != nil {
		return e.handleCloudPrimeError(ctx, err, sc, ms)
	}
	return nil
}

// handleCloudPrimeError handles an error from priming a cloud-backed chunk.
// Context errors propagate; chunkReadErrors mark the chunk as exhausted and
// log a warning; all other errors propagate.
func (e *Engine) handleCloudPrimeError(ctx context.Context, err error, sc vaultChunk, ms *mergeState) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var cre *chunkReadError
	if errors.As(err, &cre) {
		if e.logger != nil {
			e.logger.Warn("search: skipping unreadable cloud chunk",
				"vault", cre.vaultID, "chunk", cre.chunkID, "error", cre.err)
		}
		ms.chunkPositions[mergeKey{vaultID: sc.vaultID, chunkID: sc.meta.ID}] = positionExhausted
		return nil
	}
	return err
}

// chunkReadError wraps a data access error for a specific chunk, allowing
// callers to skip individual unreadable chunks without aborting the search.
type chunkReadError struct {
	vaultID uuid.UUID
	chunkID chunk.ChunkID
	err     error
}

func (e *chunkReadError) Error() string {
	return e.err.Error()
}

func (e *chunkReadError) Unwrap() error {
	return e.err
}

// openAndPrimeScanner opens a single chunk iterator and pushes its first
// record onto the merge heap. If the chunk is immediately exhausted, it
// marks it in chunkPositions. Returns error if the first Next() call fails.
// Cloud-backed chunks that fail to open are skipped (logged by the caller)
// rather than aborting the entire search — one corrupted S3 blob shouldn't
// prevent reading all other chunks.
func (e *Engine) openAndPrimeScanner(
	ctx context.Context,
	q Query,
	sc vaultChunk,
	startPos *uint64,
	ms *mergeState,
) error {
	iterSeq := e.searchChunkWithRef(ctx, q, sc.vaultID, sc.meta, startPos)
	next, stop := iter.Pull2(iterSeq)

	rr, err, ok := next()
	if !ok {
		stop()
		ms.chunkPositions[mergeKey{vaultID: sc.vaultID, chunkID: sc.meta.ID}] = positionExhausted
		return nil
	}
	if err != nil {
		stop()
		// Context errors must propagate — everything else is a data access
		// failure that should skip this chunk, not abort the search.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &chunkReadError{vaultID: sc.vaultID, chunkID: sc.meta.ID, err: err}
	}

	entry := &cursorEntry{
		vaultID:   sc.vaultID,
		chunkID:   sc.meta.ID,
		rec:       rr.Record,
		ref:       rr.Ref,
		reordered: rr.Reordered,
	}
	heap.Push(ms.h, entry)

	ms.scanners = append(ms.scanners, activeScanner{
		vaultID: sc.vaultID,
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
		key := mergeKey{vaultID: entry.vaultID, chunkID: entry.chunkID}
		if entry.reordered {
			ms.chunkResumeTS[key] = entry.rec.IngestTS
		}
		ms.chunkPositions[key] = entry.ref.Pos

		entry.rec.Ref = entry.ref
		entry.rec.VaultID = entry.vaultID

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

// Search returns an iterator over records matching the query, ordered by write timestamp.
// The iterator yields (record, nil) for each match, or (zero, err) on error.
// After yielding an error, iteration stops.
//
// For multi-vault engines, this searches across all vaults (or vaults matching
// vault_id=X predicates in the query) and merge-sorts results by WriteTS.
//
// The resume parameter allows continuing from a previous search. Pass nil to start fresh.
// The returned nextToken function returns a ResumeToken if iteration stopped early
// (limit reached, caller break, error, or context cancellation), or nil if all
// matching records were returned.
func (e *Engine) Search(ctx context.Context, q Query, resume *ResumeToken) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	// Normalize query to ensure BoolExpr is set (converts legacy Tokens/KV if needed).
	q = q.Normalize()

	// Extract vault predicates and get remaining query expression.
	allVaults := e.listVaults()
	selectedVaults, remainingExpr := ExtractVaultFilter(q.BoolExpr, allVaults)

	// Extract chunk predicates.
	chunkIDs, remainingExpr := ExtractChunkFilter(remainingExpr)

	// Normalize resume token to new format.
	// For single-vault mode, use the first selected vault as default.
	var defaultVaultID uuid.UUID
	if len(selectedVaults) > 0 {
		defaultVaultID = selectedVaults[0]
	} else if len(allVaults) > 0 {
		defaultVaultID = allVaults[0]
	}
	resume = resume.Normalize(defaultVaultID)
	if selectedVaults == nil {
		selectedVaults = allVaults // no vault filter means all vaults
	}

	// Update query to use remaining expression (without vault/chunk predicates).
	q.BoolExpr = remainingExpr

	// Track state for resume token generation.
	var lastRefs []MultiVaultPosition
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		allChunks, _, err := e.collectVaultChunks(selectedVaults, q, chunkIDs)
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
		resumePositions := buildResumeMap(resume)

		ms := &mergeState{
			h:              newMergeHeap(q, len(allChunks)),
			chunkPositions: make(map[mergeKey]uint64),
			chunkResumeTS:  make(map[mergeKey]time.Time),
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
// For multi-vault engines, this searches across all vaults (or vaults matching
// vault_id=X predicates in the query) and merge-sorts results by WriteTS.
func (e *Engine) SearchThenFollow(ctx context.Context, q Query, resume *ResumeToken) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	// Normalize query to ensure BoolExpr is set (converts legacy Tokens/KV if needed).
	q = q.Normalize()

	// Extract vault predicates and get remaining query expression.
	allVaults := e.listVaults()
	selectedVaults, remainingExpr := ExtractVaultFilter(q.BoolExpr, allVaults)
	if selectedVaults == nil {
		selectedVaults = allVaults
	}

	// Extract chunk predicates.
	chunkIDs, remainingExpr := ExtractChunkFilter(remainingExpr)

	// Update query to use remaining expression (without vault/chunk predicates).
	q.BoolExpr = remainingExpr

	var lastRefs []MultiVaultPosition
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		allChunks, _, err := e.collectVaultChunks(selectedVaults, q, chunkIDs)
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
			h:              newMergeHeap(q, len(allChunks)),
			chunkPositions: make(map[mergeKey]uint64),
			chunkResumeTS:  make(map[mergeKey]time.Time),
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
		key := mergeKey{vaultID: firstMatch.vaultID, chunkID: firstMatch.chunkID}
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
	allChunks []vaultChunk,
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

	firstMatchTS := firstMatch.rec.WriteTS

	for _, sc := range allChunks {
		key := mergeKey{vaultID: sc.vaultID, chunkID: sc.meta.ID}
		isFirstMatchChunk := key.vaultID == firstMatch.vaultID && key.chunkID == firstMatch.chunkID

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
			vaultID: sc.vaultID,
			chunkID: sc.meta.ID,
			rec:     rr.Record,
			ref:     rr.Ref,
		}
		heap.Push(ms.h, entry)

		ms.scanners = append(ms.scanners, activeScanner{
			vaultID: sc.vaultID,
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
	sc vaultChunk,
	startPos *uint64,
	isFirstMatchChunk bool,
	firstMatchTS time.Time,
) (recordWithRef, func() (recordWithRef, error, bool), func(), bool, error) {
	iterSeq := e.searchChunkWithRef(ctx, followQuery, sc.vaultID, sc.meta, startPos)
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
		if rr.Record.WriteTS.After(firstMatchTS) {
			return rr, next, stop, true, nil
		}
		// Continue to next record.
	}
}

// Follow tails records from all vaults, waiting for new arrivals.
// It first yields any existing records matching the query (optionally filtered),
// then continuously polls for new records until the context is cancelled.
//
// Unlike SearchThenFollow, this method never completes on its own - it keeps
// polling for new records until ctx is cancelled.
//
// For multi-vault engines, records are merged by WriteTS across all vaults.
func (e *Engine) Follow(ctx context.Context, q Query) iter.Seq2[chunk.Record, error] {
	// Normalize query to ensure BoolExpr is set.
	q = q.Normalize()

	// Extract vault predicates once (the expression doesn't change).
	// vaultFilter is nil when the query has no vault_id= predicate (follow all).
	vaultFilter, remainingExpr := ExtractVaultFilter(q.BoolExpr, nil)
	q.BoolExpr = remainingExpr

	return func(yield func(chunk.Record, error) bool) {
		fs := &followState{
			engine:        e,
			q:             q,
			vaultFilter:   vaultFilter,
			lastPositions: make(map[mergeKey]uint64),
			knownVaults:   make(map[uuid.UUID]bool),
		}

		// Initialize positions for vaults that exist right now.
		fs.resolveVaults()

		fs.pollLoop(ctx, yield)
	}
}

// followState holds mutable state for the Follow polling loop.
type followState struct {
	engine        *Engine
	q             Query
	vaultFilter   []uuid.UUID
	lastPositions map[mergeKey]uint64
	knownVaults   map[uuid.UUID]bool
}

// initVaultPositions marks all existing chunks in a vault as seen,
// so Follow only yields records that arrive after this point.
func (fs *followState) initVaultPositions(vaultID uuid.UUID) {
	cm, _ := fs.engine.getVaultManagers(vaultID)
	if cm == nil {
		return
	}
	metas, err := cm.List()
	if err != nil {
		return
	}
	for _, meta := range metas {
		key := mergeKey{vaultID: vaultID, chunkID: meta.ID}
		if meta.Sealed {
			fs.lastPositions[key] = positionExhausted
			continue
		}
		fs.initActiveChunkPosition(cm, vaultID, meta)
	}
	fs.knownVaults[vaultID] = true
}

// initActiveChunkPosition scans an active (unsealed) chunk to find the last
// record position, so Follow starts from after existing records.
func (fs *followState) initActiveChunkPosition(cm chunk.ChunkManager, vaultID uuid.UUID, meta chunk.ChunkMeta) {
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
		fs.lastPositions[mergeKey{vaultID: vaultID, chunkID: meta.ID}] = lastPos
	}
}

// resolveVaults returns the vaults to poll this iteration.
// When no vault_id= predicate exists, it re-evaluates the live vault
// list each call, initializing positions for any newly discovered vault.
func (fs *followState) resolveVaults() []uuid.UUID {
	vaults := fs.vaultFilter
	if vaults == nil {
		vaults = fs.engine.listVaults()
	}
	for _, id := range vaults {
		if !fs.knownVaults[id] {
			fs.initVaultPositions(id)
		}
	}
	return vaults
}

// pollLoop is the main Follow polling loop. It repeatedly collects new
// records from all vaults, sorts them by timestamp, and yields them.
func (fs *followState) pollLoop(ctx context.Context, yield func(chunk.Record, error) bool) {
	const pollInterval = 100 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		selectedVaults := fs.resolveVaults()

		pending := fs.collectNewRecords(selectedVaults, yield)
		if pending == nil {
			// yield returned false during error handling; caller wants to stop.
			return
		}

		slices.SortFunc(pending, func(a, b pendingRecord) int {
			return a.rec.WriteTS.Compare(b.rec.WriteTS)
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

// collectNewRecords scans all selected vaults for records newer than
// the last seen positions. Returns nil if yield returned false (caller stop).
func (fs *followState) collectNewRecords(
	selectedVaults []uuid.UUID,
	yield func(chunk.Record, error) bool,
) []pendingRecord {
	pending := []pendingRecord{} // non-nil empty; nil is reserved for yield-returned-false

	for _, vaultID := range selectedVaults {
		cm, _ := fs.engine.getVaultManagers(vaultID)
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
			fs.collectChunkRecords(cm, vaultID, meta, &pending)
		}
	}
	return pending
}

// collectChunkRecords reads new records from a single chunk, appending
// them to pending. Records already seen (based on lastPositions) are skipped.
func (fs *followState) collectChunkRecords(
	cm chunk.ChunkManager,
	vaultID uuid.UUID,
	meta chunk.ChunkMeta,
	pending *[]pendingRecord,
) {
	key := mergeKey{vaultID: vaultID, chunkID: meta.ID}

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
			vaultID: vaultID,
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

// yieldPending yields sorted pending records.
// Returns false if yield returned false (caller wants to stop).
func (fs *followState) yieldPending(pending []pendingRecord, yield func(chunk.Record, error) bool) bool {
	for _, p := range pending {
		p.rec.Ref = p.ref
		p.rec.VaultID = p.vaultID

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
	// Multi-vault mode not yet supported for SearchWithContext.
	if e.isMultiVault() {
		return func(yield func(chunk.Record, error) bool) {
			yield(chunk.Record{}, ErrMultiVaultNotSupported)
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

		// Sort all chunks by WriteStart for consistent ordering.
		allChunks := e.selectChunks(metas, q, chunkIDs)

		// Also need all chunks sorted ascending for context gathering.
		allChunksAsc := make([]chunk.ChunkMeta, len(metas))
		copy(allChunksAsc, metas)
		slices.SortFunc(allChunksAsc, func(a, b chunk.ChunkMeta) int {
			return a.WriteStart.Compare(b.WriteStart)
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
