package file

import (
	"context"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/chunk/glcb"
	"gastrolog/internal/tsindex"
)

// Cold-chunk rank resolution: a cloud-backed chunk whose warm cache is gone
// can still answer ingest-rank lookups exactly, by fetching just its ITSI
// section from the object via range GETs (glcb.FetchRemoteSection). This is
// what lets histogram counts for cold chunks be exact instead of estimated.
//
// The whole-blob no-fetch policy stands: this path never downloads the
// object, only its TS-index section — KB-scale, bounded, and cached so a
// histogram window's many bucket probes pay for one fetch.

// remoteTSViewBudget bounds the memory the fetched-ITSI cache may hold.
// An ITSI is 12 bytes per record, so the budget admits on the order of a
// thousand 10k-record chunks — far more cold chunks than one histogram
// window spans — while staying irrelevant next to the process's mmap
// footprint. Refetching an evicted entry costs two small GETs, so being
// wrong here is cheap in both directions.
const remoteTSViewBudget = 16 << 20

// remoteTSView is one cached fetched section: the view plus the size of the
// raw bytes backing it, for budget accounting.
type remoteTSView struct {
	view  tsindex.View
	bytes int64
}

// coldIngestRank resolves an ingest-rank lookup for a cold cloud-backed
// chunk, or reports a miss. Misses are deliberate degradations — no cloud
// store, chunk not cloud-backed, warm copy present (the mmap path serves
// it), archived object, transient fetch failure — and the caller's contract
// (0, false, nil) sends the histogram back to its labeled estimate.
func (m *Manager) coldIngestRank(id chunk.ChunkID, ts time.Time) (uint64, bool) {
	if m.cfg.CloudStore == nil || m.cloudIdx == nil || m.HasLocalContent(id) {
		return 0, false
	}
	m.cloudIdxMu.Lock()
	meta, cloudBacked := m.cloudIdx.Lookup(id)
	m.cloudIdxMu.Unlock()
	if !cloudBacked || meta.cloudBytes <= 0 {
		// Without the object's size there is no tail to range-read; the
		// size lands in the cloud index at upload/adoption time.
		return 0, false
	}

	view, ok := m.remoteTSViewFor(id, meta.cloudBytes)
	if !ok {
		return 0, false
	}
	rank, _, found := view.SearchTS(ts.UnixNano())
	if !found {
		return 0, false
	}
	return uint64(rank), true
}

// remoteTSViewFor returns the chunk's fetched ITSI view, fetching and caching
// it on first use. The per-chunk lock dedups concurrent fetches for one chunk
// without serializing fetches across chunks.
func (m *Manager) remoteTSViewFor(id chunk.ChunkID, objSize int64) (tsindex.View, bool) {
	m.remoteTSMu.Lock()
	cached, hit := m.remoteTS[id]
	m.remoteTSMu.Unlock()
	if hit {
		return cached.view, true
	}

	chunkLock := m.chunkLockFor(id)
	chunkLock.Lock()
	defer chunkLock.Unlock()
	m.remoteTSMu.Lock()
	if cached, hit := m.remoteTS[id]; hit {
		m.remoteTSMu.Unlock()
		return cached.view, true
	}
	m.remoteTSMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), cloudDownloadTimeout)
	defer cancel()
	entry, data, err := glcb.FetchRemoteSection(ctx, m.cfg.CloudStore, m.blobKey(id), objSize, glcb.SectionIngestTSIndex)
	if err != nil {
		// Archived objects and transient store trouble land here; both are
		// ordinary for cold data, so this is a Debug, not an alarm.
		m.logger.Debug("cold rank: remote ITSI unavailable", "chunk", id, "error", err)
		return nil, false
	}
	viewAny, err := glcb.DefaultRegistry().NewView(entry, data)
	if err != nil {
		m.logger.Warn("cold rank: remote ITSI section undecodable", "chunk", id, "error", err)
		return nil, false
	}
	view, isTS := viewAny.(tsindex.View)
	if !isTS {
		m.logger.Warn("cold rank: remote ITSI decoded to unexpected view", "chunk", id)
		return nil, false
	}

	m.remoteTSMu.Lock()
	if m.remoteTS == nil {
		m.remoteTS = make(map[chunk.ChunkID]remoteTSView)
	}
	// Over budget: drop arbitrary entries. Every entry costs the same two
	// small GETs to refetch, so recency bookkeeping buys nothing here.
	for cid, v := range m.remoteTS {
		if m.remoteTSBytes+int64(len(data)) <= remoteTSViewBudget {
			break
		}
		m.remoteTSBytes -= v.bytes
		delete(m.remoteTS, cid)
	}
	m.remoteTS[id] = remoteTSView{view: view, bytes: int64(len(data))}
	m.remoteTSBytes += int64(len(data))
	m.remoteTSMu.Unlock()
	return view, true
}

// dropRemoteTSView forgets a chunk's fetched ITSI. Called on delete — a
// removed chunk must not keep answering rank lookups from a stale cache.
func (m *Manager) dropRemoteTSView(id chunk.ChunkID) {
	m.remoteTSMu.Lock()
	if v, ok := m.remoteTS[id]; ok {
		m.remoteTSBytes -= v.bytes
		delete(m.remoteTS, id)
	}
	m.remoteTSMu.Unlock()
}
