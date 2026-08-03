package raftwal

import (
	"os"
	"slices"
	"sort"
	"time"
)

// ReclaimStats describes one reclaimed segment.
type ReclaimStats struct {
	Seq            int
	ReclaimedBytes int64 // file bytes released by the unlink
	ScavengedBytes int64 // live payload bytes rewritten first (0 for a pure unlink)
	Duration       time.Duration
}

// reclaimPass reclaims dead WAL space. Runs on the batch writer strictly
// after batch waiters are notified: unlinks every drained oldest segment,
// then scavenges at most one nearly-drained one.
func (w *WAL) reclaimPass() {
	for w.unlinkOldestDrained(0, time.Now()) {
	}
	w.scavengeOldest()
}

// oldestSealedSegment returns the lowest tracked segment sequence below the
// active segment, or 0 when none exists. Caller holds stateMu.
func (w *WAL) oldestSealedSegment() int {
	oldest := 0
	for seq := range w.segLive {
		if seq >= w.segSeq {
			continue
		}
		if oldest == 0 || seq < oldest {
			oldest = seq
		}
	}
	return oldest
}

// unlinkOldestDrained removes the oldest sealed segment if its counter reads
// drained AND a verification scan confirms no index reference survives — the
// counter only nominates. A disagreement quarantines the segment and raises
// OnReclaimAnomaly once. Returns whether a segment was removed.
func (w *WAL) unlinkOldestDrained(scavengedBytes int64, start time.Time) bool {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	seq := w.oldestSealedSegment()
	if seq == 0 || w.segLive[seq] != 0 {
		return false
	}
	if _, bad := w.quarantined[seq]; bad {
		return false
	}
	if refs := w.liveRefsForSegment(seq); refs > 0 {
		w.quarantineSegment(seq, refs)
		return false
	}

	path := w.segmentPath(seq)
	var reclaimedBytes int64
	if info, err := os.Stat(path); err == nil {
		reclaimedBytes = info.Size()
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false
	}
	w.closeSegmentReadersUpTo(seq)
	delete(w.segLive, seq)

	if w.cfg.OnReclaim != nil {
		w.cfg.OnReclaim(ReclaimStats{
			Seq:            seq,
			ReclaimedBytes: reclaimedBytes,
			ScavengedBytes: scavengedBytes,
			Duration:       time.Since(start),
		})
	}
	return true
}

// quarantineSegment excludes a segment from reclamation and reports the
// contradiction once. liveRefs is what the verification scan found: above
// zero when the counter read drained, zero when the counter claims live
// bytes no index entry references. Caller holds stateMu.
func (w *WAL) quarantineSegment(seq, liveRefs int) {
	if _, bad := w.quarantined[seq]; bad {
		return
	}
	w.quarantined[seq] = struct{}{}
	if w.cfg.OnReclaimAnomaly != nil {
		w.cfg.OnReclaimAnomaly(seq, liveRefs)
	}
}

// scavRecord is one live record carried out of a segment being scavenged.
type scavRecord struct {
	gid     uint32
	typ     entryType
	payload []byte
	isLog   bool
	idx     uint64 // raft index when isLog
	key     string // stable key when typ == entryStableSet
}

// scavengeOldest reclaims the oldest sealed segment when its live remainder
// is at or below Config.ScavengeMaxLiveBytes: re-append the live records
// through the normal write path, fsync, repoint the index, then unlink. Any
// error aborts with the index untouched — the copies are idempotent
// duplicates on replay and the pass retries later. Runs on the batch writer.
func (w *WAL) scavengeOldest() {
	start := time.Now()

	w.stateMu.RLock()
	victim := w.oldestSealedSegment()
	eligible := victim != 0 && w.segLive[victim] > 0 &&
		w.segLive[victim] <= w.cfg.ScavengeMaxLiveBytes
	if eligible {
		if _, bad := w.quarantined[victim]; bad {
			eligible = false
		}
	}
	w.stateMu.RUnlock()
	if !eligible {
		return
	}

	records, err := w.collectScavenge(victim)
	if err != nil {
		return // victim unreadable right now; a later pass retries
	}
	if len(records) == 0 {
		// The counter claims live bytes, the scan finds no reference: drift
		// in the opposite direction to the drained-but-referenced case.
		// Nothing can drain the segment, so quarantine it rather than let
		// it pin every segment behind it in silence.
		w.stateMu.Lock()
		w.quarantineSegment(victim, 0)
		w.stateMu.Unlock()
		return
	}
	locs, err := w.appendScavenge(records)
	if err != nil {
		return
	}
	scavenged := w.swapScavenged(victim, records, locs)
	w.unlinkOldestDrained(scavenged, start)
}

// collectScavenge gathers the victim segment's live records: group
// registrations, current stable values, and surviving log entries. Log
// payloads come from the recent window when cached, otherwise from the
// victim file (still on disk). Deterministic order: group, then kind, then
// key/index. An empty result means the index references nothing in the
// segment; a non-nil error means the payloads could not be read this pass
// and the caller must not read anything into the empty result.
func (w *WAL) collectScavenge(victim int) ([]scavRecord, error) {
	w.stateMu.RLock()
	defer w.stateMu.RUnlock()

	gids := make([]uint32, 0, len(w.groups))
	for gid := range w.groups {
		gids = append(gids, gid)
	}
	slices.Sort(gids)

	var records []scavRecord
	for _, gid := range gids {
		gs := w.groups[gid]
		if gs == nil {
			continue
		}
		if gs.regName != "" && gs.regLoc.seg == victim {
			records = append(records, scavRecord{
				gid: gid, typ: entryGroupReg, payload: []byte(gs.regName),
			})
		}
		keys := make([]string, 0, len(gs.stable))
		for k, sv := range gs.stable {
			if sv.loc.seg == victim {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			records = append(records, scavRecord{
				gid: gid, typ: entryStableSet,
				payload: encodeStableSet(k, gs.stable[k].value), key: k,
			})
		}
		var idxs []uint64
		for idx, loc := range gs.logs {
			if loc.seg == victim {
				idxs = append(idxs, idx)
			}
		}
		slices.Sort(idxs)
		for _, idx := range idxs {
			payload, ok := gs.cache[idx]
			if !ok {
				var err error
				payload, err = w.readPayload(gs.logs[idx])
				if err != nil {
					return nil, err // victim unreadable: abort, retry later
				}
			}
			records = append(records, scavRecord{
				gid: gid, typ: entryLog, payload: payload, isLog: true, idx: idx,
			})
		}
	}
	return records, nil
}

// appendScavenge writes the records through the normal append path on the
// active segment (rotating on size like any append) and fsyncs. Runs on the
// batch writer.
func (w *WAL) appendScavenge(records []scavRecord) ([]logLoc, error) {
	locs := make([]logLoc, len(records))
	for i, r := range records {
		entrySize := int64(headerSize + len(r.payload))
		if w.segSize > 0 && w.segSize+entrySize > w.cfg.SegmentTargetSize {
			if err := w.rotateSegment(); err != nil {
				return nil, err
			}
		}
		locs[i] = logLoc{seg: w.segSeq, off: w.segSize + headerSize, length: len(r.payload)}
		if err := w.appendEntry(r.gid, r.typ, r.payload); err != nil {
			return nil, err
		}
	}
	if err := w.syncActiveSegment(); err != nil {
		return nil, err
	}
	return locs, nil
}

// swapScavenged repoints the index at the fsynced copies and moves the
// live-bytes accounting, draining the victim. Copies never enter the recent
// window: re-caching cold records would evict the hot tail. Returns the
// payload bytes moved.
func (w *WAL) swapScavenged(victim int, records []scavRecord, locs []logLoc) int64 {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()

	var moved int64
	for i, r := range records {
		gs := w.groups[r.gid]
		if gs == nil {
			continue
		}
		newLoc := locs[i]
		switch {
		case r.isLog:
			if cur, ok := gs.logs[r.idx]; ok && cur.seg == victim {
				gs.logs[r.idx] = newLoc
				w.segLive[victim] -= int64(cur.length)
				w.segLive[newLoc.seg] += int64(newLoc.length)
				moved += int64(newLoc.length)
			}
		case r.typ == entryStableSet:
			if cur, ok := gs.stable[r.key]; ok && cur.loc.seg == victim {
				gs.stable[r.key] = stableVal{value: cur.value, loc: newLoc}
				w.segLive[victim] -= int64(cur.loc.length)
				w.segLive[newLoc.seg] += int64(newLoc.length)
				moved += int64(newLoc.length)
			}
		case r.typ == entryGroupReg:
			if gs.regLoc.seg == victim {
				w.segLive[victim] -= int64(gs.regLoc.length)
				gs.regLoc = newLoc
				w.segLive[newLoc.seg] += int64(newLoc.length)
				moved += int64(newLoc.length)
			}
		}
	}
	return moved
}
