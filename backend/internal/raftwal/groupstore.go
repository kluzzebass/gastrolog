package raftwal

import (
	"encoding/binary"
	"fmt"

	hraft "github.com/hashicorp/raft"
)

// GroupStore implements raft.LogStore and raft.StableStore for a single
// Raft group, backed by the shared WAL. Writes go through the WAL's batch
// writer; reads are served from the in-memory index.
type GroupStore struct {
	wal     *WAL
	groupID uint32
}

// Compile-time interface checks.
var (
	_ hraft.LogStore    = (*GroupStore)(nil)
	_ hraft.StableStore = (*GroupStore)(nil)
)

// --- LogStore ---

func (g *GroupStore) FirstIndex() (uint64, error) {
	g.wal.stateMu.RLock()
	defer g.wal.stateMu.RUnlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return 0, nil
	}
	return gs.firstIndex, nil
}

func (g *GroupStore) LastIndex() (uint64, error) {
	g.wal.stateMu.RLock()
	defer g.wal.stateMu.RUnlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return 0, nil
	}
	return gs.lastIndex, nil
}

// GetLog serves recent entries from the in-memory window and older entries
// from the WAL segment files (memory over throughput: heap is bounded by
// Config.LogCacheBudgetBytes, not by log length).
func (g *GroupStore) GetLog(index uint64, log *hraft.Log) error {
	g.wal.stateMu.RLock()
	defer g.wal.stateMu.RUnlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return hraft.ErrLogNotFound
	}
	if enc, ok := gs.cache[index]; ok {
		return decodelog(enc, log)
	}
	loc, ok := gs.logs[index]
	if !ok {
		return hraft.ErrLogNotFound
	}
	enc, err := g.wal.readPayload(loc)
	if err != nil {
		return fmt.Errorf("raftwal: read log %d from segment %d: %w", index, loc.seg, err)
	}
	return decodelog(enc, log)
}

func (g *GroupStore) StoreLog(log *hraft.Log) error {
	return g.StoreLogs([]*hraft.Log{log})
}

// StoreLogs submits the whole batch as ONE writeOp: one queue round-trip, one
// WAL record, one fsync boundary. The record's single CRC makes the batch
// atomic on replay — a torn write drops it entirely, never half-applies it.
// Entry order within the record preserves the caller's slice order.
func (g *GroupStore) StoreLogs(logs []*hraft.Log) error {
	switch len(logs) {
	case 0:
		return nil
	case 1:
		return g.wal.submit(writeOp{
			groupID: g.groupID,
			typ:     entryLog,
			payload: encodelog(logs[0]),
		})
	default:
		return g.wal.submit(writeOp{
			groupID: g.groupID,
			typ:     entryLogBatch,
			payload: encodeLogBatch(logs),
		})
	}
}

func (g *GroupStore) DeleteRange(lo, hi uint64) error {
	return g.wal.submit(writeOp{
		groupID: g.groupID,
		typ:     entryDeleteRange,
		payload: encodeDeleteRange(lo, hi),
	})
}

// --- StableStore ---

func (g *GroupStore) Set(key []byte, val []byte) error {
	return g.wal.submit(writeOp{
		groupID: g.groupID,
		typ:     entryStableSet,
		payload: encodeStableSet(string(key), val),
	})
}

func (g *GroupStore) Get(key []byte) ([]byte, error) {
	g.wal.stateMu.RLock()
	defer g.wal.stateMu.RUnlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return nil, nil
	}
	val, ok := gs.stable[string(key)]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

func (g *GroupStore) SetUint64(key []byte, val uint64) error {
	return g.wal.submit(writeOp{
		groupID: g.groupID,
		typ:     entryStableUint64,
		payload: encodeStableUint64(string(key), val),
	})
}

func (g *GroupStore) GetUint64(key []byte) (uint64, error) {
	g.wal.stateMu.RLock()
	defer g.wal.stateMu.RUnlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return 0, nil
	}
	val, ok := gs.stable[string(key)]
	if !ok {
		return 0, nil
	}
	if len(val) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(val), nil
}
