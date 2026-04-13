package raftwal

import (
	"encoding/binary"

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
	g.wal.mu.Lock()
	defer g.wal.mu.Unlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return 0, nil
	}
	return gs.firstIndex, nil
}

func (g *GroupStore) LastIndex() (uint64, error) {
	g.wal.mu.Lock()
	defer g.wal.mu.Unlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return 0, nil
	}
	return gs.lastIndex, nil
}

func (g *GroupStore) GetLog(index uint64, log *hraft.Log) error {
	g.wal.mu.Lock()
	defer g.wal.mu.Unlock()
	gs := g.wal.groups[g.groupID]
	if gs == nil {
		return hraft.ErrLogNotFound
	}
	if index <= gs.deletedThrough {
		return hraft.ErrLogNotFound
	}
	data, ok := gs.logs[index]
	if !ok {
		return hraft.ErrLogNotFound
	}
	return decodelog(data, log)
}

func (g *GroupStore) StoreLog(log *hraft.Log) error {
	return g.StoreLogs([]*hraft.Log{log})
}

func (g *GroupStore) StoreLogs(logs []*hraft.Log) error {
	for _, log := range logs {
		payload := encodelog(log)
		if err := g.wal.submit(writeOp{
			groupID: g.groupID,
			typ:     entryLog,
			payload: payload,
		}); err != nil {
			return err
		}
	}
	return nil
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
	g.wal.mu.Lock()
	defer g.wal.mu.Unlock()
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
	g.wal.mu.Lock()
	defer g.wal.mu.Unlock()
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
