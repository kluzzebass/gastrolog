// Package vaultraft holds the vault control-plane Raft FSM (gastrolog-5xxbd).
// Tier chunk metadata is namespaced under OpTierFSM (per-tier sub-FSMs) so
// per-tier Raft groups can be retired without changing the tierfsm wire encoding.
package vaultraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"gastrolog/internal/glid"
	tierfsm "gastrolog/internal/tier/raftfsm"

	hraft "github.com/hashicorp/raft"
)

var vaultSnapMagic = [8]byte{'G', 'L', 'V', 'C', 'T', 'L', 'S', '1'}

const vaultSnapVersion uint32 = 1

// FSM implements the vault control-plane replicated state machine: no-ops,
// tier-scoped tierfsm commands, and snapshot/restore across tiers.
type FSM struct {
	tierMu sync.Mutex
	tiers  map[glid.GLID]*tierfsm.FSM
}

// NewFSM returns a new vault control-plane FSM instance.
func NewFSM() *FSM {
	return &FSM{
		tiers: make(map[glid.GLID]*tierfsm.FSM),
	}
}

// Apply executes vault control-plane commands. Empty payloads are ignored.
// The first byte selects the opcode; see cmd.go.
func (f *FSM) Apply(l *hraft.Log) any {
	if l == nil || len(l.Data) == 0 {
		return nil
	}
	switch l.Data[0] {
	case OpNoop:
		return nil
	case OpTierFSM:
		if len(l.Data) < 1+glid.Size {
			return fmt.Errorf("vaultraft: OpTierFSM payload too short (%d bytes)", len(l.Data))
		}
		var tierID glid.GLID
		copy(tierID[:], l.Data[1:1+glid.Size])
		sub := l.Data[1+glid.Size:]
		if len(sub) == 0 {
			return errors.New("vaultraft: OpTierFSM missing tier command body")
		}
		f.tierMu.Lock()
		t := f.tiers[tierID]
		if t == nil {
			t = tierfsm.New()
			f.tiers[tierID] = t
		}
		subFSM := t
		f.tierMu.Unlock()
		inner := &hraft.Log{Index: l.Index, Term: l.Term, Type: l.Type, Data: sub}
		return subFSM.Apply(inner)
	default:
		return fmt.Errorf("vaultraft: unknown opcode %d", l.Data[0])
	}
}

// TierFSM returns the tierfsm sub-machine for tierID, or nil if no command
// has been applied for that tier yet.
func (f *FSM) TierFSM(tierID glid.GLID) *tierfsm.FSM {
	f.tierMu.Lock()
	defer f.tierMu.Unlock()
	return f.tiers[tierID]
}

// Snapshot returns a snapshot of all tier sub-FSMs (versioned wire format).
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.tierMu.Lock()
	ids := slices.SortedFunc(maps.Keys(f.tiers), compareGLID)
	var tierBlobs [][]byte
	for _, id := range ids {
		t := f.tiers[id]
		if t == nil {
			continue
		}
		snap, err := t.Snapshot()
		if err != nil {
			f.tierMu.Unlock()
			return nil, err
		}
		raw, err := persistSnapshotToBytes(snap)
		if err != nil {
			f.tierMu.Unlock()
			return nil, err
		}
		blob := make([]byte, 0, glid.Size+len(raw))
		blob = append(blob, id[:]...)
		blob = append(blob, raw...)
		tierBlobs = append(tierBlobs, blob)
	}
	f.tierMu.Unlock()
	return &vaultCtlSnapshot{tierBlobs: tierBlobs}, nil
}

// Restore replaces FSM state from a snapshot produced by Snapshot, or the
// legacy single-byte empty snapshot ({1}) written by older builds.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("vaultraft restore: read: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if len(data) == 1 && data[0] == 1 {
		// Legacy empty snapshot from the pre-composite FSM; treat as empty state.
		f.tierMu.Lock()
		f.tiers = make(map[glid.GLID]*tierfsm.FSM)
		f.tierMu.Unlock()
		return nil
	}
	if len(data) < len(vaultSnapMagic)+8 {
		return fmt.Errorf("vaultraft restore: truncated snapshot (%d bytes)", len(data))
	}
	if !bytes.Equal(data[:len(vaultSnapMagic)], vaultSnapMagic[:]) {
		return errors.New("vaultraft restore: bad magic")
	}
	ver := binary.BigEndian.Uint32(data[len(vaultSnapMagic) : len(vaultSnapMagic)+4])
	if ver != vaultSnapVersion {
		return fmt.Errorf("vaultraft restore: unsupported snapshot version %d", ver)
	}
	n := int(binary.BigEndian.Uint32(data[len(vaultSnapMagic)+4 : len(vaultSnapMagic)+8]))
	off := len(vaultSnapMagic) + 8
	nextTiers := make(map[glid.GLID]*tierfsm.FSM)
	for range n {
		if off+glid.Size+4 > len(data) {
			return errors.New("vaultraft restore: truncated tier header")
		}
		var tid glid.GLID
		copy(tid[:], data[off:off+glid.Size])
		off += glid.Size
		blen := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		if blen < 0 || off+blen > len(data) {
			return errors.New("vaultraft restore: invalid tier blob length")
		}
		blob := data[off : off+blen]
		off += blen
		t := tierfsm.New()
		if err := t.Restore(io.NopCloser(bytes.NewReader(blob))); err != nil {
			return fmt.Errorf("vaultraft restore tier %x: %w", tid[:], err)
		}
		nextTiers[tid] = t
	}
	f.tierMu.Lock()
	f.tiers = nextTiers
	f.tierMu.Unlock()
	return nil
}

func compareGLID(a, b glid.GLID) int {
	return bytes.Compare(a[:], b[:])
}

func persistSnapshotToBytes(snap hraft.FSMSnapshot) ([]byte, error) {
	var buf bytes.Buffer
	sink := &bufSink{Writer: &buf}
	if err := snap.Persist(sink); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type bufSink struct{ io.Writer }

func (s *bufSink) Close() error  { return nil }
func (s *bufSink) ID() string    { return "vaultraft" }
func (s *bufSink) Cancel() error { return nil }

type vaultCtlSnapshot struct {
	tierBlobs [][]byte // each: [16 tierID][tier snapshot bytes...]
}

func (s *vaultCtlSnapshot) Persist(sink hraft.SnapshotSink) error {
	if _, err := sink.Write(vaultSnapMagic[:]); err != nil {
		_ = sink.Cancel()
		return err
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], vaultSnapVersion)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(s.tierBlobs))) //nolint:gosec // G115: tier count bounded in practice
	if _, err := sink.Write(hdr[:]); err != nil {
		_ = sink.Cancel()
		return err
	}
	for _, blob := range s.tierBlobs {
		if len(blob) < glid.Size {
			_ = sink.Cancel()
			return errors.New("vaultraft snapshot: tier blob too short")
		}
		tid := blob[:glid.Size]
		payload := blob[glid.Size:]
		if _, err := sink.Write(tid); err != nil {
			_ = sink.Cancel()
			return err
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload))) //nolint:gosec // G115
		if _, err := sink.Write(lenBuf[:]); err != nil {
			_ = sink.Cancel()
			return err
		}
		if _, err := sink.Write(payload); err != nil {
			_ = sink.Cancel()
			return err
		}
	}
	return sink.Close()
}

func (vaultCtlSnapshot) Release() {}
