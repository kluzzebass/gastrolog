// Package vaultraft holds the vault control-plane Raft FSM (gastrolog-5xxbd).
// Tier chunk metadata is namespaced under OpVaultChunkFSM (per-tier sub-FSMs) on that
// same Raft group, without changing the tierfsm wire encoding.
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
	"gastrolog/internal/vaultraft/tierfsm"

	hraft "github.com/hashicorp/raft"
)

var vaultSnapMagic = [8]byte{'G', 'L', 'V', 'C', 'T', 'L', 'S', '1'}

const vaultSnapVersion uint32 = 1

// FSM implements the vault control-plane replicated state machine: no-ops,
// tier-scoped tierfsm commands, and snapshot/restore across tiers.
//
// Readiness for reads/writes is NOT tracked on this FSM — it is tracked at
// the Raft level via r.AppliedIndex(), which advances for every log entry
// type (LogCommand, LogConfiguration, LogNoop) whereas FSM.Apply is only
// called for LogCommand. On a fresh cluster the only log entries are the
// bootstrap configuration and the leader's post-election no-op, which
// never reach FSM.Apply. See buildTierRaftCallbacks in
// orchestrator/reconfig_vaults.go for the readiness wiring.
type FSM struct {
	tierMu sync.Mutex
	tiers  map[glid.GLID]*tierfsm.FSM

	// onAfterRestore fires (outside tierMu) once Restore() has swapped
	// the tier-sub-FSM map. The orchestrator uses this to walk each
	// tier's reconciler and run ReconcileFromSnapshot, which processes
	// any pendingDeletes obligations the rejoining node owes and
	// projects FSM-sealed state onto local files. Without this hook
	// the receipt protocol's catchup mechanism is dead code.
	// See gastrolog-51gme.
	onAfterRestore func()
}

// NewFSM returns a new vault control-plane FSM instance.
func NewFSM() *FSM {
	return &FSM{
		tiers: make(map[glid.GLID]*tierfsm.FSM),
	}
}

// SetOnAfterRestore registers a callback fired (outside the FSM
// mutex) at the tail of every successful Restore. Idempotent;
// replaces any prior callback. The orchestrator wires this when
// the vault-ctl Raft group is first ensured so that snapshot install
// triggers ReconcileFromSnapshot on every tier in the vault.
func (f *FSM) SetOnAfterRestore(fn func()) {
	f.tierMu.Lock()
	defer f.tierMu.Unlock()
	f.onAfterRestore = fn
}

// Tiers returns a snapshot of the current (tierID → sub-FSM) map.
// Safe for the orchestrator's after-restore handler to iterate
// without holding tierMu.
func (f *FSM) Tiers() map[glid.GLID]*tierfsm.FSM {
	f.tierMu.Lock()
	defer f.tierMu.Unlock()
	out := make(map[glid.GLID]*tierfsm.FSM, len(f.tiers))
	maps.Copy(out, f.tiers)
	return out
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
	case OpVaultChunkFSM:
		if len(l.Data) < 1+glid.Size {
			return fmt.Errorf("vaultraft: OpVaultChunkFSM payload too short (%d bytes)", len(l.Data))
		}
		var tierID glid.GLID
		copy(tierID[:], l.Data[1:1+glid.Size])
		sub := l.Data[1+glid.Size:]
		if len(sub) == 0 {
			return errors.New("vaultraft: OpVaultChunkFSM missing tier command body")
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

// EnsureTierFSM returns the tierfsm sub-state for tierID, creating an empty
// sub-FSM if none exists yet (for wiring OnDelete/OnUpload before first Apply).
func (f *FSM) EnsureTierFSM(tierID glid.GLID) *tierfsm.FSM {
	f.tierMu.Lock()
	defer f.tierMu.Unlock()
	t := f.tiers[tierID]
	if t == nil {
		t = tierfsm.New()
		f.tiers[tierID] = t
	}
	return t
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
//
// Streams the snapshot incrementally rather than slurping it into memory —
// the combined tier-state blob may be large on clusters with many tiers.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	// Peek the first byte to distinguish the legacy single-byte empty form
	// from the magic-prefixed format.
	var first [1]byte
	n1, err := rc.Read(first[:])
	if err == io.EOF || n1 == 0 {
		return nil
	}
	if err != nil && err != io.EOF {
		return fmt.Errorf("vaultraft restore: read first byte: %w", err)
	}
	if first[0] == 1 {
		// Legacy empty snapshot from the pre-composite FSM.
		var probe [1]byte
		if n, _ := rc.Read(probe[:]); n == 0 {
			f.tierMu.Lock()
			f.tiers = make(map[glid.GLID]*tierfsm.FSM)
			hook := f.onAfterRestore
			f.tierMu.Unlock()
			if hook != nil {
				hook()
			}
			return nil
		}
		return errors.New("vaultraft restore: trailing bytes after legacy empty sentinel")
	}

	// Read the remainder of the magic and validate.
	var restMagic [7]byte
	if _, err := io.ReadFull(rc, restMagic[:]); err != nil {
		return fmt.Errorf("vaultraft restore: read magic: %w", err)
	}
	if first[0] != vaultSnapMagic[0] || !bytes.Equal(restMagic[:], vaultSnapMagic[1:]) {
		return errors.New("vaultraft restore: bad magic")
	}

	var verBuf [4]byte
	if _, err := io.ReadFull(rc, verBuf[:]); err != nil {
		return fmt.Errorf("vaultraft restore: read version: %w", err)
	}
	ver := binary.BigEndian.Uint32(verBuf[:])
	if ver != vaultSnapVersion {
		return fmt.Errorf("vaultraft restore: unsupported snapshot version %d", ver)
	}

	var countBuf [4]byte
	if _, err := io.ReadFull(rc, countBuf[:]); err != nil {
		return fmt.Errorf("vaultraft restore: read tier count: %w", err)
	}
	n := int(binary.BigEndian.Uint32(countBuf[:]))

	nextTiers := make(map[glid.GLID]*tierfsm.FSM, n)
	for i := range n {
		var tid glid.GLID
		if _, err := io.ReadFull(rc, tid[:]); err != nil {
			return fmt.Errorf("vaultraft restore: read tier[%d] id: %w", i, err)
		}
		var blenBuf [4]byte
		if _, err := io.ReadFull(rc, blenBuf[:]); err != nil {
			return fmt.Errorf("vaultraft restore: read tier[%d] blob length: %w", i, err)
		}
		blen := int64(binary.BigEndian.Uint32(blenBuf[:]))
		tierReader := io.LimitReader(rc, blen)
		t := tierfsm.New()
		if err := t.Restore(io.NopCloser(tierReader)); err != nil {
			return fmt.Errorf("vaultraft restore tier %x: %w", tid[:], err)
		}
		// Drain any unread bytes so the next tier header aligns.
		if _, err := io.Copy(io.Discard, tierReader); err != nil {
			return fmt.Errorf("vaultraft restore: drain tier[%d] tail: %w", i, err)
		}
		nextTiers[tid] = t
	}
	f.tierMu.Lock()
	f.tiers = nextTiers
	hook := f.onAfterRestore
	f.tierMu.Unlock()
	// Fire outside the mutex — the handler walks per-tier reconcilers
	// which can call back into the FSM (Tiers, PendingDeletes, etc.).
	if hook != nil {
		hook()
	}
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
