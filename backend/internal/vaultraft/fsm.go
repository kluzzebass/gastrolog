// Package vaultraft holds the vault control-plane Raft FSM (gastrolog-5xxbd).
// Vault chunk metadata is namespaced under OpVaultChunkFSM (per-vault sub-FSMs) on that
// same Raft group, without changing the vaultctlfsm wire encoding.
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
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

var vaultSnapMagic = [8]byte{'G', 'L', 'V', 'C', 'T', 'L', 'S', '1'}

const vaultSnapVersion uint32 = 1

// FSM implements the vault control-plane replicated state machine: no-ops,
// vault-scoped vaultctlfsm commands, and snapshot/restore across vaults.
//
// Readiness for reads/writes is NOT tracked on this FSM — it is tracked at
// the Raft level via r.AppliedIndex(), which advances for every log entry
// type (LogCommand, LogConfiguration, LogNoop) whereas FSM.Apply is only
// called for LogCommand. On a fresh cluster the only log entries are the
// bootstrap configuration and the leader's post-election no-op, which
// never reach FSM.Apply. See buildVaultRaftCallbacks in
// orchestrator/reconfig_vaults.go for the readiness wiring.
type FSM struct {
	mu sync.Mutex
	vaults     map[glid.GLID]*vaultctlfsm.FSM

	// onAfterRestore fires (outside mu) once Restore() has swapped
	// the vault-sub-FSM map. The orchestrator uses this to walk each
	// instance's reconciler and run ReconcileFromSnapshot, which processes
	// any pendingDeletes obligations the rejoining node owes and
	// projects FSM-sealed state onto local files. Without this hook
	// the receipt protocol's catchup mechanism is dead code.
	// See gastrolog-51gme.
	onAfterRestore func()
}

// NewFSM returns a new vault control-plane FSM instance.
func NewFSM() *FSM {
	return &FSM{
		vaults:    make(map[glid.GLID]*vaultctlfsm.FSM),
	}
}

// SetOnAfterRestore registers a callback fired (outside the FSM
// mutex) at the tail of every successful Restore. Idempotent;
// replaces any prior callback. The orchestrator wires this when
// the vault-ctl Raft group is first ensured so that snapshot install
// triggers ReconcileFromSnapshot on every instance in the vault.
func (f *FSM) SetOnAfterRestore(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onAfterRestore = fn
}

// Instances returns a snapshot of the current (vaultID → sub-FSM) map.
// Safe for the orchestrator's after-restore handler to iterate
// without holding mu.
func (f *FSM) Vaults() map[glid.GLID]*vaultctlfsm.FSM {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[glid.GLID]*vaultctlfsm.FSM, len(f.vaults))
	maps.Copy(out, f.vaults)
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
		var vaultID glid.GLID
		copy(vaultID[:], l.Data[1:1+glid.Size])
		sub := l.Data[1+glid.Size:]
		if len(sub) == 0 {
			return errors.New("vaultraft: OpVaultChunkFSM missing instance command body")
		}
		f.mu.Lock()
		t := f.vaults[vaultID]
		if t == nil {
			t = vaultctlfsm.New()
			f.vaults[vaultID] = t
		}
		subFSM := t
		f.mu.Unlock()
		inner := &hraft.Log{Index: l.Index, Term: l.Term, Type: l.Type, Data: sub}
		return subFSM.Apply(inner)
	default:
		return fmt.Errorf("vaultraft: unknown opcode %d", l.Data[0])
	}
}

// VaultFSM returns the vaultctlfsm sub-machine for vaultID, or nil if no
// command has been applied for that instance yet.
func (f *FSM) VaultFSM(vaultID glid.GLID) *vaultctlfsm.FSM {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.vaults[vaultID]
}

// EnsureVaultFSM returns the vaultctlfsm sub-state for vaultID, creating
// an empty sub-FSM if none exists yet (for wiring OnDelete/OnUpload before
// first Apply).
func (f *FSM) EnsureVaultFSM(vaultID glid.GLID) *vaultctlfsm.FSM {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.vaults[vaultID]
	if t == nil {
		t = vaultctlfsm.New()
		f.vaults[vaultID] = t
	}
	return t
}

// Snapshot returns a snapshot of all vault sub-FSMs (versioned wire format).
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.Lock()
	ids := slices.SortedFunc(maps.Keys(f.vaults), compareGLID)
	var vaultBlobs [][]byte
	for _, id := range ids {
		t := f.vaults[id]
		if t == nil {
			continue
		}
		snap, err := t.Snapshot()
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
		raw, err := persistSnapshotToBytes(snap)
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
		blob := make([]byte, 0, glid.Size+len(raw))
		blob = append(blob, id[:]...)
		blob = append(blob, raw...)
		vaultBlobs = append(vaultBlobs, blob)
	}
	f.mu.Unlock()
	return &vaultCtlSnapshot{vaultBlobs: vaultBlobs}, nil
}

// Restore replaces FSM state from a snapshot produced by Snapshot, or the
// legacy single-byte empty snapshot ({1}) written by older builds.
//
// Streams the snapshot incrementally rather than slurping it into memory —
// the combined instance-state blob may be large on clusters with many vaults.
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
			f.mu.Lock()
			f.vaults = make(map[glid.GLID]*vaultctlfsm.FSM)
			hook := f.onAfterRestore
			f.mu.Unlock()
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
		return fmt.Errorf("vaultraft restore: read vault count: %w", err)
	}
	n := int(binary.BigEndian.Uint32(countBuf[:]))

	nextVaults := make(map[glid.GLID]*vaultctlfsm.FSM, n)
	for i := range n {
		var vid glid.GLID
		if _, err := io.ReadFull(rc, vid[:]); err != nil {
			return fmt.Errorf("vaultraft restore: read vault[%d] id: %w", i, err)
		}
		var blenBuf [4]byte
		if _, err := io.ReadFull(rc, blenBuf[:]); err != nil {
			return fmt.Errorf("vaultraft restore: read vault[%d] blob length: %w", i, err)
		}
		blen := int64(binary.BigEndian.Uint32(blenBuf[:]))
		vaultReader := io.LimitReader(rc, blen)
		t := vaultctlfsm.New()
		if err := t.Restore(io.NopCloser(vaultReader)); err != nil {
			return fmt.Errorf("vaultraft restore vault %x: %w", vid[:], err)
		}
		// Drain any unread bytes so the next instance header aligns.
		if _, err := io.Copy(io.Discard, vaultReader); err != nil {
			return fmt.Errorf("vaultraft restore: drain vault[%d] tail: %w", i, err)
		}
		nextVaults[vid] = t
	}
	f.mu.Lock()
	f.vaults = nextVaults
	hook := f.onAfterRestore
	f.mu.Unlock()
	// Fire outside the mutex — the handler walks per-vault reconcilers
	// which can call back into the FSM (Instances, PendingDeletes, etc.).
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
	vaultBlobs [][]byte // each: [16 vaultID][instance snapshot bytes...]
}

func (s *vaultCtlSnapshot) Persist(sink hraft.SnapshotSink) error {
	if _, err := sink.Write(vaultSnapMagic[:]); err != nil {
		_ = sink.Cancel()
		return err
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], vaultSnapVersion)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(s.vaultBlobs))) //nolint:gosec // G115: vault count bounded in practice
	if _, err := sink.Write(hdr[:]); err != nil {
		_ = sink.Cancel()
		return err
	}
	for _, blob := range s.vaultBlobs {
		if len(blob) < glid.Size {
			_ = sink.Cancel()
			return errors.New("vaultraft snapshot: vault blob too short")
		}
		vid := blob[:glid.Size]
		payload := blob[glid.Size:]
		if _, err := sink.Write(vid); err != nil {
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
