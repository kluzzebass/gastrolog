// Package vaultraft holds the vault control-plane Raft FSM (gastrolog-5xxbd).
// Vault chunk metadata is namespaced under OpVaultChunkFSM (per-vault sub-FSMs) on that
// same Raft group, without changing the vaultctlfsm wire encoding.
package vaultraft

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/applywait"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

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
	mu     sync.Mutex
	vaults map[glid.GLID]*vaultctlfsm.FSM

	// onAfterRestore fires (outside mu) once Restore() has swapped
	// the vault-sub-FSM map. The orchestrator uses this to walk each
	// instance's reconciler and run ReconcileFromSnapshot, which processes
	// any pendingDeletes obligations the rejoining node owes and
	// projects FSM-sealed state onto local files. Without this hook
	// the receipt protocol's catchup mechanism is dead code.
	// See gastrolog-51gme.
	onAfterRestore func()

	// applyWait is advanced after each log entry is applied (and after a
	// snapshot restore, up to the index the snapshot embeds). The vault-ctl
	// forward paths block on it after forwarding a command to the group
	// leader so their next local read sees post-mutation state — the
	// read-after-write barrier (gastrolog-4l24u), event-driven per
	// gastrolog-3klg1.
	applyWait *applywait.Tracker
}

// NewFSM returns a new vault control-plane FSM instance.
func NewFSM() *FSM {
	return &FSM{
		vaults:    make(map[glid.GLID]*vaultctlfsm.FSM),
		applyWait: applywait.New(),
	}
}

// ApplyWait returns the tracker that follows this FSM's applied index.
// VaultApplyForwarder / VaultCtlChunkApplyForwarder wait on it after
// forwarding a command to the vault-ctl group leader.
func (f *FSM) ApplyWait() *applywait.Tracker {
	return f.applyWait
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
// The log data is a marshaled gastrologv1.VaultRaftCommand; see cmd.go.
func (f *FSM) Apply(l *hraft.Log) any {
	if l == nil {
		return nil
	}
	// The entry is consumed regardless of dispatch outcome — advance the
	// apply tracker on every return path, after the sub-FSM mutation (if
	// any) is visible. Mirrors raft's own applied-index semantics, which
	// count entries whose FSM application returned an error.
	defer f.applyWait.Advance(l.Index)
	if len(l.Data) == 0 {
		return nil
	}
	var cmd gastrologv1.VaultRaftCommand
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("vaultraft: decode command: %w", err)
	}
	switch c := cmd.GetCommand().(type) {
	case *gastrologv1.VaultRaftCommand_Noop:
		return nil
	case *gastrologv1.VaultRaftCommand_VaultScoped:
		vsc := c.VaultScoped
		if len(vsc.GetVaultId()) < glid.Size {
			return fmt.Errorf("vaultraft: vault-scoped command missing vault id (%d bytes)", len(vsc.GetVaultId()))
		}
		var vaultID glid.GLID
		copy(vaultID[:], vsc.GetVaultId())
		inner := vsc.GetCommand()
		if inner == nil || inner.GetCommand() == nil {
			return errors.New("vaultraft: vault-scoped command missing instance command body")
		}
		f.mu.Lock()
		t := f.vaults[vaultID]
		if t == nil {
			t = vaultctlfsm.New()
			f.vaults[vaultID] = t
		}
		subFSM := t
		f.mu.Unlock()
		return subFSM.ApplyCommand(inner)
	default:
		return fmt.Errorf("vaultraft: unknown command %T", cmd.GetCommand())
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

// Snapshot returns a snapshot of all vault sub-FSMs as a VaultGroupSnapshot
// proto (gastrolog-5lrg7). Vaults are emitted in GLID order so equal FSM
// state yields a byte-stable group snapshot.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := slices.SortedFunc(maps.Keys(f.vaults), glid.Compare)
	group := &gastrologv1.VaultGroupSnapshot{
		Vaults: make([]*gastrologv1.VaultGroupSnapshotEntry, 0, len(ids)),
		// Embed the highest applied index so a follower that installs this
		// snapshot (instead of replaying log entries) can release apply-wait
		// barriers for every command the snapshot covers. Raft serializes
		// Snapshot with Apply on the FSM goroutine, so this reads the exact
		// index the captured state reflects.
		LastAppliedIndex: f.applyWait.Applied(),
	}
	for _, id := range ids {
		t := f.vaults[id]
		if t == nil {
			continue
		}
		idCopy := id
		group.Vaults = append(group.Vaults, &gastrologv1.VaultGroupSnapshotEntry{
			VaultId:  idCopy[:],
			Snapshot: t.SnapshotProto(),
		})
	}
	return &vaultCtlSnapshot{group: group}, nil
}

// Restore replaces FSM state from a snapshot produced by Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	raw, err := io.ReadAll(rc) //ok:io-readall raft snapshot restore; input is this FSM's own marshaled VaultGroupSnapshot, bounded by manifest size
	if err != nil {
		return fmt.Errorf("vaultraft restore: read: %w", err)
	}
	if len(raw) == 0 {
		return nil
	}
	var group gastrologv1.VaultGroupSnapshot
	if err := proto.Unmarshal(raw, &group); err != nil {
		return fmt.Errorf("vaultraft restore: decode: %w", err)
	}

	nextVaults := make(map[glid.GLID]*vaultctlfsm.FSM, len(group.GetVaults()))
	for i, entry := range group.GetVaults() {
		vid := glid.FromBytes(entry.GetVaultId())
		t := vaultctlfsm.New()
		t.RestoreProto(entry.GetSnapshot())
		if _, dup := nextVaults[vid]; dup {
			return fmt.Errorf("vaultraft restore: duplicate vault[%d] id %x", i, vid[:])
		}
		nextVaults[vid] = t
	}
	f.mu.Lock()
	f.vaults = nextVaults
	hook := f.onAfterRestore
	f.mu.Unlock()
	// Wake apply-wait barriers covered by this snapshot — the restored
	// state includes every command up to the embedded index. Advance after
	// the sub-FSM map swap (waiters must read post-restore state) and
	// before the potentially slow after-restore hook.
	f.applyWait.Advance(group.GetLastAppliedIndex())
	// Fire outside the mutex — the handler walks per-vault reconcilers
	// which can call back into the FSM (Instances, PendingDeletes, etc.).
	if hook != nil {
		hook()
	}
	return nil
}

type vaultCtlSnapshot struct {
	group *gastrologv1.VaultGroupSnapshot
}

func (s *vaultCtlSnapshot) Persist(sink hraft.SnapshotSink) error {
	b, err := proto.Marshal(s.group)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("vaultraft snapshot: marshal: %w", err)
	}
	if _, err := sink.Write(b); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (vaultCtlSnapshot) Release() {}
