// Package vaultraft holds the vault control-plane Raft FSM (gastrolog-5xxbd).
// Today it is a no-op placeholder; vault-scoped commands will be added as
// metadata moves off tier-local Raft.
package vaultraft

import (
	"fmt"
	"io"

	hraft "github.com/hashicorp/raft"
)

// FSM implements a minimal vault control-plane replicated state machine.
type FSM struct{}

// NewFSM returns a new vault control-plane FSM instance.
func NewFSM() *FSM { return &FSM{} }

// Apply executes vault control-plane commands. Empty payloads are ignored.
// The first byte selects the opcode; see OpNoop and future constants in cmd.go.
func (f *FSM) Apply(l *hraft.Log) any {
	if l == nil || len(l.Data) == 0 {
		return nil
	}
	switch l.Data[0] {
	case OpNoop:
		return nil
	default:
		return fmt.Errorf("vaultraft: unknown opcode %d", l.Data[0])
	}
}

// Snapshot returns an empty snapshot.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) { return emptySnapshot{}, nil }

// Restore discards snapshot bytes (empty format).
func (f *FSM) Restore(rc io.ReadCloser) error { return rc.Close() }

type emptySnapshot struct{}

func (emptySnapshot) Persist(sink hraft.SnapshotSink) error {
	if _, err := sink.Write([]byte{1}); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (emptySnapshot) Release() {}
