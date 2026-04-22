// Package vaultraft holds the vault control-plane Raft FSM (gastrolog-5xxbd).
// Today it is a no-op placeholder; vault-scoped commands will be added as
// metadata moves off tier-local Raft.
package vaultraft

import (
	"io"

	hraft "github.com/hashicorp/raft"
)

// FSM implements a minimal vault control-plane replicated state machine.
type FSM struct{}

// NewFSM returns a new vault control-plane FSM instance.
func NewFSM() *FSM { return &FSM{} }

// Apply is a no-op until vault-scoped commands are defined.
func (f *FSM) Apply(_ *hraft.Log) any { return nil }

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
