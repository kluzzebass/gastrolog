package raftfsm

import (
	"testing"

	"github.com/hashicorp/raft"
)

// FuzzFSMApply feeds random bytes as a Raft log entry to the FSM.
// The FSM must handle malformed commands without panicking — returning
// an error from Apply is fine.
func FuzzFSMApply(f *testing.F) {
	// Seed with empty, short, and garbage payloads.
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte("not a protobuf"))
	// A truncated protobuf varint.
	f.Add([]byte{0x0a, 0x80})

	fsm := New()

	f.Fuzz(func(t *testing.T, data []byte) {
		// Apply may return nil or an error; it must never panic.
		_ = fsm.Apply(&raft.Log{Data: data, Index: 1})
	})
}
