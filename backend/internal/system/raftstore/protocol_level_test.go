package raftstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"

	"google.golang.org/protobuf/encoding/protowire"
)

// rotationPolicyAtLevel2 stands in for a command type a later release adds:
// the table puts put_rotation_policy at level 2, above every binary here.
var rotationPolicyAtLevel2 = raftlevel.DefaultTable().With(
	raftlevel.Case((&gastrologv1.SystemCommand{}).ProtoReflect().Descriptor(), "put_rotation_policy"), 2)

// unknownCommandType is a SystemCommand whose oneof case is a field number
// this binary does not know, as a newer binary's command type decodes here.
func unknownCommandType() []byte {
	b := protowire.AppendTag(nil, 999, protowire.BytesType)
	return protowire.AppendBytes(b, []byte{0x08, 0x01})
}

func newRotationPolicy() system.RotationPolicyConfig {
	return system.RotationPolicyConfig{ID: glid.New(), Name: "level-2-probe", MaxAge: &testMaxAge}
}

func TestProposerRefusesCommandAboveActiveLevel(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New(raftfsm.WithBinaryLevel(2))
	r := newTestRaftWithFSM(t, fsm)
	s := New(r, fsm, 5*time.Second)
	s.gate = s.gate.WithTable(rotationPolicyAtLevel2)
	ctx := context.Background()
	if err := r.Barrier(5 * time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	before := r.LastIndex()
	err := s.PutRotationPolicy(ctx, newRotationPolicy())
	if !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
		t.Fatalf("PutRotationPolicy at active level 0 = %v, want ErrCommandAboveClusterLevel", err)
	}
	if got := r.LastIndex(); got != before {
		t.Fatalf("raft log grew from %d to %d: the refused command was proposed", before, got)
	}

	if err := s.SetProtocolLevel(ctx, 2); err != nil {
		t.Fatalf("SetProtocolLevel(2): %v", err)
	}
	if err := s.PutRotationPolicy(ctx, newRotationPolicy()); err != nil {
		t.Fatalf("PutRotationPolicy at active level 2: %v", err)
	}
}

func TestFollowerRefusesBeforeForwarding(t *testing.T) {
	t.Parallel()
	fsm := raftfsm.New()
	s := New(newFollowerRaft(t, fsm), fsm, 5*time.Second)
	s.gate = s.gate.WithTable(rotationPolicyAtLevel2)
	fwd := &mockForwarder{}
	s.SetForwarder(fwd)

	err := s.PutRotationPolicy(context.Background(), newRotationPolicy())
	if !errors.Is(err, raftlevel.ErrCommandAboveClusterLevel) {
		t.Fatalf("PutRotationPolicy = %v, want ErrCommandAboveClusterLevel", err)
	}
	if fwd.called {
		t.Fatal("the refused command was forwarded to the leader")
	}
}

// TestLeaderRefusesEntriesItCannotDecode covers a newer node forwarding a
// command type to an older leader: ApplyRaw is what the ForwardApply
// handler calls on the leader.
func TestLeaderRefusesEntriesItCannotDecode(t *testing.T) {
	t.Parallel()
	var stopped atomic.Int32
	fsm := raftfsm.New(raftfsm.WithOnUnapplicable(func(string, uint64, error) { stopped.Add(1) }))
	r := newTestRaftWithFSM(t, fsm)
	s := New(r, fsm, 5*time.Second)
	// The leader's own post-election entry must be in the log before the
	// log's length is compared.
	if err := r.Barrier(5 * time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"unknown command type", unknownCommandType(), raftlevel.ErrUnknownCommand},
		{"undecodable bytes", []byte("not a protobuf"), raftlevel.ErrUndecodableCommand},
		{"no command", nil, raftlevel.ErrUnknownCommand},
	}
	for _, tc := range cases {
		before := r.LastIndex()
		_, err := s.ApplyRaw(tc.data)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: ApplyRaw = %v, want %v", tc.name, err, tc.want)
		}
		if got := r.LastIndex(); got != before {
			t.Errorf("%s: raft log grew from %d to %d", tc.name, before, got)
		}
	}
	if n := stopped.Load(); n != 0 {
		t.Fatalf("an unapplicable entry reached the FSM %d times", n)
	}
}
