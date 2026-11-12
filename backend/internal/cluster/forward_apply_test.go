package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/system"
)

// A forwarded command the leader's FSM rejects comes back as
// FailedPrecondition, distinct from a leader that could not apply at all.
func TestForwardApplyRejectionIsFailedPrecondition(t *testing.T) {
	for name, cause := range map[string]error{
		"illegal transition": fmt.Errorf("set node state: %w: live → removed", system.ErrIllegalNodeStateTransition),
		"unknown node":       fmt.Errorf("set node state: %w: n9", system.ErrNodeNotFound),
		"level lowered":      fmt.Errorf("%w: active level is 2, refused 1", system.ErrProtocolLevelLowered),
		"above level":        fmt.Errorf("refused proposal: %w", raftlevel.ErrCommandAboveClusterLevel),
		"unknown command":    fmt.Errorf("refused proposal: %w", raftlevel.ErrUnknownCommand),
		"undecodable":        fmt.Errorf("refused proposal: %w", raftlevel.ErrUndecodableCommand),
	} {
		t.Run(name, func(t *testing.T) {
			s := &Server{applyFn: func(context.Context, []byte) (uint64, error) { return 0, cause }}
			_, err := s.forwardApply(context.Background(), &gastrologv1.ForwardApplyRequest{})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("code = %v, want FailedPrecondition (%v)", status.Code(err), err)
			}
		})
	}
	s := &Server{applyFn: func(context.Context, []byte) (uint64, error) { return 0, errors.New("raft apply: timeout") }}
	_, err := s.forwardApply(context.Background(), &gastrologv1.ForwardApplyRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal for an unclassified failure", status.Code(err))
	}
}

func noopEntry(t *testing.T) []byte {
	t.Helper()
	data, err := proto.Marshal(&gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_Noop{Noop: &gastrologv1.NoopCommand{}}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// The leader that receives a forwarded vault-ctl entry is the node that hands
// it to raft.Apply: it refuses an entry it cannot decode itself, or one above
// the active level, without proposing it, and with a code the forwarder does
// not retry against the next leader.
func TestForwardVaultApplyRefusesWhatTheLeaderCannotPropose(t *testing.T) {
	t.Parallel()
	unknown := protowire.AppendBytes(protowire.AppendTag(nil, 999, protowire.BytesType), nil)
	noopAt2 := raftlevel.DefaultTable().With(
		raftlevel.Case((&gastrologv1.VaultRaftCommand{}).ProtoReflect().Descriptor(), "noop"), 2)
	activeLevel := uint32(1)
	gate := raftlevel.NewGate(func() uint32 { return activeLevel }).WithTable(noopAt2)

	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"unknown command type", unknown, raftlevel.ErrUnknownCommand},
		{"undecodable bytes", []byte{0xFF}, raftlevel.ErrUndecodableCommand},
		{"above the active level", noopEntry(t), raftlevel.ErrCommandAboveClusterLevel},
	}
	for _, tc := range cases {
		proposed := false
		s := &Server{groupApplyFn: func(context.Context, string, []byte) (uint64, error) {
			proposed = true
			return 1, nil
		}}
		s.SetVaultCtlGate(gate)
		_, err := s.forwardVaultApply(context.Background(), &gastrologv1.ForwardVaultApplyRequest{GroupId: []byte("g"), Command: tc.data})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: code = %v, want InvalidArgument (%v)", tc.name, status.Code(err), err)
		}
		if !strings.Contains(err.Error(), tc.want.Error()) {
			t.Errorf("%s: %v does not say %v", tc.name, err, tc.want)
		}
		if proposed {
			t.Errorf("%s: the leader proposed the entry", tc.name)
		}
	}

	activeLevel = 2
	proposed := false
	s := &Server{groupApplyFn: func(context.Context, string, []byte) (uint64, error) {
		proposed = true
		return 7, nil
	}}
	s.SetVaultCtlGate(gate)
	resp, err := s.forwardVaultApply(context.Background(), &gastrologv1.ForwardVaultApplyRequest{GroupId: []byte("g"), Command: noopEntry(t)})
	if err != nil || !proposed || resp.GetAppliedIndex() != 7 {
		t.Fatalf("at active level 2: resp=%v err=%v proposed=%v", resp, err, proposed)
	}
}
