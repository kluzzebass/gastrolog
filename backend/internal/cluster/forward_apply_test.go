package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
)

// A forwarded command the leader's FSM rejects comes back as
// FailedPrecondition, distinct from a leader that could not apply at all.
func TestForwardApplyRejectionIsFailedPrecondition(t *testing.T) {
	for name, cause := range map[string]error{
		"illegal transition": fmt.Errorf("set node state: %w: live → removed", system.ErrIllegalNodeStateTransition),
		"unknown node":       fmt.Errorf("set node state: %w: n9", system.ErrNodeNotFound),
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
