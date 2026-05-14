package cluster

import (
	"errors"
	"fmt"
	"testing"
)

// gastrolog-2cb2r: JoinCluster classifies certain gRPC + raftadmin
// errors as transient and retries until the caller's ctx deadline.
// These tests pin the classification — if a future agent changes the
// underlying transport errors, the breakage should show up here, not
// as a silent regression to CrashLoopBackOff-driven retry.

func TestIsTransientJoinErr_NotTheLeader(t *testing.T) {
	t.Parallel()
	cases := []string{
		"membership change: node is not the leader",
		"add voter RPC: rpc error: code = Unknown desc = node is not the leader",
		"await membership change: node is not the leader",
	}
	for _, msg := range cases {
		err := errors.New(msg)
		if !isTransientJoinErr(err) {
			t.Errorf("expected transient for %q, got fatal", msg)
		}
	}
}

func TestIsTransientJoinErr_LeadershipLost(t *testing.T) {
	t.Parallel()
	for _, msg := range []string{
		"membership change: leadership lost while committing log",
		"membership change: leadership transfer in progress",
	} {
		if !isTransientJoinErr(errors.New(msg)) {
			t.Errorf("expected transient for %q", msg)
		}
	}
}

func TestIsTransientJoinErr_Unavailable(t *testing.T) {
	t.Parallel()
	for _, msg := range []string{
		"dial node-1: rpc error: code = Unavailable desc = connection error: desc = transport: Error while dialing",
		"add voter RPC: rpc error: code = Unavailable desc = no healthy upstream",
	} {
		if !isTransientJoinErr(errors.New(msg)) {
			t.Errorf("expected transient for %q", msg)
		}
	}
}

func TestIsTransientJoinErr_ConnectionRefused(t *testing.T) {
	t.Parallel()
	msg := "dial node-1: connection refused"
	if !isTransientJoinErr(errors.New(msg)) {
		t.Errorf("expected transient for %q", msg)
	}
}

func TestIsTransientJoinErr_FatalCases(t *testing.T) {
	t.Parallel()
	// These are configuration / permission errors that should NOT
	// retry — retrying would just spin until ctx deadline on a problem
	// no amount of waiting will fix.
	cases := []string{
		"dial node-1: rpc error: code = Unauthenticated desc = invalid client cert",
		"add voter RPC: rpc error: code = PermissionDenied desc = client not authorized",
		"membership change: malformed node ID",
		"dial node-1: certificate has expired",
		"some completely unexpected error",
	}
	for _, msg := range cases {
		if isTransientJoinErr(errors.New(msg)) {
			t.Errorf("expected fatal for %q, got transient", msg)
		}
	}
}

func TestIsTransientJoinErr_NilErr(t *testing.T) {
	t.Parallel()
	if isTransientJoinErr(nil) {
		t.Error("expected fatal (false) for nil error")
	}
}

func TestIsTransientJoinErr_WrappedErrors(t *testing.T) {
	t.Parallel()
	// Production callers wrap errors via fmt.Errorf("...%w", err).
	// The classifier MUST see through the wrapping (Error() concatenates
	// the wrapped error's message).
	inner := errors.New("membership change: node is not the leader")
	wrapped := fmt.Errorf("join cluster: %w", inner)
	if !isTransientJoinErr(wrapped) {
		t.Errorf("expected transient for wrapped error, got fatal: %v", wrapped)
	}
}
