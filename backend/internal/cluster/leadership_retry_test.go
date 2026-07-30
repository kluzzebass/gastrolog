package cluster

import (
	"errors"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

// Raft's three leadership errors need three different responses, and the bug
// this guards was treating them as one.

func TestLeadershipTransferIsRetriedUntilItSettles(t *testing.T) {
	t.Parallel()
	calls := 0
	var slept time.Duration
	err := applyRetryingLeadershipTransfer(func() error {
		calls++
		if calls < 3 {
			return hraft.ErrLeadershipTransferInProgress
		}
		return nil
	}, func(d time.Duration) { slept += d })

	if err != nil {
		t.Fatalf("apply that settles on the 3rd attempt returned %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("attempts = %d, want 3", calls)
	}
	if slept == 0 {
		t.Error("retried without pausing; a tight loop cannot outwait a transfer")
	}
}

// A transfer that never settles must surface the error rather than spin. The
// bound is what separates "wait out a handover" from "hang on a broken cluster".
func TestLeadershipTransferRetryIsBounded(t *testing.T) {
	t.Parallel()
	calls := 0
	err := applyRetryingLeadershipTransfer(func() error {
		calls++
		return hraft.ErrLeadershipTransferInProgress
	}, func(time.Duration) {})

	if !errors.Is(err, hraft.ErrLeadershipTransferInProgress) {
		t.Fatalf("err = %v, want the transfer error surfaced", err)
	}
	if calls != leadershipTransferRetries {
		t.Errorf("attempts = %d, want exactly %d", calls, leadershipTransferRetries)
	}
}

// ErrNotLeader must NOT be retried here — it is the caller's signal to forward,
// and retrying locally would just delay that.
func TestNotLeaderIsReturnedImmediatelyForForwarding(t *testing.T) {
	t.Parallel()
	calls := 0
	err := applyRetryingLeadershipTransfer(func() error {
		calls++
		return hraft.ErrNotLeader
	}, func(time.Duration) { t.Error("must not sleep for ErrNotLeader") })

	if !errors.Is(err, hraft.ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
	if calls != 1 {
		t.Errorf("attempts = %d, want 1 — ErrNotLeader is forwarded, not retried", calls)
	}
}

// ErrLeadershipLost must NOT be retried: leadership was lost WHILE COMMITTING,
// so the entry may already be applied. A duplicate CmdSealChunk is worse than a
// surfaced error that a reconcile pass will notice.
func TestLeadershipLostIsNotRetried(t *testing.T) {
	t.Parallel()
	calls := 0
	err := applyRetryingLeadershipTransfer(func() error {
		calls++
		return hraft.ErrLeadershipLost
	}, func(time.Duration) { t.Error("must not sleep for ErrLeadershipLost") })

	if !errors.Is(err, hraft.ErrLeadershipLost) {
		t.Fatalf("err = %v, want ErrLeadershipLost", err)
	}
	if calls != 1 {
		t.Errorf("attempts = %d, want 1 — the entry may already be committed", calls)
	}
}

// The success path must not pay for the retry machinery.
func TestSuccessAppliesOnce(t *testing.T) {
	t.Parallel()
	calls := 0
	if err := applyRetryingLeadershipTransfer(func() error {
		calls++
		return nil
	}, func(time.Duration) { t.Error("must not sleep on success") }); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("attempts = %d, want 1", calls)
	}
}
