package cluster

import (
	"errors"
	"time"

	hraft "github.com/hashicorp/raft"
)

// Raft leadership errors are not equally recoverable, and treating them alike
// is how an announce gets dropped.
//
//   - ErrNotLeader: another node leads. Forward to it. Safe — nothing was
//     appended here.
//   - ErrLeadershipTransferInProgress: THIS node still leads but is rejecting
//     new entries while handing over. Forwarding is wrong, because
//     LeaderWithID may still name this node and the RPC would come straight
//     back into the same rejection. The transfer settles in milliseconds, so
//     a bounded retry is the correct response: afterwards the apply either
//     succeeds or returns ErrNotLeader and forwards normally.
//   - ErrLeadershipLost: leadership was lost WHILE COMMITTING. The entry may
//     already be committed, so a retry risks applying a chunk-lifecycle
//     command twice. Deliberately NOT retried — a duplicate CmdSealChunk or
//     CmdRequestDelete is worse than a dropped announce that
//     reconcileSealAnnounceDivergence will notice.
const (
	// leadershipTransferRetries bounds the retry. A transfer is a handful of
	// Raft round trips; if it has not settled after this many attempts the
	// cluster has a bigger problem than one announce, and the error should
	// surface rather than spin.
	leadershipTransferRetries = 5
	// leadershipTransferBackoff is the pause between attempts. Short because
	// the condition it waits out is short; the bound above is what stops this
	// from becoming an unbounded wait.
	leadershipTransferBackoff = 20 * time.Millisecond
)

// applyRetryingLeadershipTransfer calls apply, retrying only while Raft reports
// a leadership transfer in progress. Returns the final error, which callers
// then handle as usual (notably: forwarding on ErrNotLeader).
//
// Takes a sleep function so tests can drive the retry without wall-clock waits.
func applyRetryingLeadershipTransfer(apply func() error, sleep func(time.Duration)) error {
	if sleep == nil {
		sleep = time.Sleep
	}
	var err error
	for attempt := range leadershipTransferRetries {
		err = apply()
		if !errors.Is(err, hraft.ErrLeadershipTransferInProgress) {
			return err
		}
		if attempt < leadershipTransferRetries-1 {
			sleep(leadershipTransferBackoff)
		}
	}
	return err
}
