// Package waittest synchronises tests on conditions instead of on the clock.
//
// A test that sleeps past an asynchronous step, or bounds one with a small
// deadline, measures the scheduler as much as the code under test: a busy
// machine becomes indistinguishable from a broken one, and the failure it
// produces points at the wrong suspect. Waiting on the condition itself
// moves load from whether the test passes to when it finishes.
package waittest

import (
	"testing"
	"time"
)

// Failsafe bounds every wait. It stands in for a wedged system, not for the
// measurement, so it is deliberately generous: reaching it means the awaited
// condition is never coming, not that the machine is busy.
const Failsafe = 30 * time.Second

// For polls cond until it holds, failing t if the Failsafe passes first.
// what names the awaited condition in the failure message.
func For(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(Failsafe)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
