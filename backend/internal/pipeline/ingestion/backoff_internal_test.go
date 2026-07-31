package ingestion

// Pure-function coverage for the ingester retry backoff: the curve is
// exercised directly, with no sleeps and no manager lifecycle.

import (
	"testing"
	"time"
)

func TestRetryBackoffDelayCurve(t *testing.T) {
	t.Parallel()

	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, retryBackoffBase},      // clean previous run: back to base
		{1, retryBackoffBase},      // first failure
		{2, 6 * time.Second},       // base × factor
		{3, 12 * time.Second},      // base × factor²
		{6, 96 * time.Second},      // still under cap
		{8, retryBackoffCap},       // 384s pre-cap → capped at 5m
		{20, retryBackoffCap},      // deep failure streak stays capped
		{1 << 30, retryBackoffCap}, // absurd count: no overflow, still cap
	}
	for _, tc := range cases {
		if got := retryBackoffDelay(tc.failures); got != tc.want {
			t.Errorf("retryBackoffDelay(%d) = %v, want %v", tc.failures, got, tc.want)
		}
	}
}

func TestRetryBackoffDelayMonotonic(t *testing.T) {
	t.Parallel()

	prev := time.Duration(0)
	for n := 0; n <= 16; n++ {
		d := retryBackoffDelay(n)
		if d < prev {
			t.Fatalf("retryBackoffDelay(%d) = %v < previous %v; curve must be non-decreasing", n, d, prev)
		}
		if d > retryBackoffCap {
			t.Fatalf("retryBackoffDelay(%d) = %v exceeds cap %v", n, d, retryBackoffCap)
		}
		prev = d
	}
}

// TestDefaultRetryDelayJitterBounds pins the jitter window: delay ∈
// [pre-jitter, pre-jitter + 2/3·pre-jitter). At the base this is the
// historical 3–5s first-retry window.
func TestDefaultRetryDelayJitterBounds(t *testing.T) {
	t.Parallel()

	for _, failures := range []int{0, 1, 2, 5, 50} {
		pre := retryBackoffDelay(failures)
		hi := pre + time.Duration(int64(pre)*2/3)
		for range 100 {
			d := defaultRetryDelay(failures)
			if d < pre || d >= hi {
				t.Fatalf("defaultRetryDelay(%d) = %v outside [%v, %v)", failures, d, pre, hi)
			}
		}
	}
}
