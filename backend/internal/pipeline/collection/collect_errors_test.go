package collection

import (
	"errors"
	"fmt"
	"os"
	"testing"

	erragg "gastrolog/internal/errs"
)

// TestExpectedCollectErr pins classification to collection-owned sentinels:
// expected catch-up failures carry ErrSegmentUnavailable, ErrCorruptSegment,
// or os.ErrNotExist in their chain; everything else is unexpected.
func TestExpectedCollectErr(t *testing.T) {
	t.Parallel()

	expected := []error{
		ErrSegmentUnavailable,
		ErrCorruptSegment,
		os.ErrNotExist,
		// The shapes the transport adapters actually produce:
		// registry / holder-resolution misses in the segment pull client…
		fmt.Errorf("%w: segment abc not in vault-ctl registry", ErrSegmentUnavailable),
		fmt.Errorf("%w: no remote holder for segment abc", ErrSegmentUnavailable),
		// …the SegmentPuller re-attaching the sentinel around the RPC status…
		fmt.Errorf("pull from node-a: %w", fmt.Errorf("receive segment chunk from node-a: %w: %w",
			ErrSegmentUnavailable, errors.New("rpc error: code = NotFound"))),
		// …and PromoteVerified's corrupt-transfer attachments.
		errors.Join(ErrCorruptSegment, errors.New("segment header: bad magic")),
		fmt.Errorf("%w: segment checksum 0000abcd does not match published checksum 0000ef01", ErrCorruptSegment),
		fmt.Errorf("pull segment abc: %w", os.ErrNotExist),
		// …and PromoteVerified losing the pre-head file to a concurrent
		// release purge, in both attachment shapes.
		ErrPreHeadPurged,
		fmt.Errorf("%w: %w", ErrPreHeadPurged, os.ErrNotExist),
		// A pass aggregate is expected when every failure it summarizes is.
		erragg.SummaryJoin(
			fmt.Errorf("pull from n1: %w", ErrSegmentUnavailable),
			fmt.Errorf("%w: no remote holder for segment s", ErrSegmentUnavailable),
			errors.Join(ErrCorruptSegment, errors.New("bad magic")),
		),
	}
	for _, err := range expected {
		if !expectedCollectErr(err) {
			t.Errorf("want expected: %v", err)
		}
	}

	unexpected := []error{
		nil,
		errors.New("vault-ctl FSM required"),
		errors.New("disk full"),
		// One unexpected failure makes the whole pass aggregate unexpected.
		erragg.SummaryJoin(
			fmt.Errorf("pull from n1: %w", ErrSegmentUnavailable),
			errors.New("permission denied"),
		),
	}
	for _, err := range unexpected {
		if expectedCollectErr(err) {
			t.Errorf("want unexpected: %v", err)
		}
	}
}

// TestExpectedCollectErrIgnoresProse would catch a regression back to
// string-matching: errors that carry the exact prose the adapters emit — but
// no sentinel — must classify as unexpected. Classification is errors.Is on
// collection-owned sentinels only; rewording a message in another package can
// never flip it.
func TestExpectedCollectErrIgnoresProse(t *testing.T) {
	t.Parallel()

	proseOnly := []error{
		errors.New("no remote holder for segment abc"),
		errors.New("segment xyz not in vault-ctl registry"),
		errors.New("serve segment vault/seg: segment not found"),
		errors.New("rpc error: code = NotFound desc = serve segment v/s: segment not found"),
		errors.New("segment unavailable"), // same text as the sentinel, wrong identity
	}
	for _, err := range proseOnly {
		if expectedCollectErr(err) {
			t.Errorf("prose without a sentinel must be unexpected: %v", err)
		}
	}
}
