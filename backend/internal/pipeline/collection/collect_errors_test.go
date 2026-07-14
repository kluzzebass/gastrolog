package collection

import (
	"errors"
	"fmt"
	"os"
	"testing"

	erragg "gastrolog/internal/errs"
)

// TestRetryableCollectErr pins classification to collection-owned sentinels:
// deferred (retryable) failures carry ErrSegmentUnavailable, ErrCorruptSegment,
// or os.ErrNotExist in their chain; everything else is terminal.
func TestRetryableCollectErr(t *testing.T) {
	t.Parallel()

	retryable := []error{
		ErrSegmentUnavailable,
		ErrCorruptSegment,
		os.ErrNotExist,
		// The shapes the transport adapters actually produce (gastrolog-466kq5):
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
		// A pass aggregate is retryable when every failure it summarizes is.
		erragg.SummaryJoin(
			fmt.Errorf("pull from n1: %w", ErrSegmentUnavailable),
			fmt.Errorf("%w: no remote holder for segment s", ErrSegmentUnavailable),
			errors.Join(ErrCorruptSegment, errors.New("bad magic")),
		),
	}
	for _, err := range retryable {
		if !retryableCollectErr(err) {
			t.Errorf("expected retryable: %v", err)
		}
	}

	nonRetryable := []error{
		nil,
		errors.New("vault-ctl FSM required"),
		errors.New("disk full"),
		// One terminal failure poisons the whole pass aggregate.
		erragg.SummaryJoin(
			fmt.Errorf("pull from n1: %w", ErrSegmentUnavailable),
			errors.New("permission denied"),
		),
	}
	for _, err := range nonRetryable {
		if retryableCollectErr(err) {
			t.Errorf("expected non-retryable: %v", err)
		}
	}
}

// TestRetryableCollectErrIgnoresProse would catch a regression back to
// string-matching: errors that carry the exact prose the adapters emit — but
// no sentinel — must classify as terminal. Classification is errors.Is on
// collection-owned sentinels only; rewording a message in another package can
// never flip it (gastrolog-466kq5).
func TestRetryableCollectErrIgnoresProse(t *testing.T) {
	t.Parallel()

	proseOnly := []error{
		errors.New("no remote holder for segment abc"),
		errors.New("segment xyz not in vault-ctl registry"),
		errors.New("serve segment vault/seg: segment not found"),
		errors.New("rpc error: code = NotFound desc = serve segment v/s: segment not found"),
		errors.New("segment unavailable"), // same text as the sentinel, wrong identity
	}
	for _, err := range proseOnly {
		if retryableCollectErr(err) {
			t.Errorf("prose without a sentinel must be terminal: %v", err)
		}
	}
}
