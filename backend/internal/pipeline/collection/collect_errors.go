package collection

import (
	"errors"
	"os"

	erragg "gastrolog/internal/errs"
)

// ErrSegmentUnavailable marks a pull failure where no source can serve the
// segment right now: the vault-ctl registry does not (yet) list it, no remote
// holder is known, or the serving holder no longer has the bytes. These are
// expected catch-up races at high ingest — the next vault-ctl publish or
// retry wake resolves them — so they classify as deferred, not failed.
//
// Collection owns this sentinel; transport adapters attach it at the
// boundary. The segment pull client (orchestrator) wraps it around registry
// and holder-resolution misses, and cluster.SegmentPuller translates the
// PullSegment RPC's NotFound status into it — mirroring how chunk/glcb
// translates blob-store sentinels into chunk sentinels. Classification here
// never inspects transport types or error prose.
var ErrSegmentUnavailable = errors.New("segment unavailable")

// expectedCollectErr reports whether a collect pass failure is an expected
// catch-up race at high ingest: the registry still lists a segment but no peer
// has bytes yet (no holder acks), a holder already purged head/ after seal, or
// a pulled copy failed verification and must be re-pulled from another holder.
// It decides only how loudly a failed pass is logged — every failed pass
// retries on the manager's backoff wake. A pass aggregate (SummaryJoin) is
// expected only when every failure it summarizes is, so one unexpected failure
// surfaces at Warn.
func expectedCollectErr(err error) bool {
	if err == nil {
		return false
	}
	subs := erragg.Unpack(err)
	if subs == nil {
		subs = []error{err}
	}
	for _, sub := range subs {
		if !expectedCollectSuberr(sub) {
			return false
		}
	}
	return true
}

// expectedCollectSuberr classifies one failure by collection-owned
// sentinels via errors.Is — never by transport status types or error prose,
// so rewording a message elsewhere cannot flip classification.
func expectedCollectSuberr(err error) bool {
	if errors.Is(err, ErrCorruptSegment) {
		// Checksum verification failed: the serving holder has wrong bytes.
		// The pre-head copy is already discarded and another holder can
		// serve correct bytes on the retry.
		return true
	}
	if errors.Is(err, ErrSegmentUnavailable) {
		return true
	}
	if errors.Is(err, ErrPreHeadPurged) {
		// A concurrent release purge deleted the pre-head file mid-promote.
		// The next pass re-reads registry truth: a
		// released segment drops out of Roll; a still-assigned one re-pulls.
		// Explicit even though the wrapped ENOENT already matches the
		// os.ErrNotExist arm below — classification must not depend on how
		// the sentinel is attached.
		return true
	}
	return errors.Is(err, os.ErrNotExist)
}
