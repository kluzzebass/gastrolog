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

// retryableCollectErr reports whether a collect pass failure is an expected
// catch-up race at high ingest: the registry still lists a segment but no peer
// has bytes yet (no holder acks), a holder already purged head/ after seal, or
// a pulled copy failed verification and must be re-pulled from another holder.
// The next vault-ctl publish or leadership wake retries; logging at Warn
// drowns signal. A pass aggregate (SummaryJoin) is retryable only when every
// failure it summarizes is — one terminal failure must surface at Warn.
func retryableCollectErr(err error) bool {
	if err == nil {
		return false
	}
	subs := erragg.Unpack(err)
	if subs == nil {
		subs = []error{err}
	}
	for _, sub := range subs {
		if !retryableCollectSuberr(sub) {
			return false
		}
	}
	return true
}

// retryableCollectSuberr classifies one failure by collection-owned
// sentinels via errors.Is — never by transport status types or error prose,
// so rewording a message elsewhere cannot flip classification
// (gastrolog-466kq5).
func retryableCollectSuberr(err error) bool {
	if errors.Is(err, ErrCorruptSegment) {
		// Checksum verification failed: the serving holder has wrong bytes.
		// The pre-head copy is already discarded; the pull must retry on the
		// manager's own backoff wake (another holder can serve correct
		// bytes) — no future publish event exists to retry it otherwise.
		return true
	}
	if errors.Is(err, ErrSegmentUnavailable) {
		return true
	}
	if errors.Is(err, ErrPreHeadPurged) {
		// A concurrent release purge deleted the pre-head file mid-promote
		// (gastrolog-2as548). The next pass re-reads registry truth: a
		// released segment drops out of Roll; a still-assigned one re-pulls.
		// Explicit even though the wrapped ENOENT already matches the
		// os.ErrNotExist arm below — classification must not depend on how
		// the sentinel is attached.
		return true
	}
	return errors.Is(err, os.ErrNotExist)
}
