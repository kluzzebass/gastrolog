// vaultDist owns one vault's distribution staging state and its invariants:
// completed-segment registration for pull serving, vault-ctl publish with
// bytes-present and retirement guards, and completed-to-head promotion for
// local holders. The worker machinery that feeds it (queues, batching, retry
// backoff) lives in manager.go.
package distribution

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
)

// errPublishBytesMissing is returned when a queued publish runs after local
// segment bytes were purged. The worker must not retry — a stale queue item
// or publish retry after ReleaseSegments would otherwise re-commit metadata
// to vault-ctl without any on-disk copy (permanent collection wedge).
var errPublishBytesMissing = errors.New("segment bytes missing for publish")

// VaultConfig is per-vault distribution state.
type VaultConfig struct {
	// Publisher commits completed-segment metadata to vault-ctl.
	Publisher Publisher
	// LocalHolder reports whether this node holds the vault locally (completed→head rename).
	LocalHolder func() bool
	// OnLocalHeadPromoted fires after a locally-held segment lands in head/.
	// Collection uses this to commit holder receipts without waiting for the
	// next publish wake — publish applies before finalizeAfterPublish promotes.
	OnLocalHeadPromoted func(segmentID glid.GLID)
	// OnPublishCommitted fires after vault-ctl accepts segment metadata and
	// the segment is registered for pull. Wired to wake collection on the
	// same node when origin and home overlap.
	OnPublishCommitted func(segmentID glid.GLID)
}

type vaultDist struct {
	root                string
	log                 *slog.Logger
	publisher           Publisher
	localHolder         func() bool
	onLocalHeadPromoted func(glid.GLID)
	onPublishCommitted  func(glid.GLID)
	mu                  sync.RWMutex
	segments            map[glid.GLID]string   // segment ID → on-disk path
	retired             map[glid.GLID]struct{} // released from vault-ctl; skip rescan republish
	// badHeader remembers completed/ files whose fixed header failed to
	// decode, keyed by segment ID (state, not time): each corrupt file is
	// read and warned about exactly once, not on every rescan wake
	// (gastrolog-faj2yv).
	badHeader map[glid.GLID]struct{}
	// published counts segments this origin committed to the vault-ctl
	// registry (one per successful publish) — the segment-publish stage
	// counter (gastrolog-4r784a). Origin-owned: the leader publishes intent.
	published atomic.Uint64
}

func newVaultDist(root string, cfg VaultConfig, log *slog.Logger) (*vaultDist, error) {
	if cfg.Publisher == nil {
		return nil, errors.New("publisher required")
	}
	if cfg.LocalHolder == nil {
		cfg.LocalHolder = func() bool { return false }
	}
	if log == nil {
		log = slog.Default()
	}
	return &vaultDist{
		root:                root,
		log:                 log,
		publisher:           cfg.Publisher,
		localHolder:         cfg.LocalHolder,
		onLocalHeadPromoted: cfg.OnLocalHeadPromoted,
		onPublishCommitted:  cfg.OnPublishCommitted,
		segments:            make(map[glid.GLID]string),
		retired:             make(map[glid.GLID]struct{}),
		badHeader:           make(map[glid.GLID]struct{}),
	}, nil
}

// prepare stages a completed segment for vault-ctl publish: builds metadata and
// registers the on-disk path for pull serving. Local holders keep the file in
// completed/ until publish succeeds (see finalizeAfterPublish).
//
// alreadyStaged reports that a prior prepare registered this segment — the
// stranded rescan and the completed-channel delivery can race on the same
// segment (the file exists in completed/ before its notification is consumed),
// and only the first staging may enqueue the publish (gastrolog-x5c8ge).
func (v *vaultDist) prepare(seg segmentation.CompletedSegment) (meta Metadata, path string, alreadyStaged bool, err error) {
	v.mu.RLock()
	registered, known := v.segments[seg.SegmentID]
	v.mu.RUnlock()
	if known {
		seg.Path = registered
		meta, err := metadataForPublish(seg)
		return meta, registered, true, err
	}

	path = seg.Path
	meta, err = metadataForPublish(seg)
	if err != nil {
		return Metadata{}, "", false, err
	}

	v.mu.Lock()
	v.segments[seg.SegmentID] = path
	v.mu.Unlock()
	return meta, path, false, nil
}

// finalizeAfterPublish moves a locally-held segment into head/ after vault-ctl
// has committed the registry entry.
func (v *vaultDist) finalizeAfterPublish(segID glid.GLID, path string) error {
	if !v.localHolder() {
		return nil
	}
	dest, err := PromoteToHead(path, v.root)
	if err != nil {
		return err
	}
	v.mu.Lock()
	v.segments[segID] = dest
	v.mu.Unlock()
	if v.onLocalHeadPromoted != nil {
		v.onLocalHeadPromoted(segID)
	}
	return nil
}

func metadataForPublish(seg segmentation.CompletedSegment) (Metadata, error) {
	hdr := seg.Header
	if seg.Path != "" && hdr.IsUnpopulated() {
		// Header-only read: this node finalized and fsynced the file, so a
		// full segment.Open here re-verified every record byte just to fetch
		// counts; the checksum travels in the header for downstream
		// verification instead (gastrolog-faj2yv).
		h, err := segment.ReadHeader(seg.Path)
		if err != nil {
			return Metadata{}, err
		}
		hdr = h
	}
	return metadataFromPath(seg.Path, seg.VaultID, seg.SegmentID, hdr)
}

// stranded returns completed segments on disk that this manager has not
// prepared yet — segments whose channel notification was dropped (burst) or
// that predate this process (restart). Cost is one directory listing plus one
// fixed-header read per unknown segment: a restart backlog must not re-verify
// every byte of every completed file before the first publish
// (gastrolog-faj2yv).
func (v *vaultDist) stranded(vaultID glid.GLID) []segmentation.CompletedSegment {
	ids, err := paths.ListSegmentIDs(paths.CompletedDir(v.root))
	if err != nil {
		v.log.Warn("stranded rescan: reading completed/ failed", "vault", vaultID, "error", err)
		return nil
	}
	// Publish in segment-ID order (GLIDs are time-ordered), matching the old
	// name-sorted directory walk.
	sorted := slices.SortedFunc(maps.Keys(ids), glid.GLID.Compare)
	var out []segmentation.CompletedSegment
	for _, segID := range sorted {
		v.mu.RLock()
		_, known := v.segments[segID]
		_, retired := v.retired[segID]
		_, badHeader := v.badHeader[segID]
		v.mu.RUnlock()
		if known || retired || badHeader {
			continue
		}
		path := paths.CompletedSegment(v.root, segID)
		hdr, err := segment.ReadHeader(path)
		if err != nil {
			if os.IsNotExist(err) {
				// Raced a release purge between the listing and the read; the
				// ID drops out of the next listing on its own.
				continue
			}
			v.mu.Lock()
			v.badHeader[segID] = struct{}{}
			v.mu.Unlock()
			v.log.Warn("stranded rescan: completed segment header unreadable; skipping",
				"vault", vaultID, "segment", segID, "path", path, "error", err)
			continue
		}
		out = append(out, segmentation.CompletedSegment{
			VaultID:   vaultID,
			SegmentID: segID,
			Path:      path,
			Header:    hdr,
		})
	}
	return out
}

func (v *vaultDist) publish(ctx context.Context, seg segmentation.CompletedSegment) error {
	// Synchronous path (PublishCompleted): publishes regardless of prior
	// staging; the FSM treats an identical re-publish as a no-op.
	meta, path, _, err := v.prepare(seg)
	if err != nil {
		return err
	}
	return v.publishStaged(ctx, meta, seg.SegmentID, path)
}

func (v *vaultDist) publishStaged(ctx context.Context, meta Metadata, segID glid.GLID, path string) error {
	if v.isRetired(segID) {
		return nil
	}
	if !v.segmentBytesPresent(segID, path) {
		v.retireSegment(segID)
		v.log.Warn("segment bytes missing at publish; retiring segment",
			"segment", segID, "path", path)
		return errPublishBytesMissing
	}
	if err := v.publisher.Publish(ctx, meta); err != nil {
		return err
	}
	if v.isRetired(segID) {
		return nil
	}
	if err := v.finalizeAfterPublish(segID, path); err != nil {
		return err
	}
	v.published.Add(1)
	if v.onPublishCommitted != nil {
		v.onPublishCommitted(segID)
	}
	return nil
}

func (v *vaultDist) publishStagedBatch(ctx context.Context, items []pendingPublish) error {
	live := make([]pendingPublish, 0, len(items))
	for _, p := range items {
		if v.isRetired(p.segID) {
			continue
		}
		if !v.segmentBytesPresent(p.segID, p.path) {
			// Retire THIS item only. Failing the whole coalesced batch here
			// stranded the surviving batchmates permanently: the batch error
			// was classified non-retryable, the items stayed in v.segments,
			// and the stranded rescan skipped them as known — durable
			// segments invisible to vault-ctl until restart (gastrolog-353kwm).
			v.retireSegment(p.segID)
			v.log.Warn("segment bytes missing at publish; retiring segment",
				"segment", p.segID, "path", p.path)
			continue
		}
		live = append(live, p)
	}
	if len(live) == 0 {
		return nil
	}
	if len(live) == 1 {
		p := live[0]
		return v.publishStaged(ctx, p.meta, p.segID, p.path)
	}
	metas := make([]Metadata, len(live))
	for i, p := range live {
		metas[i] = p.meta
	}
	var err error
	if bp, ok := v.publisher.(BatchPublisher); ok {
		err = bp.PublishBatch(ctx, metas)
	} else {
		for _, meta := range metas {
			if pubErr := v.publisher.Publish(ctx, meta); pubErr != nil {
				err = pubErr
				break
			}
		}
	}
	if err != nil {
		return err
	}
	for _, p := range live {
		if v.isRetired(p.segID) {
			continue
		}
		if err := v.finalizeAfterPublish(p.segID, p.path); err != nil {
			return err
		}
		v.published.Add(1)
		if v.onPublishCommitted != nil {
			v.onPublishCommitted(p.segID)
		}
	}
	return nil
}

func (v *vaultDist) servePull(req PullRequest) error {
	path, ok := v.segmentPathForPull(req.SegmentID)
	if !ok {
		return ErrSegmentNotFound
	}
	return StreamSegment(path, req.Dest)
}

// segmentPathForPull resolves a segment for pull serving. The registered path
// can go stale during completed→head promotion; fall back to the layout dirs.
func (v *vaultDist) segmentPathForPull(segmentID glid.GLID) (string, bool) {
	v.mu.RLock()
	registered, known := v.segments[segmentID]
	v.mu.RUnlock()
	if known {
		if _, err := os.Stat(registered); err == nil {
			return registered, true
		}
	}
	// Probe order: head/, completed/, pre-head/.
	if path, ok := paths.FindSegment(v.root, segmentID,
		paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead); ok {
		v.mu.Lock()
		v.segments[segmentID] = path
		v.mu.Unlock()
		return path, true
	}
	return "", false
}

func (v *vaultDist) isRetired(segID glid.GLID) bool {
	v.mu.RLock()
	_, ok := v.retired[segID]
	v.mu.RUnlock()
	return ok
}

// segmentBytesPresent reports whether this vault still holds the segment in
// staging (completed/, head/, or pre-head/). Publish must not commit vault-ctl
// metadata when bytes are gone — RetireSegments only guards the rescan path,
// not pending queue items or publish retries.
func (v *vaultDist) segmentBytesPresent(segID glid.GLID, path string) bool {
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}
	// Probe order: head/, completed/, pre-head/.
	_, ok := paths.FindSegment(v.root, segID,
		paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead)
	return ok
}

func (v *vaultDist) retireSegment(segID glid.GLID) {
	v.mu.Lock()
	delete(v.segments, segID)
	v.retired[segID] = struct{}{}
	v.mu.Unlock()
}
