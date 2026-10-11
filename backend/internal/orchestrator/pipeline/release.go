package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// PurgeReleasedStaging deletes the staging copies under roots of every segment
// fsm records as released: the on-disk catch-up for releases whose event this
// node did not consume. covered is the release sequence read before the
// listing, so when err is nil no segment released at or before covered has a
// staging copy under roots. purged is every released segment the listing
// found. err joins every listing and delete that failed; the pass then
// accounts for nothing.
func PurgeReleasedStaging(fsm *vaultctlfsm.FSM, roots []string) (covered uint64, purged []glid.GLID, err error) {
	covered = fsm.ReleaseSeq()
	purged, listErr := releasedOnDisk(fsm, roots)
	return covered, purged, errors.Join(listErr, purgeReleased(roots, purged))
}

// purgeReleased deletes the staging copies (completed/, head/, pre-head/) of
// ids under every root. It is the only deletion of released segments' staging
// copies; ids must be released in the vault-ctl FSM. Registry absence is not
// release: a completed segment awaiting its publish is registry-absent too,
// and deleting it loses ingested records.
func purgeReleased(roots []string, ids []glid.GLID) error {
	var errs []error
	for _, id := range ids {
		for _, root := range roots {
			if err := paths.PurgeSegmentStaging(root, id); err != nil {
				errs = append(errs, fmt.Errorf("purge segment %s under %s: %w", id, root, err))
			}
		}
	}
	return errors.Join(errs...)
}

// releasedOnDisk lists the segments with a staging copy under roots that fsm
// records as released. A staging directory that does not exist holds nothing.
func releasedOnDisk(fsm *vaultctlfsm.FSM, roots []string) ([]glid.GLID, error) {
	seen := make(map[glid.GLID]struct{})
	var released []glid.GLID
	var errs []error
	for _, root := range roots {
		for _, dir := range []string{paths.CompletedDir(root), paths.HeadDir(root), paths.PreHeadDir(root)} {
			ids, err := paths.ListSegmentIDs(dir)
			if err != nil {
				if !os.IsNotExist(err) {
					errs = append(errs, fmt.Errorf("list %s: %w", dir, err))
				}
				continue
			}
			for id := range ids {
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
				if fsm.SegmentReleased(id) {
					released = append(released, id)
				}
			}
		}
	}
	return released, errors.Join(errs...)
}

// releaseConsumer applies vault-ctl segment release to this node's staging
// copies of one vault's segments: an origin retires the released segments
// from distribution, and every staging root drops its copies. A release
// applied in the vault-ctl FSM is the only trigger: a segment the FSM has not
// released is never touched, because until then a home may still need to pull
// it from this origin.
type releaseConsumer struct {
	vaultID glid.GLID
	// dist is nil when this node does not originate segments for the vault.
	dist   *distribution.Manager
	logger *slog.Logger
	roots  []string

	mu    sync.Mutex
	fsm   *vaultctlfsm.FSM
	unsub func()
}

// bind subscribes the consumer to fsm's release events, then consumes every
// segment fsm already records as released that still has a staging copy here.
// Releases applied before the subscription — while this node was down, before
// the vault registered, or carried in by a snapshot install that replaced the
// FSM — fire no event on this subscription, so the on-disk catch-up is what
// reclaims them. Rebinding to the FSM already bound is a no-op.
func (c *releaseConsumer) bind(fsm *vaultctlfsm.FSM) {
	c.mu.Lock()
	if c.fsm == fsm {
		c.mu.Unlock()
		return
	}
	if c.unsub != nil {
		c.unsub()
	}
	c.fsm = fsm
	c.unsub = fsm.AddOnReleaseSegments(c.consume)
	c.mu.Unlock()
	released, err := releasedOnDisk(fsm, c.roots)
	if err != nil {
		c.logger.Warn("released segment catch-up: staging listing incomplete",
			"vault", c.vaultID, "error", err)
	}
	c.consume(released)
}

// unbindIfSet drops the release subscription; a nil consumer is a no-op.
func (c *releaseConsumer) unbindIfSet() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unsub != nil {
		c.unsub()
	}
	c.unsub = nil
	c.fsm = nil
}

// consume retires ids from distribution, so its completed/ rescan does not
// republish them, and then purges them. A failed delete stays on disk for the
// released-segment sweep, which confirms nothing until the copy is gone.
func (c *releaseConsumer) consume(ids []glid.GLID) {
	if len(ids) == 0 {
		return
	}
	if c.dist != nil {
		c.dist.RetireSegments(c.vaultID, ids)
	}
	if err := purgeReleased(c.roots, ids); err != nil {
		c.logger.Warn("released segment: staging purge failed",
			"vault", c.vaultID, "error", err)
	}
}
