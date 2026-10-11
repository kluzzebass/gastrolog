package pipeline

import (
	"log/slog"
	"sync"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// releaseConsumer applies vault-ctl segment release to this node's staging
// copies of one vault's segments. An origin retires the released segments from
// distribution and deletes their completed/ copies; every staging root drops
// its head/ and pre-head/ copies. A release applied in the vault-ctl FSM is the
// only trigger: a segment the FSM has not released is never touched, because
// until then a home may still need to pull it from this origin.
type releaseConsumer struct {
	vaultID glid.GLID
	dist    *distribution.Manager
	logger  *slog.Logger
	// completedRoot is the origin staging root whose completed/ copies are
	// purged; empty when this node does not originate segments for the vault.
	completedRoot string
	headRoots     []string

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
	c.consume(c.releasedOnDisk(fsm))
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

func (c *releaseConsumer) consume(ids []glid.GLID) {
	if len(ids) == 0 {
		return
	}
	if c.completedRoot != "" {
		c.dist.RetireSegments(c.vaultID, ids)
		for _, id := range ids {
			if err := paths.PurgeCompleted(c.completedRoot, id); err != nil {
				c.logger.Warn("released segment: completed/ purge failed",
					"vault", c.vaultID, "segment", id, "error", err)
			}
		}
	}
	for _, id := range ids {
		for _, root := range c.headRoots {
			if err := paths.PurgeHeadStaging(root, id); err != nil {
				c.logger.Warn("released segment: head/ purge failed",
					"vault", c.vaultID, "segment", id, "error", err)
			}
		}
	}
}

// releasedOnDisk lists the segments with a staging copy in a directory this
// consumer purges that fsm records as released.
func (c *releaseConsumer) releasedOnDisk(fsm *vaultctlfsm.FSM) []glid.GLID {
	var dirs []string
	if c.completedRoot != "" {
		dirs = append(dirs, paths.CompletedDir(c.completedRoot))
	}
	for _, root := range c.headRoots {
		dirs = append(dirs, paths.HeadDir(root), paths.PreHeadDir(root))
	}
	seen := make(map[glid.GLID]struct{})
	var released []glid.GLID
	for _, dir := range dirs {
		ids, err := paths.ListSegmentIDs(dir)
		if err != nil {
			c.logger.Warn("released segment catch-up: list staging dir failed",
				"vault", c.vaultID, "dir", dir, "error", err)
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
	return released
}
