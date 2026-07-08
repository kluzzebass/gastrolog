package orchestrator

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func mootsFixture(t *testing.T, vaultID glid.GLID, disposition string, rules []retentionRule) *Orchestrator {
	t.Helper()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	runner := &retentionRunner{vaultID: vaultID, disposition: disposition, rules: rules}
	o.mu.Lock()
	if o.retention == nil {
		o.retention = make(map[string]*retentionRunner)
	}
	o.retention[vaultID.String()+":s1"] = runner
	o.mu.Unlock()
	return o
}

// TestRetentionMootsPull pins the catch-up skip gate: a chunk past its TTL
// window on a delete-disposition vault is a pointless pull; everything else
// must still recover. The route veto is cardinal-rule adjacent — route
// fan-out reads the leader's local copy before destruction, so skipping the
// pull there could destroy records unrouted.
func TestRetentionMootsPull(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	expired := vaultctlfsm.ManifestEntry{
		ID:       chunk.NewChunkID(),
		State:    chunk.ChunkStateSealed,
		SealedAt: time.Now().Add(-time.Hour),
	}
	fresh := vaultctlfsm.ManifestEntry{
		ID:       chunk.NewChunkID(),
		State:    chunk.ChunkStateSealed,
		SealedAt: time.Now().Add(-time.Second),
	}
	ttl := []retentionRule{{policy: chunk.NewTTLRetentionPolicy(3 * time.Minute)}}

	t.Run("expired chunk on delete disposition is moot", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionDelete, ttl)
		if !o.retentionMootsPull(vaultID, expired) {
			t.Fatal("expired chunk under TTL+delete must skip the pull")
		}
	})

	t.Run("fresh chunk still pulls", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionDelete, ttl)
		if o.retentionMootsPull(vaultID, fresh) {
			t.Fatal("chunk inside its window must not be skipped")
		}
	})

	t.Run("route disposition vetoes the skip", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionRoute, ttl)
		if o.retentionMootsPull(vaultID, expired) {
			t.Fatal("route disposition must always pull — fan-out needs local bytes")
		}
	})

	t.Run("size-only rules cannot judge a single chunk", func(t *testing.T) {
		size := []retentionRule{{policy: chunk.NewSizeRetentionPolicy(1)}}
		o := mootsFixture(t, vaultID, system.RetentionDispositionDelete, size)
		if o.retentionMootsPull(vaultID, expired) {
			t.Fatal("non-TTL policies must not participate in the skip")
		}
	})

	t.Run("no runners means no opinion", func(t *testing.T) {
		o := newTestOrch(t, Config{LocalNodeID: "node-A"})
		if o.retentionMootsPull(vaultID, expired) {
			t.Fatal("without retention runners the pull must proceed")
		}
	})

	t.Run("zero SealedAt never skips", func(t *testing.T) {
		o := mootsFixture(t, vaultID, system.RetentionDispositionDelete, ttl)
		if o.retentionMootsPull(vaultID, vaultctlfsm.ManifestEntry{ID: chunk.NewChunkID(), State: chunk.ChunkStateSealed}) {
			t.Fatal("no anchor, no skip")
		}
	})
}

// TestRetentionFanOutAbortsUnderDiskProtect pins the cardinal-rule fix from
// the disk-guard live test: with admission rejecting everything, route
// fan-out must ABORT (chunk retained, retried later) — the old tolerate-
// per-record-errors path destroyed a chunk after 1.5M rejected records
// delivered nothing.
func TestRetentionFanOutAbortsUnderDiskProtect(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"a": 1 * gib, "b": 1 * gib})
	g.evaluate(nil) // engages protect
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	o.diskGuard = g

	r := &retentionRunner{
		vaultID: glid.New(),
		orch:    o,
		logger:  slog.Default(),
		idleLog: logging.Throttle{Interval: time.Minute},
	}
	if done := r.fireRetentionEvent(chunk.NewChunkID()); done {
		t.Fatal("fan-out under disk protect must abort so the chunk survives for a later sweep")
	}
}
