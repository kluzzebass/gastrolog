package orchestrator

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/system"
)

// reasonSweepFixture is a source vault of four sealed chunks A<B<C<D (oldest
// first) holding 1, 2, 4 and 8 records, so a destination's matched-record
// count names exactly which chunks reached it. The routing table sends
// retention output to one archive vault per `_reason` value.
type reasonSweepFixture struct {
	orch     *Orchestrator
	cm       chunk.ChunkManager
	sourceID glid.GLID
	archives map[chunk.RetentionReason]glid.GLID
	chunks   []chunk.ChunkID // A, B, C, D
	now      time.Time
}

var reasonSweepRecordCounts = []int{1, 2, 4, 8}

func newReasonSweepFixture(t *testing.T) reasonSweepFixture {
	t.Helper()
	sourceID := glid.New()
	archives := map[chunk.RetentionReason]glid.GLID{
		chunk.RetentionReasonAge:          glid.New(),
		chunk.RetentionReasonSize:         glid.New(),
		chunk.RetentionReasonCount:        glid.New(),
		chunk.RetentionReasonMemoryBudget: glid.New(),
	}
	var routes []*routing.Route
	for reason, archiveID := range archives {
		expr := fmt.Sprintf(`_source="retention" AND _vault=%q AND _reason=%q`, sourceID.String(), reason)
		r, err := routing.CompileRoute(glid.New(), "retain-"+string(reason), 0, expr, []glid.GLID{archiveID})
		if err != nil {
			t.Fatalf("CompileRoute %s: %v", reason, err)
		}
		routes = append(routes, r)
	}
	orch := newRoutingOrch(t, routing.NewTable(routes))

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := base
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		Now:            func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("source CM: %v", err)
	}
	orch.RegisterVault(NewVaultFromComponents(sourceID, cm, nil, nil))

	var chunks []chunk.ChunkID
	for c, n := range reasonSweepRecordCounts {
		clock = base.Add(time.Duration(c) * time.Hour)
		var id chunk.ChunkID
		for i := range n {
			rec := chunk.Record{Attrs: chunk.Attributes{"i": fmt.Sprint(i)}, Raw: []byte("record")}
			cid, _, err := cm.Append(rec)
			if err != nil {
				t.Fatalf("Append chunk %d: %v", c, err)
			}
			id = cid
		}
		if err := cm.Seal(); err != nil {
			t.Fatalf("Seal chunk %d: %v", c, err)
		}
		chunks = append(chunks, id)
	}
	return reasonSweepFixture{
		orch:     orch,
		cm:       cm,
		sourceID: sourceID,
		archives: archives,
		chunks:   chunks,
		now:      base.Add(3*time.Hour + 30*time.Minute),
	}
}

func (f reasonSweepFixture) runner(disposition string) *retentionRunner {
	return &retentionRunner{
		vaultID:     f.sourceID,
		cm:          f.cm,
		im:          &retentionFakeIndexManager{},
		isLeader:    true,
		orch:        f.orch,
		logger:      slog.Default(),
		now:         func() time.Time { return f.now },
		disposition: disposition,
		inflight:    make(map[chunk.ChunkID]bool),
	}
}

// policies returns the three triggers, each selecting a different oldest
// prefix of A<B<C<D:
//
//	age   (maxAge 3h, now = D + 30m)   selects A
//	size  (budget = claim(C)+claim(D)) selects A, B
//	count (maxChunks 1)                selects A, B, C
//
// With precedence age > size > count, A carries `age`, B `size`, C `count`.
func (f reasonSweepFixture) policies(t *testing.T, r *retentionRunner) (age, size, count system.RetentionPolicyConfig) {
	t.Helper()
	metas, err := f.cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	claims := r.chunkDiskClaims(metas)
	budget := claims[f.chunks[2]] + claims[f.chunks[3]]
	if budget <= 0 || claims[f.chunks[0]] <= 0 {
		t.Fatalf("fixture needs positive disk claims, got %v", claims)
	}
	maxChunks := int64(1)
	age = system.RetentionPolicyConfig{ID: glid.New(), MaxAge: strPtr("3h")}
	size = system.RetentionPolicyConfig{ID: glid.New(), MaxSize: strPtr(fmt.Sprintf("%dB", budget))}
	count = system.RetentionPolicyConfig{ID: glid.New(), MaxChunks: &maxChunks}
	return age, size, count
}

func toRule(t *testing.T, cfgs ...system.RetentionPolicyConfig) retentionRule {
	t.Helper()
	merged := system.RetentionPolicyConfig{ID: glid.New()}
	for _, c := range cfgs {
		if c.MaxAge != nil {
			merged.MaxAge = c.MaxAge
		}
		if c.MaxSize != nil {
			merged.MaxSize = c.MaxSize
		}
		if c.MaxChunks != nil {
			merged.MaxChunks = c.MaxChunks
		}
	}
	p, err := merged.ToRetentionPolicy()
	if err != nil || p == nil {
		t.Fatalf("ToRetentionPolicy: %v (policy=%v)", err, p)
	}
	return retentionRule{policy: p}
}

// assertReasonRouting asserts each archive received exactly the records of
// the chunks its `_reason` names. A retention submit returns only after
// routing has counted the record, so the counters are final once the sweep
// returns.
func (f reasonSweepFixture) assertReasonRouting(t *testing.T, want map[chunk.RetentionReason]int64) {
	t.Helper()
	stats := f.orch.VaultRouteStatsList()
	for reason, archiveID := range f.archives {
		var got int64
		if s := stats[archiveID]; s != nil {
			got = s.Matched
		}
		if got != want[reason] {
			t.Errorf("_reason=%q archive matched %d records, want %d", reason, got, want[reason])
		}
	}
	if s := f.orch.GetRouteStats(); s.Unmatched != 0 {
		t.Errorf("every retention record must match its reason's route, got %d unmatched", s.Unmatched)
	}
}

// wantByTrigger is the matched-record count per archive when A, B and C are
// retained by age, size and count respectively (1, 2 and 4 records).
var wantByTrigger = map[chunk.RetentionReason]int64{
	chunk.RetentionReasonAge:   1,
	chunk.RetentionReasonSize:  2,
	chunk.RetentionReasonCount: 4,
}

// TestSweepRoutesEachChunkUnderItsTrigger drives one policy stating all three
// triggers — the composite a vault gets from a single retention policy with
// maxAge, maxSize and maxChunks — and asserts every chunk's records route
// under the trigger that selected it, the preceding one where several did.
func TestSweepRoutesEachChunkUnderItsTrigger(t *testing.T) {
	t.Parallel()
	f := newReasonSweepFixture(t)
	r := f.runner(system.RetentionDispositionRoute)
	age, size, count := f.policies(t, r)

	r.sweep([]retentionRule{toRule(t, age, size, count)})

	f.assertReasonRouting(t, wantByTrigger)
}

// TestSweepReasonIndependentOfRuleOrder attaches the triggers as three
// separate retention policies listed count, size, age: a chunk two policies
// select still carries the preceding reason rather than the first-listed
// policy's.
func TestSweepReasonIndependentOfRuleOrder(t *testing.T) {
	t.Parallel()
	f := newReasonSweepFixture(t)
	r := f.runner(system.RetentionDispositionRoute)
	age, size, count := f.policies(t, r)

	r.sweep([]retentionRule{toRule(t, count), toRule(t, size), toRule(t, age)})

	f.assertReasonRouting(t, wantByTrigger)
}

// TestSweepSingleTriggerReason pins each trigger on its own: every chunk it
// selects routes under its reason and no other archive receives anything.
func TestSweepSingleTriggerReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		reason chunk.RetentionReason
		pick   func(age, size, count system.RetentionPolicyConfig) system.RetentionPolicyConfig
		want   int64
	}{
		{chunk.RetentionReasonAge, func(a, _, _ system.RetentionPolicyConfig) system.RetentionPolicyConfig { return a }, 1},
		{chunk.RetentionReasonSize, func(_, s, _ system.RetentionPolicyConfig) system.RetentionPolicyConfig { return s }, 1 + 2},
		{chunk.RetentionReasonCount, func(_, _, c system.RetentionPolicyConfig) system.RetentionPolicyConfig { return c }, 1 + 2 + 4},
	}
	for _, c := range cases {
		t.Run(string(c.reason), func(t *testing.T) {
			t.Parallel()
			f := newReasonSweepFixture(t)
			r := f.runner(system.RetentionDispositionRoute)
			age, size, count := f.policies(t, r)

			r.sweep([]retentionRule{toRule(t, c.pick(age, size, count))})

			f.assertReasonRouting(t, map[chunk.RetentionReason]int64{c.reason: c.want})
		})
	}
}

// TestSweepDeleteDispositionRoutesNoReason pins that the reason changes
// nothing for the delete disposition: the routing engine is never invoked.
func TestSweepDeleteDispositionRoutesNoReason(t *testing.T) {
	t.Parallel()
	f := newReasonSweepFixture(t)
	r := f.runner(system.RetentionDispositionDelete)
	age, size, count := f.policies(t, r)

	r.sweep([]retentionRule{toRule(t, age, size, count)})

	assertNoRetentionFanOut(t, f.orch, "delete disposition with reason routes")
}

// TestMemoryBudgetDrainRoutesUnderMemoryBudgetReason pins that a memory
// vault's budget enforcement, which fires retention events outside any
// retention policy, routes its records under `_reason="memory-budget"`.
func TestMemoryBudgetDrainRoutesUnderMemoryBudgetReason(t *testing.T) {
	t.Parallel()
	f := newReasonSweepFixture(t)
	r := f.runner(system.RetentionDispositionRoute)

	f.orch.drainExcessChunks(r, f.cm, 1)

	f.assertReasonRouting(t, map[chunk.RetentionReason]int64{chunk.RetentionReasonMemoryBudget: 1})
}
