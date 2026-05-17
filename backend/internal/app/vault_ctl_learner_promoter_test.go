package app

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Most of the vault-ctl promoter's logic flows through the
// (*raftgroup.Group, hraft.Raft) handle. The promotion of a learner
// requires calling g.Raft.AddVoter() on a real Raft instance — a
// concern that's well-covered by raftgroup's own tests, plus the
// end-to-end k8s verification described in the issue.
//
// The unit tests below focus on the parts that are easy to mock and
// have the most subtle behavior: the peerVaultAppliedIndex helper
// (which has to match VaultStats.Id bytes against a glid.GLID),
// vault-list enumeration, and stale-counter cleanup. The catchup +
// stability machinery is identical to the system-Raft promoter
// (already exhaustively tested in system_raft_learner_promoter_test.go).

// fakePeerStats implements peerStatsReader with configurable returns.
type fakePeerStats struct {
	mu     sync.Mutex
	byNode map[string]*gastrologv1.NodeStats
}

func (f *fakePeerStats) Get(senderID string) *gastrologv1.NodeStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byNode[senderID]
}

// fakeGroupMgr implements vaultCtlRaftGroupAccess. Returns nil for
// every group — sufficient for the tick-iteration tests below, which
// exercise the cfgStore enumeration and counter-cleanup paths without
// needing a real Raft group. Live AddVoter exercise happens in the
// k8s verification.
type fakeGroupMgr struct{}

func (fakeGroupMgr) GetGroup(_ string) *raftgroup.Group { return nil }

func newVaultPromoterForTest(t *testing.T, cfgStore system.Store, ps peerStatsReader) *vaultCtlLearnerPromoter {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newVaultCtlLearnerPromoter(cfgStore, fakeGroupMgr{}, ps, "local-node", logger)
}

func TestPeerVaultAppliedIndex_MatchByGLIDBytes(t *testing.T) {
	t.Parallel()
	target := glid.New()
	other := glid.New()

	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{
		"peer": {Vaults: []*gastrologv1.VaultStats{
			vaultStatsByID(other, 50),
			vaultStatsByID(target, 100),
			vaultStatsByID(glid.New(), 200),
		}},
	}}

	got, ok := peerVaultAppliedIndex(ps, "peer", target)
	if !ok {
		t.Fatalf("expected target found, got ok=false")
	}
	if got != 100 {
		t.Fatalf("expected applied=100, got %d", got)
	}
}

func TestPeerVaultAppliedIndex_NoPeerEntry(t *testing.T) {
	t.Parallel()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{}}
	if _, ok := peerVaultAppliedIndex(ps, "ghost", glid.New()); ok {
		t.Fatal("expected ok=false for missing peer")
	}
}

func TestPeerVaultAppliedIndex_PeerHasNoVaultEntry(t *testing.T) {
	t.Parallel()
	target := glid.New()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{
		"peer": {Vaults: []*gastrologv1.VaultStats{
			vaultStatsByID(glid.New(), 100),
			vaultStatsByID(glid.New(), 200),
		}},
	}}
	if _, ok := peerVaultAppliedIndex(ps, "peer", target); ok {
		t.Fatal("expected ok=false when target vault not present in peer's snapshot")
	}
}

// TestPeerVaultAppliedIndex_NilVaultEntryTolerated guards against
// stats slices containing nil entries (broadcast race during slice
// append). The helper iterates safely.
func TestPeerVaultAppliedIndex_NilVaultEntryTolerated(t *testing.T) {
	t.Parallel()
	target := glid.New()
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{
		"peer": {Vaults: []*gastrologv1.VaultStats{
			nil,
			vaultStatsByID(target, 100),
			nil,
		}},
	}}
	got, ok := peerVaultAppliedIndex(ps, "peer", target)
	if !ok || got != 100 {
		t.Fatalf("expected (100, true), got (%d, %v)", got, ok)
	}
}

// TestVaultCtlPromoter_Tick_NoGroupsNoOp verifies the tick is safe to
// run when no vault has a Raft group available (placement excludes
// this node from every vault). fakeGroupMgr always returns nil from
// GetGroup; tick should drain quietly.
func TestVaultCtlPromoter_Tick_NoGroupsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()
	if err := store.PutVault(ctx, system.VaultConfig{ID: glid.New(), Name: "v1", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if err := store.PutVault(ctx, system.VaultConfig{ID: glid.New(), Name: "v2", Type: system.VaultTypeMemory}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	ps := &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{}}

	p := newVaultPromoterForTest(t, store, ps)
	p.tick(ctx) // should not panic, should not add any catchup tick entries

	if len(p.catchupTicks) != 0 {
		t.Fatalf("expected empty catchupTicks, got %v", p.catchupTicks)
	}
}

// TestVaultCtlPromoter_StaleCounterPruning verifies that catchup-tick
// entries are cleaned when their (vault, node) tuple no longer
// appears in the current configuration. Achieved here by seeding
// catchupTicks manually (since fakeGroupMgr returns nil and thus
// never adds entries) and observing the post-tick deletion.
func TestVaultCtlPromoter_StaleCounterPruning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()
	p := newVaultPromoterForTest(t, store, &fakePeerStats{byNode: map[string]*gastrologv1.NodeStats{}})

	// Pre-seed counter entries that won't appear in this tick's seen
	// set (no vaults configured).
	p.catchupTicks[catchupKey{vaultID: glid.New(), nodeID: "ghost"}] = 1
	p.catchupTicks[catchupKey{vaultID: glid.New(), nodeID: "ghost2"}] = 2

	p.tick(ctx)

	if len(p.catchupTicks) != 0 {
		t.Fatalf("expected stale counters pruned, got %v", p.catchupTicks)
	}
}

// TestStartVaultCtlLearnerPromoter_RegistersOperatorVisibleJob
// verifies the promoter ships as a proper scheduled job: name + cron
// set, a non-empty Describe text so the inspector shows context to
// the operator, and the captured task drives a real tick.
func TestStartVaultCtlLearnerPromoter_RegistersOperatorVisibleJob(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	// One vault in the store so tickOnce → tick has something to iterate.
	v := system.VaultConfig{ID: glid.New(), Name: "v1"}
	if err := store.PutVault(context.Background(), v); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	ps := &mockPeerStats{}
	p := newVaultPromoterForTest(t, store, ps)
	sched := &fakeScheduler{}

	if err := startVaultCtlLearnerPromoter(context.Background(), sched, p); err != nil {
		t.Fatalf("startVaultCtlLearnerPromoter: %v", err)
	}
	if sched.addJobName != vaultCtlLearnerPromoterJobName {
		t.Errorf("AddJob name: got %q, want %q", sched.addJobName, vaultCtlLearnerPromoterJobName)
	}
	if sched.addJobCron != vaultCtlLearnerPromoterSchedule {
		t.Errorf("AddJob cron: got %q, want %q", sched.addJobCron, vaultCtlLearnerPromoterSchedule)
	}
	if sched.describeMessage == "" {
		t.Error("Describe message empty — operator inspector will show no context")
	}

	// Run the captured task — fakeGroupMgr returns nil for GetGroup so
	// the per-vault evaluation short-circuits before any AddVoter
	// attempt. The task should still execute end-to-end without panic.
	if task, ok := sched.addJobTaskFn.(func()); ok {
		task()
	} else {
		t.Fatalf("expected captured task of type func(), got %T", sched.addJobTaskFn)
	}
}

// TestStartVaultCtlLearnerPromoter_PropagatesAddJobError verifies the
// caller sees an AddJob failure (e.g. duplicate name).
func TestStartVaultCtlLearnerPromoter_PropagatesAddJobError(t *testing.T) {
	t.Parallel()
	store := sysmem.NewStore()
	ps := &mockPeerStats{}
	p := newVaultPromoterForTest(t, store, ps)
	sched := &fakeScheduler{addJobErr: errFakeMember}

	if err := startVaultCtlLearnerPromoter(context.Background(), sched, p); err == nil {
		t.Fatal("expected AddJob error to propagate")
	}
}
