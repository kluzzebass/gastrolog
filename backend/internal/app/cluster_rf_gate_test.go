package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Tests for the RF-preservation gate (gastrolog-3vyex): a node removal
// that would leave a vault with fewer surviving placements than its
// configured replication factor, and nowhere to re-place, is refused for
// operator-driven removal and allowed for preStop self-removal.
//
// The gate is built from pure helpers over a system.Store, so the
// eligibility arithmetic is testable without standing up a cluster; the
// serializer and the policy split are exercised through nodeRemover
// (the production type wired into makeRemoveNodeFunc) below.

// rfTestCluster is a small builder for gate fixtures: named nodes with
// lifecycle state and optional file storages, plus vaults with
// placements.
type rfTestCluster struct {
	t     *testing.T
	store *sysmem.Store
	ids   map[string]glid.GLID
}

func newRFTestCluster(t *testing.T) *rfTestCluster {
	t.Helper()
	return &rfTestCluster{t: t, store: sysmem.NewStore(), ids: map[string]glid.GLID{}}
}

// node registers a cluster member in the given lifecycle state.
func (c *rfTestCluster) node(name string, state system.NodeState) glid.GLID {
	c.t.Helper()
	id := glid.New()
	c.ids[name] = id
	cfg := system.NodeConfig{ID: id, Name: name}
	if err := c.store.PutNode(context.Background(), cfg); err != nil {
		c.t.Fatalf("PutNode %s: %v", name, err)
	}
	// Nodes are created Live (Unknown maps to Live via EffectiveState);
	// anything else is an explicit transition, mirroring the real
	// lifecycle path.
	if state != system.NodeStateLive && state != system.NodeStateUnknown {
		c.setState(name, state)
	}
	return id
}

// setState transitions a node, walking the legal path to Decommissioning.
func (c *rfTestCluster) setState(name string, state system.NodeState) {
	c.t.Helper()
	ctx := context.Background()
	id := c.ids[name]
	if state == system.NodeStateDecommissioning {
		if err := c.store.SetNodeState(ctx, id, system.NodeStateDraining, time.Now()); err != nil {
			c.t.Fatalf("SetNodeState %s Draining: %v", name, err)
		}
	}
	if err := c.store.SetNodeState(ctx, id, state, time.Now()); err != nil {
		c.t.Fatalf("SetNodeState %s %v: %v", name, state, err)
	}
}

// storage gives a node one file storage of the given class and returns
// its storage ID.
func (c *rfTestCluster) storage(name string, class uint32) string {
	c.t.Helper()
	sid := glid.New()
	if err := c.store.SetNodeStorageConfig(context.Background(), system.NodeStorageConfig{
		NodeID: c.ids[name].String(),
		FileStorages: []system.FileStorage{
			{ID: sid, StorageClass: class, Name: "fs-" + name, Path: "/data/" + name},
		},
	}); err != nil {
		c.t.Fatalf("SetNodeStorageConfig %s: %v", name, err)
	}
	return sid.String()
}

// memVault places a memory vault on the named nodes (first is leader).
func (c *rfTestCluster) memVault(name string, rf uint32, nodes ...string) glid.GLID {
	c.t.Helper()
	id := glid.New()
	if err := c.store.PutVault(context.Background(), system.VaultConfig{
		ID: id, Name: name, Type: system.VaultTypeMemory, ReplicationFactor: rf,
	}); err != nil {
		c.t.Fatalf("PutVault %s: %v", name, err)
	}
	placements := make([]system.VaultPlacement, 0, len(nodes))
	for i, n := range nodes {
		placements = append(placements, system.VaultPlacement{
			StorageID: system.SyntheticStorageID(c.ids[n].String()),
			Leader:    i == 0,
		})
	}
	c.place(id, placements)
	return id
}

// fileVault places a file vault of the given storage class on the listed
// storage IDs (first is leader).
func (c *rfTestCluster) fileVault(name string, rf, class uint32, storageIDs ...string) glid.GLID {
	c.t.Helper()
	id := glid.New()
	if err := c.store.PutVault(context.Background(), system.VaultConfig{
		ID: id, Name: name, Type: system.VaultTypeFile, StorageClass: class, ReplicationFactor: rf,
	}); err != nil {
		c.t.Fatalf("PutVault %s: %v", name, err)
	}
	placements := make([]system.VaultPlacement, 0, len(storageIDs))
	for i, sid := range storageIDs {
		placements = append(placements, system.VaultPlacement{StorageID: sid, Leader: i == 0})
	}
	c.place(id, placements)
	return id
}

func (c *rfTestCluster) place(vaultID glid.GLID, placements []system.VaultPlacement) {
	c.t.Helper()
	if err := c.store.SetVaultPlacements(context.Background(), vaultID, placements); err != nil {
		c.t.Fatalf("SetVaultPlacements: %v", err)
	}
}

// below runs the gate helper for the named target node.
func (c *rfTestCluster) below(target string) []degradedVault {
	c.t.Helper()
	return vaultsBelowRFAfterRemoval(context.Background(), c.store, c.ids[target].String())
}

// ---------------------------------------------------------------------
// Eligibility arithmetic
// ---------------------------------------------------------------------

// A 3-node cluster at RF=3 has no spare: removing any node drops the
// vault to two surviving placements with nothing to re-place onto.
func TestVaultsBelowRF_ThreeNodesRF3_NoSpare(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	for _, target := range []string{"a", "b", "c"} {
		degraded := c.below(target)
		if len(degraded) != 1 {
			t.Fatalf("removing %s: expected 1 degraded vault, got %v", target, degraded)
		}
		d := degraded[0]
		if d.Name != "triple" || d.RF != 3 || d.Surviving != 2 || d.Eligible != 0 {
			t.Fatalf("removing %s: unexpected degraded vault %+v", target, d)
		}
	}
}

// A 4th eligible Live node is a re-placement target, so the same RF=3
// removal is allowed — the placement manager backfills onto it.
func TestVaultsBelowRF_FourNodesRF3_EligibleSpare(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.node("d", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	if degraded := c.below("a"); len(degraded) != 0 {
		t.Fatalf("expected removal allowed with an eligible spare, got %v", degraded)
	}
}

// The spare only counts when it is Live. Every soft-offline and
// in-transition state is excluded, matching the placement manager, which
// will not put new members on those nodes either.
func TestVaultsBelowRF_SpareMustBeLive(t *testing.T) {
	t.Parallel()
	for _, state := range []system.NodeState{
		system.NodeStateUnreachable,
		system.NodeStateMaintenance,
		system.NodeStateDraining,
		system.NodeStateDecommissioning,
	} {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			c := newRFTestCluster(t)
			c.node("a", system.NodeStateLive)
			c.node("b", system.NodeStateLive)
			c.node("c", system.NodeStateLive)
			c.node("d", state)
			c.memVault("triple", 3, "a", "b", "c")

			degraded := c.below("a")
			if len(degraded) != 1 {
				t.Fatalf("spare in state %v must not count as eligible, got %v", state, degraded)
			}
			if degraded[0].Eligible != 0 {
				t.Fatalf("eligible count with %v spare: got %d, want 0", state, degraded[0].Eligible)
			}
		})
	}
}

// Eligibility follows the placement manager's rule, not just liveness: a
// Live spare whose storage class does not match the vault cannot host a
// replica, so the removal is still refused.
func TestVaultsBelowRF_SpareLacksStorageClass(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.node("d", system.NodeStateLive)
	sa := c.storage("a", 1)
	sb := c.storage("b", 1)
	sc := c.storage("c", 1)
	c.storage("d", 2) // wrong class for the vault
	c.fileVault("files", 3, 1, sa, sb, sc)

	degraded := c.below("a")
	if len(degraded) != 1 || degraded[0].Eligible != 0 {
		t.Fatalf("class-mismatched spare must not count as eligible, got %v", degraded)
	}
}

// Same topology, matching class: the spare counts and the removal is
// allowed. Pins that the class check is the discriminator above, not
// file vaults being categorically refused.
func TestVaultsBelowRF_SpareWithMatchingStorageClass(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.node("d", system.NodeStateLive)
	sa := c.storage("a", 1)
	sb := c.storage("b", 1)
	sc := c.storage("c", 1)
	c.storage("d", 1)
	c.fileVault("files", 3, 1, sa, sb, sc)

	if degraded := c.below("a"); len(degraded) != 0 {
		t.Fatalf("expected removal allowed with a class-matched spare, got %v", degraded)
	}
}

// Redundancy that survives without the target is not the gate's
// business: RF=2 with three placements loses one and still has two.
func TestVaultsBelowRF_RFSatisfiedWithoutTarget(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("pair", 2, "a", "b", "c")

	if degraded := c.below("a"); len(degraded) != 0 {
		t.Fatalf("expected no degradation when RF is met without the target, got %v", degraded)
	}
}

// A vault two members short of RF with a single spare is still below RF
// after re-placement — the gate counts eligible nodes rather than
// stopping at "at least one exists".
func TestVaultsBelowRF_OneSpareCannotCoverTwoMissing(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("d", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b") // already under-replicated

	degraded := c.below("a")
	if len(degraded) != 1 {
		t.Fatalf("expected refusal: 1 surviving + 1 eligible < RF 3, got %v", degraded)
	}
	if degraded[0].Surviving != 1 || degraded[0].Eligible != 1 {
		t.Fatalf("unexpected arithmetic: %+v", degraded[0])
	}
}

// Unset RF means a single copy: removing one of two holders leaves one,
// which satisfies RF. (Removing the sole holder is the orphan gate's
// case, not this one.)
func TestVaultsBelowRF_UnsetRFMeansOneCopy(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.memVault("plain", 0, "a", "b")

	if degraded := c.below("a"); len(degraded) != 0 {
		t.Fatalf("unset RF must mean one copy, got %v", degraded)
	}
}

// A placement on a node that is no longer a cluster member is a
// leftover, not a surviving copy. Without this, a memory vault's
// synthetic storage ID would keep resolving to a departed node and the
// gate would count data that no longer exists.
func TestVaultsBelowRF_DepartedMemberDoesNotCountAsSurviving(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	gone := c.node("gone", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "gone")

	// Node "gone" leaves the cluster; its placement entry lingers until
	// the placement manager reconciles.
	if err := c.store.DeleteNode(ctx, gone); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	degraded := c.below("a")
	if len(degraded) != 1 {
		t.Fatalf("expected refusal, got %v", degraded)
	}
	if degraded[0].Surviving != 1 {
		t.Fatalf("departed member counted as surviving: %+v", degraded[0])
	}
}

// Per-vault evaluation: a healthy vault in the same store does not drag
// an affected one into the result, and vice versa.
func TestVaultsBelowRF_PerVault(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")
	c.memVault("pair", 2, "a", "b", "c")

	degraded := c.below("a")
	if len(degraded) != 1 || degraded[0].Name != "triple" {
		t.Fatalf("expected only the RF=3 vault to be reported, got %v", degraded)
	}
}

// Empty store, and a vault with no placements at all, are both
// no-degradation: there is no redundancy to preserve.
func TestVaultsBelowRF_EmptyAndUnplaced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sysmem.NewStore()
	if got := vaultsBelowRFAfterRemoval(ctx, store, "node-A"); len(got) != 0 {
		t.Fatalf("empty store: got %v", got)
	}
	if got := vaultsBelowRFAfterRemoval(ctx, nil, "node-A"); len(got) != 0 {
		t.Fatalf("nil store: got %v", got)
	}

	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	if err := c.store.PutVault(ctx, system.VaultConfig{
		ID: glid.New(), Name: "ghost", Type: system.VaultTypeMemory, ReplicationFactor: 3,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	if got := c.below("a"); len(got) != 0 {
		t.Fatalf("unplaced vault: got %v", got)
	}
}

// JSONL vaults are pinned to their path's node, so no other node is ever
// an eligible replacement — the arithmetic reports zero spares rather
// than pretending a Live node could take over.
func TestVaultsBelowRF_JSONLVaultHasNoSpare(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	vaultID := glid.New()
	if err := c.store.PutVault(ctx, system.VaultConfig{
		ID: vaultID, Name: "jsonl", Type: system.VaultTypeJSONL, ReplicationFactor: 2,
	}); err != nil {
		t.Fatalf("PutVault: %v", err)
	}
	c.place(vaultID, []system.VaultPlacement{
		{StorageID: system.SyntheticStorageID(c.ids["a"].String()), Leader: true},
		{StorageID: system.SyntheticStorageID(c.ids["b"].String()), Leader: false},
	})

	degraded := c.below("b")
	if len(degraded) != 1 || degraded[0].Eligible != 0 {
		t.Fatalf("JSONL vault must report no eligible spare, got %v", degraded)
	}
}

// TestRFRefusalError_Format pins the operator-actionable shape: the
// sentinel is wrapped (so callers can errors.Is it), every affected
// vault is named with its ID and the redundancy arithmetic, and the
// documented escape hatch is present. The "refusing to remove node"
// prefix is load-bearing — the RPC layer maps it to FailedPrecondition.
func TestRFRefusalError_Format(t *testing.T) {
	t.Parallel()
	id1, id2 := glid.New(), glid.New()
	err := rfRefusalError("node-A", []degradedVault{
		{ID: id1, Name: "alpha", RF: 3, Surviving: 2, Eligible: 0},
		{ID: id2, Name: "beta", RF: 2, Surviving: 1, Eligible: 0},
	})
	if !errors.Is(err, ErrWouldDropBelowRF) {
		t.Fatalf("refusal must wrap ErrWouldDropBelowRF, got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"refusing to remove node", "node-A", "2 vault(s)",
		"alpha", "beta", id1.String(), id2.String(),
		"2 of 3", "1 of 2", "--force",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error missing %q in:\n%s", want, msg)
		}
	}
}

// ---------------------------------------------------------------------
// Policy split and force, through the production gate evaluator
// ---------------------------------------------------------------------

func testLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// Operator-driven removal is pessimistic: the RF gate refuses.
func TestRemovalGates_OperatorRefusesBelowRF(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	logger, _ := testLogger()
	err := evaluateRemovalGates(context.Background(), c.store, c.ids["a"].String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator}, logger)
	if !errors.Is(err, ErrWouldDropBelowRF) {
		t.Fatalf("operator removal below RF must be refused, got %v", err)
	}
	if !strings.Contains(err.Error(), "triple") {
		t.Fatalf("refusal must name the affected vault: %v", err)
	}
}

// preStop self-removal is optimistic: the same removal is allowed, and
// the reason is on the record.
func TestRemovalGates_SelfRemovalAllowedBelowRF(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	logger, logs := testLogger()
	err := evaluateRemovalGates(context.Background(), c.store, c.ids["a"].String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicySelf}, logger)
	if err != nil {
		t.Fatalf("self-removal below RF must be allowed, got %v", err)
	}
	if !strings.Contains(logs.String(), "self-removal proceeds below replication factor") {
		t.Fatalf("self-removal below RF must be logged, got:\n%s", logs.String())
	}
}

// --force overrides the operator refusal and says so loudly.
func TestRemovalGates_ForceBypassesRFGateLoudly(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	logger, logs := testLogger()
	err := evaluateRemovalGates(context.Background(), c.store, c.ids["a"].String(),
		cluster.RemoveNodeOptions{Force: true, Policy: cluster.RemovalPolicyOperator}, logger)
	if err != nil {
		t.Fatalf("--force must bypass the RF gate, got %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "FORCE REMOVE: bypassing RF-preservation gate") {
		t.Fatalf("force bypass must be logged loudly, got:\n%s", out)
	}
	if !strings.Contains(out, "triple") {
		t.Fatalf("force bypass log must name the degraded vault, got:\n%s", out)
	}
}

// The orphan gate is unchanged by the policy split: total data loss is
// refused for BOTH policies, and only --force gets past it.
func TestRemovalGates_OrphanGateUnchangedByPolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.memVault("solo", 1, "a")

	for _, policy := range []cluster.RemovalPolicy{cluster.RemovalPolicyOperator, cluster.RemovalPolicySelf} {
		logger, _ := testLogger()
		err := evaluateRemovalGates(ctx, c.store, c.ids["a"].String(),
			cluster.RemoveNodeOptions{Policy: policy}, logger)
		if err == nil {
			t.Fatalf("policy %v: orphaning removal must be refused", policy)
		}
		if !strings.Contains(err.Error(), "would orphan") {
			t.Fatalf("policy %v: expected orphan refusal, got %v", policy, err)
		}
		if errors.Is(err, ErrWouldDropBelowRF) {
			t.Fatalf("policy %v: orphan refusal must not masquerade as an RF refusal: %v", policy, err)
		}
	}

	logger, logs := testLogger()
	if err := evaluateRemovalGates(ctx, c.store, c.ids["a"].String(),
		cluster.RemoveNodeOptions{Force: true, Policy: cluster.RemovalPolicyOperator}, logger); err != nil {
		t.Fatalf("--force must bypass the orphan gate, got %v", err)
	}
	if !strings.Contains(logs.String(), "FORCE REMOVE: bypassing orphan-refusal gate") {
		t.Fatalf("orphan force bypass must be logged loudly, got:\n%s", logs.String())
	}
}

// A removal that neither orphans nor degrades passes both gates without
// any warning noise.
func TestRemovalGates_SafeRemovalPasses(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("pair", 2, "a", "b", "c")

	logger, logs := testLogger()
	if err := evaluateRemovalGates(context.Background(), c.store, c.ids["a"].String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator}, logger); err != nil {
		t.Fatalf("safe removal must pass, got %v", err)
	}
	if strings.Contains(logs.String(), "WARN") {
		t.Fatalf("safe removal must not warn, got:\n%s", logs.String())
	}
}

// ---------------------------------------------------------------------
// Serialization: concurrent removals re-evaluate per iteration
// ---------------------------------------------------------------------

// newTestRemover builds the production serializer over a memory store,
// with an execute that performs the FSM half of a real removal (delete
// the NodeConfig, which sweeps the node's storage config). The Raft
// membership change has no analogue here; the FSM delete is what the
// gates read.
func newTestRemover(store system.Store, logger *slog.Logger, removed *[]string, mu *sync.Mutex) *nodeRemover {
	return &nodeRemover{
		cfgStore: store,
		logger:   logger,
		execute: func(ctx context.Context, targetNodeID string, _ cluster.RemoveNodeOptions) error {
			id, err := glid.Parse(targetNodeID)
			if err != nil {
				return err
			}
			if err := store.DeleteNode(ctx, id); err != nil {
				return err
			}
			mu.Lock()
			*removed = append(*removed, targetNodeID)
			mu.Unlock()
			return nil
		},
	}
}

// Two operator removals fired at the same instant must not both clear a
// gate computed against the pre-removal cluster. With RF=3 on a,b,c and
// one spare d, the first removal is safe (2 surviving + 1 spare = 3) and
// the second is not (1 surviving + 1 spare = 2) — so exactly one
// succeeds, whichever wins the race.
func TestNodeRemover_ConcurrentOperatorRemovalsReEvaluate(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.node("d", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	logger, _ := testLogger()
	var mu sync.Mutex
	var removed []string
	remover := newTestRemover(c.store, logger, &removed, &mu)

	targets := []string{c.ids["a"].String(), c.ids["b"].String()}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = remover.remove(context.Background(), target,
				cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator})
		}()
	}
	close(start)
	wg.Wait()

	var ok, refused int
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrWouldDropBelowRF):
			refused++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || refused != 1 {
		t.Fatalf("expected exactly one removal to survive the gate, got %d ok / %d refused (errs=%v)", ok, refused, errs)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(removed) != 1 {
		t.Fatalf("expected exactly one node actually removed, got %v", removed)
	}
}

// The same burst under the self policy is allowed through — the pods are
// leaving regardless — but each request is still evaluated in turn, and
// each one is on the record.
func TestNodeRemover_ConcurrentSelfRemovalsAllowedAndRecorded(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.memVault("triple", 3, "a", "b", "c")

	logger, logs := testLogger()
	var mu sync.Mutex
	var removed []string
	remover := newTestRemover(c.store, logger, &removed, &mu)

	targets := []string{c.ids["a"].String(), c.ids["b"].String()}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = remover.remove(context.Background(), target,
				cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicySelf})
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("self-removal %d must be allowed, got %v", i, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(removed) != 2 {
		t.Fatalf("expected both self-removals to complete, got %v", removed)
	}
	if got := strings.Count(logs.String(), "self-removal proceeds below replication factor"); got != 2 {
		t.Fatalf("each self-removal must be evaluated and logged: got %d log lines, want 2", got)
	}
}

// The gate allows a removal when an eligible spare exists because the
// placement manager will use it. This walks that whole promise on a
// 4-node cluster: gate passes, the removal executes, and the next
// reconcile restores the vault to its replication factor on the spare.
func TestNodeRemover_AllowedRemovalIsReplacedByReconcile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.node("c", system.NodeStateLive)
	c.node("d", system.NodeStateLive) // the spare the gate counts on
	vaultID := c.memVault("triple", 3, "a", "b", "c")

	logger, _ := testLogger()
	var mu sync.Mutex
	var removed []string
	remover := newTestRemover(c.store, logger, &removed, &mu)

	if err := remover.remove(ctx, c.ids["c"].String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator}); err != nil {
		t.Fatalf("removal with an eligible spare must be allowed: %v", err)
	}

	// The placement manager sees the surviving nodes and the spare.
	peers := cluster.NewPeerState(time.Minute)
	now := time.Now()
	peers.Update(c.ids["b"].String(), nil, now)
	peers.Update(c.ids["d"].String(), nil, now)
	pm := &placementManager{
		cfgStore:    c.store,
		peerState:   peers,
		alerts:      alert.New(),
		localNodeID: c.ids["a"].String(),
		logger:      logger,
		triggerCh:   make(chan struct{}, 1),
	}
	pm.reconcile(ctx)

	placements, err := c.store.GetVaultPlacements(ctx, vaultID)
	if err != nil {
		t.Fatalf("GetVaultPlacements: %v", err)
	}
	nscs, err := c.store.ListNodeStorageConfigs(ctx)
	if err != nil {
		t.Fatalf("ListNodeStorageConfigs: %v", err)
	}
	holders := map[string]bool{}
	for _, nid := range system.PlacementNodeIDs(placements, nscs) {
		holders[nid] = true
	}
	if len(holders) != 3 {
		t.Fatalf("expected RF=3 restored after reconcile, got holders %v", holders)
	}
	if holders[c.ids["c"].String()] {
		t.Fatalf("removed node still holds a placement: %v", holders)
	}
	if !holders[c.ids["d"].String()] {
		t.Fatalf("the spare the gate counted on was not used: %v", holders)
	}

	// And with the spare consumed, the next operator removal is refused.
	if err := remover.remove(ctx, c.ids["b"].String(),
		cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator}); !errors.Is(err, ErrWouldDropBelowRF) {
		t.Fatalf("second removal must be refused once the spare is in use, got %v", err)
	}
}

// The orphan gate is serialized too: two removals that are each
// individually safe must not both proceed and orphan the vault. RF=2 on
// a,b — removing a is fine, then removing b would leave zero placements,
// so the second is refused.
func TestNodeRemover_ConcurrentRemovalsCannotOrphan(t *testing.T) {
	t.Parallel()
	c := newRFTestCluster(t)
	c.node("a", system.NodeStateLive)
	c.node("b", system.NodeStateLive)
	c.memVault("pair", 1, "a", "b")

	logger, _ := testLogger()
	var mu sync.Mutex
	var removed []string
	remover := newTestRemover(c.store, logger, &removed, &mu)

	targets := []string{c.ids["a"].String(), c.ids["b"].String()}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, target := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = remover.remove(context.Background(), target,
				cluster.RemoveNodeOptions{Policy: cluster.RemovalPolicyOperator})
		}()
	}
	close(start)
	wg.Wait()

	var ok int
	for _, err := range errs {
		if err == nil {
			ok++
			continue
		}
		if !strings.Contains(err.Error(), "refusing to remove node") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 {
		t.Fatalf("expected exactly one removal to pass the gates, got %d (errs=%v)", ok, errs)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(removed) != 1 {
		t.Fatalf("expected exactly one node actually removed, got %v", removed)
	}
}
