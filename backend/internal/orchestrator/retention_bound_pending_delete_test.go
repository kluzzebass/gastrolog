package orchestrator

// The age/count bound re-check runs at the end of a retention sweep, and on
// any vault with a vault-ctl Raft group a delete does not remove the chunk's
// manifest entry when the sweep requests it: CmdRequestDelete only opens a
// pending delete, and the manifest entry goes when the last expected node's
// CmdAckDelete commits. Every node — the leader included — fulfils its
// obligation on a goroutine, so the re-check normally runs inside that ack
// window. These tests drive that window deterministically: a shared
// in-process vault-ctl log applies every committed command to every node's
// FSM, and delete acks reach the log only when the test lets them.

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type deleteAck struct {
	id   chunk.ChunkID
	node string
}

// vaultCtlLog is one vault-ctl Raft group spanning every node: a committed
// command applies to every node's FSM, in log order. Delete acks arrive via
// ack, which commits them unless their node is held; a held node's acks
// park until release. proposed records every ack that reached the log's
// door so a test waits on the event, never on time.
type vaultCtlLog struct {
	t        *testing.T
	mu       sync.Mutex
	cond     *sync.Cond
	fsms     map[string]*vaultctlfsm.FSM
	held     map[string]bool
	parked   []deleteAck
	proposed map[deleteAck]bool
	// rejectRequestDelete fails every CmdRequestDelete before it commits.
	rejectRequestDelete bool
}

func newVaultCtlLog(t *testing.T) *vaultCtlLog {
	l := &vaultCtlLog{
		t:        t,
		fsms:     make(map[string]*vaultctlfsm.FSM),
		held:     make(map[string]bool),
		proposed: make(map[deleteAck]bool),
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Apply implements vaultctlfsm.Applier.
func (l *vaultCtlLog) Apply(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rejectRequestDelete {
		var cmd gastrologv1.VaultCtlCommand
		if err := proto.Unmarshal(data, &cmd); err != nil {
			return err
		}
		if cmd.GetRequestDelete() != nil {
			return errors.New("vault-ctl apply rejected")
		}
	}
	return l.commitLocked(data)
}

func (l *vaultCtlLog) setRejectRequestDelete(reject bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rejectRequestDelete = reject
}

func (l *vaultCtlLog) commitLocked(data []byte) error {
	for _, nid := range slices.Sorted(maps.Keys(l.fsms)) {
		if res := l.fsms[nid].Apply(&hraft.Log{Data: data}); res != nil {
			if err, ok := res.(error); ok {
				return err
			}
		}
	}
	return nil
}

func (l *vaultCtlLog) mustCommit(data []byte) {
	l.t.Helper()
	if err := l.Apply(data); err != nil {
		l.t.Fatalf("commit: %v", err)
	}
}

func (l *vaultCtlLog) ack(id chunk.ChunkID, node string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := deleteAck{id: id, node: node}
	l.proposed[a] = true
	l.cond.Broadcast()
	if l.held[node] {
		l.parked = append(l.parked, a)
		return nil
	}
	return l.commitLocked(vaultctlfsm.MarshalAckDelete(id, node))
}

func (l *vaultCtlLog) hold(nodes ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, n := range nodes {
		l.held[n] = true
	}
}

// release commits every parked ack from the given nodes and stops holding
// them. Safe to call from any goroutine.
func (l *vaultCtlLog) release(nodes ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, n := range nodes {
		delete(l.held, n)
	}
	var still []deleteAck
	for _, a := range l.parked {
		if l.held[a.node] {
			still = append(still, a)
			continue
		}
		if err := l.commitLocked(vaultctlfsm.MarshalAckDelete(a.id, a.node)); err != nil {
			l.t.Errorf("commit released ack: %v", err)
		}
	}
	l.parked = still
}

// waitProposed blocks until every (chunk, node) pair has proposed its ack —
// each node has deleted its local copy by then.
func (l *vaultCtlLog) waitProposed(ids []chunk.ChunkID, nodes []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		missing := false
		for _, id := range ids {
			for _, n := range nodes {
				if !l.proposed[deleteAck{id: id, node: n}] {
					missing = true
				}
			}
		}
		if !missing {
			return
		}
		l.cond.Wait()
	}
}

type boundClusterNode struct {
	id   string
	dir  string
	orch *Orchestrator
	cm   *chunkfile.Manager
	inst *VaultInstance
	fsm  *vaultctlfsm.FSM
}

// boundCluster is a vault placed on the first rf of its nodes, with a
// file-backed chunk store per node and the vault-ctl group spanning all of
// them. nodes[0] is the placement leader and runs retention.
type boundCluster struct {
	t       *testing.T
	vaultID glid.GLID
	nodes   []*boundClusterNode
	holders []string
	log     *vaultCtlLog
	spy     *alertSpy
	guard   *diskGuard
	runner  *retentionRunner
	base    time.Time
	seeded  int
}

func newBoundCluster(t *testing.T, nodeIDs []string, rf int) *boundCluster {
	t.Helper()
	c := &boundCluster{
		t:       t,
		vaultID: glid.New(),
		holders: nodeIDs[:rf],
		log:     newVaultCtlLog(t),
		base:    time.Now().Add(-24 * time.Hour),
	}
	for _, nid := range nodeIDs {
		c.nodes = append(c.nodes, &boundClusterNode{id: nid, dir: t.TempDir()})
	}
	for _, n := range c.nodes {
		c.startNode(n, vaultctlfsm.New())
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.orch.Stop()
		}
		for _, n := range c.nodes {
			_ = n.cm.Close()
		}
	})
	return c
}

func (c *boundCluster) leader() *boundClusterNode { return c.nodes[0] }

// startNode builds a node's orchestrator, chunk store and reconciler over
// its directory and the given FSM, and joins the FSM to the log. Starting
// the leader also builds a fresh disk guard and retention runner, as a
// restarted process would.
func (c *boundCluster) startNode(n *boundClusterNode, fsm *vaultctlfsm.FSM) {
	t := c.t
	t.Helper()
	isLeader := n == c.leader()
	cfg := Config{LocalNodeID: n.id, Logger: quietLogger}
	if isLeader {
		c.spy = &alertSpy{}
		cfg.Alerts = c.spy
	}
	n.orch = newTestOrch(t, cfg)
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: n.dir, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	n.cm = cm
	n.fsm = fsm

	inst := &VaultInstance{
		VaultID: c.vaultID,
		Type:    "file",
		Chunks:  cm,
		Indexes: indexfile.NewManager(n.dir, nil, nil, cm),
	}
	inst.applyRaftCallbacks(buildVaultRaftCallbacks(nil, fsm, c.log))
	inst.RaftLeadershipFacet = RaftLeadershipFacet{
		HasRaftLeader: func() bool { return true },
		IsRaftLeader:  func() bool { return isLeader },
	}
	inst.IsFSMReady = func() bool { return true }
	inst.ApplyRaftAckDelete = c.log.ack
	var targets []system.ReplicationTarget
	for _, h := range c.holders[1:] {
		targets = append(targets, system.ReplicationTarget{NodeID: h})
	}
	if isLeader {
		inst.FollowerTargets = targets
	} else {
		inst.IsFollower = true
	}
	rec := NewVaultLifecycleReconciler(n.orch, c.vaultID, inst, n.id, quietLogger)
	inst.Reconciler = rec
	rec.Wire(fsm)
	n.inst = inst
	vault := NewVault(c.vaultID, inst)
	vault.Name = "bound-cluster"
	n.orch.RegisterVault(vault)

	c.log.mu.Lock()
	c.log.fsms[n.id] = fsm
	c.log.mu.Unlock()

	if !isLeader {
		return
	}
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	g.SetVaultGuard(c.vaultID, "bound-cluster", []string{"volA"}, 10*gib, "", "")
	n.orch.diskGuard = g
	c.guard = g
	c.runner = &retentionRunner{
		isLeader:                  true,
		vaultID:                   c.vaultID,
		vaultName:                 "bound-cluster",
		orch:                      n.orch,
		cm:                        cm,
		im:                        inst.Indexes,
		followerTargets:           targets,
		reconciler:                rec,
		applyRaftRetentionPending: inst.ApplyRaftRetentionPending,
		disposition:               system.RetentionDispositionDelete,
		now:                       time.Now,
		logger:                    quietLogger,
	}
}

// restartLeader stops the leader and brings it back over the same chunk
// directory with an FSM restored from a snapshot of the one it had.
func (c *boundCluster) restartLeader() {
	t := c.t
	t.Helper()
	old := c.leader()
	raw, err := proto.Marshal(old.fsm.SnapshotProto())
	if err != nil {
		t.Fatal(err)
	}
	old.orch.Stop()
	if err := old.cm.Close(); err != nil {
		t.Fatal(err)
	}
	var snap gastrologv1.VaultCtlSnapshot
	if err := proto.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	restored := vaultctlfsm.New()
	restored.RestoreProto(&snap)
	c.startNode(old, restored)
}

// seed creates n sealed chunks, oldest first, on every holder's disk and in
// the vault-ctl manifest, each sealed at sealedAt.
func (c *boundCluster) seed(n int, sealedAt time.Time) []chunk.ChunkID {
	c.t.Helper()
	starts := make([]time.Time, n)
	for i := range starts {
		starts[i] = c.base.Add(time.Duration(c.seeded) * time.Minute)
		c.seeded++
	}
	return c.seedAt(starts, sealedAt, func(int) bool { return true })
}

// seedAt creates one sealed chunk per start, in the given order, on every
// holder's disk and in the vault-ctl manifest. A chunk for which onLeader
// reports false never reaches the leader's own chunk store: the leader knows
// it only from the manifest.
func (c *boundCluster) seedAt(starts []time.Time, sealedAt time.Time, onLeader func(i int) bool) []chunk.ChunkID {
	t := c.t
	t.Helper()
	ids := make([]chunk.ChunkID, 0, len(starts))
	for i, start := range starts {
		id := chunk.NewChunkID()
		recs := make([]chunk.Record, 3)
		for j := range recs {
			ts := start.Add(time.Duration(j) * time.Second)
			recs[j] = chunk.Record{SourceTS: ts, IngestTS: ts, WriteTS: ts, Raw: []byte("bound-record")}
		}
		end := recs[len(recs)-1].WriteTS
		for _, h := range c.holders {
			if h == c.leader().id && !onLeader(i) {
				continue
			}
			if _, err := c.node(h).cm.ImportRecords(id, testIterFromRecords(recs)); err != nil {
				t.Fatalf("import %s on %s: %v", id, h, err)
			}
		}
		c.log.mustCommit(vaultctlfsm.MarshalCreateChunk(id, start, start, start))
		c.log.mustCommit(vaultctlfsm.MarshalSealChunk(id, end, int64(len(recs)), 100, start, end, end, true, sealedAt))
		ids = append(ids, id)
	}
	return ids
}

func (c *boundCluster) node(id string) *boundClusterNode {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	c.t.Fatalf("no node %q", id)
	return nil
}

func (c *boundCluster) countKey() string {
	return alarmVaultBoundCapped + ":" + c.vaultID.String() + "/count"
}

func (c *boundCluster) ageKey() string {
	return alarmVaultBoundCapped + ":" + c.vaultID.String() + "/age"
}

// assertManifest checks every node's FSM agrees on exactly which chunks are
// in the manifest and which of those carry a pending delete.
func (c *boundCluster) assertManifest(label string, present, pending []chunk.ChunkID) {
	t := c.t
	t.Helper()
	for _, n := range c.nodes {
		var got []chunk.ChunkID
		for _, e := range n.fsm.List() {
			got = append(got, e.ID)
		}
		if !sameChunkSet(got, present) {
			t.Fatalf("%s: node %s manifest = %v, want %v", label, n.id, got, present)
		}
		var gotPending []chunk.ChunkID
		for _, p := range n.fsm.PendingDeletes() {
			gotPending = append(gotPending, p.ChunkID)
		}
		if !sameChunkSet(gotPending, pending) {
			t.Fatalf("%s: node %s pending deletes = %v, want %v", label, n.id, gotPending, pending)
		}
	}
}

func sameChunkSet(a, b []chunk.ChunkID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[chunk.ChunkID]bool, len(a))
	for _, id := range a {
		set[id] = true
	}
	for _, id := range b {
		if !set[id] {
			return false
		}
	}
	return true
}

func countBoundRules(maxChunks int) []retentionRule {
	p := chunk.NewCountRetentionPolicy(maxChunks)
	return []retentionRule{{policy: p, refuse: true, countPolicy: p}}
}

func ageBoundRules(maxAge time.Duration) []retentionRule {
	p := chunk.NewTTLRetentionPolicy(maxAge)
	return []retentionRule{{policy: p, refuse: true, agePolicy: p}}
}

var (
	fourNodes   = []string{"node-1", "node-2", "node-3", "node-4"}
	quietLogger = slog.New(slog.DiscardHandler)
)

// TestBoundRecheckExcludesCommittedPendingDeletes is the multi-node count
// bound in the ack window: a delete-disposition sweep requests every delete
// the bound needs, and the re-check that follows must not read the chunks
// whose deletes are committed but not yet acknowledged as still retained.
// One expected node then withholds its acks across a further sweep; the
// bound stays clear while the manifest keeps reporting the chunks as
// present-and-pending — nothing is reported deleted that is not.
func TestBoundRecheckExcludesCommittedPendingDeletes(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	ids := c.seed(5, time.Now())
	expired, kept := ids[:3], ids[3:]
	rules := countBoundRules(2)

	c.log.hold(fourNodes...)
	c.runner.sweep(rules)

	if c.guard.vaultChunkCountBoundCapped(c.vaultID) {
		t.Fatal("a sweep that committed every delete the bound needs must not leave the count bound capped")
	}
	if err := c.leader().orch.vaultAdmissionGate(c.vaultID); err != nil {
		t.Fatalf("admission must not be refused once every needed delete is committed: %v", err)
	}
	if c.spy.has(c.countKey()) {
		t.Fatal("vault-bound-capped must not raise for deletes that are committed and awaiting acks")
	}
	c.log.waitProposed(expired, fourNodes)
	c.assertManifest("all acks held", ids, expired)

	c.log.release("node-1", "node-2", "node-3")
	c.runner.sweep(rules)

	if c.guard.vaultChunkCountBoundCapped(c.vaultID) {
		t.Fatal("one node withholding its acks must not cap the count bound")
	}
	c.assertManifest("node-4 acks held", ids, expired)
	for _, n := range c.nodes {
		for _, p := range n.fsm.PendingDeletes() {
			if !maps.Equal(p.ExpectedFrom, map[string]bool{"node-4": true}) {
				t.Fatalf("node %s: chunk %s still expects acks from %v, want only node-4", n.id, p.ChunkID, p.ExpectedFrom)
			}
		}
	}

	c.log.release("node-4")
	c.assertManifest("all acks landed", kept, nil)
	c.runner.sweep(rules)
	if c.guard.vaultChunkCountBoundCapped(c.vaultID) {
		t.Fatal("the count bound must stay clear once the deletes finalize")
	}
	c.assertManifest("after final sweep", kept, nil)
	for _, h := range c.holders {
		metas, err := c.node(h).cm.List()
		if err != nil {
			t.Fatal(err)
		}
		var onDisk []chunk.ChunkID
		for _, m := range metas {
			onDisk = append(onDisk, m.ID)
		}
		if !sameChunkSet(onDisk, kept) {
			t.Fatalf("node %s holds %v on disk, want exactly the kept chunks %v", h, onDisk, kept)
		}
	}
}

func (c *boundCluster) pendingOnLeader() []chunk.ChunkID {
	return c.leader().fsm.PendingDeleteIDs()
}

func (c *boundCluster) assertUncapped(label string) {
	t := c.t
	t.Helper()
	if c.guard.vaultChunkCountBoundCapped(c.vaultID) || c.guard.vaultAgeBoundCapped(c.vaultID) {
		t.Fatalf("%s: bound capped (age=%v count=%v)", label,
			c.guard.vaultAgeBoundCapped(c.vaultID), c.guard.vaultChunkCountBoundCapped(c.vaultID))
	}
	if err := c.leader().orch.vaultAdmissionGate(c.vaultID); err != nil {
		t.Fatalf("%s: admission refused: %v", label, err)
	}
	if c.spy.has(c.countKey()) || c.spy.has(c.ageKey()) {
		t.Fatalf("%s: vault-bound-capped standing", label)
	}
}

// TestBoundRecheckLeaderOwnAckWindow is the RF=1 shape: the vault-ctl group
// spans every node, but only the placement leader owes an ack, and that ack
// is still proposed asynchronously after the sweep's re-check begins. The
// leader's own ack window alone must not cap the bound — on a lone node and
// on a four-node cluster alike.
func TestBoundRecheckLeaderOwnAckWindow(t *testing.T) {
	t.Parallel()
	for _, nodeIDs := range [][]string{fourNodes[:1], fourNodes} {
		t.Run(fmt.Sprintf("%d-nodes", len(nodeIDs)), func(t *testing.T) {
			t.Parallel()
			c := newBoundCluster(t, nodeIDs, 1)
			ids := c.seed(5, time.Now())
			expired, kept := ids[:3], ids[3:]
			rules := countBoundRules(2)

			c.log.hold("node-1")
			c.runner.sweep(rules)
			c.assertUncapped("leader ack held")
			c.log.waitProposed(expired, []string{"node-1"})
			c.assertManifest("leader ack held", ids, expired)
			for _, p := range c.leader().fsm.PendingDeletes() {
				if !maps.Equal(p.ExpectedFrom, map[string]bool{"node-1": true}) {
					t.Fatalf("an RF=1 delete must expect only the placement leader, got %v", p.ExpectedFrom)
				}
			}

			c.runner.sweep(rules)
			c.assertUncapped("second sweep, leader ack still held")
			c.assertManifest("second sweep, leader ack still held", ids, expired)

			c.log.release("node-1")
			c.assertManifest("leader acked", kept, nil)
			c.runner.sweep(rules)
			c.assertUncapped("after finalize")
		})
	}
}

// TestBoundRecheckAgeAndCountTreatedAlike pins that the age and count bounds
// read the same retained set: with both bounds refusing and every ack held,
// neither caps over its own committed deletes.
func TestBoundRecheckAgeAndCountTreatedAlike(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	now := time.Now()
	old := c.seed(2, now.Add(-2*time.Hour))
	fresh := c.seed(4, now)
	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	countPolicy := chunk.NewCountRetentionPolicy(3)
	rules := []retentionRule{
		{policy: agePolicy, refuse: true, agePolicy: agePolicy},
		{policy: countPolicy, refuse: true, countPolicy: countPolicy},
	}

	c.log.hold(fourNodes...)
	c.runner.sweep(rules)
	c.assertUncapped("all acks held")

	expired := append(slices.Clone(old), fresh[0])
	if !sameChunkSet(c.pendingOnLeader(), expired) {
		t.Fatalf("pending deletes = %v, want the two over-age chunks and the oldest over-count chunk %v", c.pendingOnLeader(), expired)
	}
	c.log.waitProposed(expired, fourNodes)
	c.runner.sweep(rules)
	c.assertUncapped("second sweep, all acks held")
	c.assertManifest("second sweep, all acks held", append(slices.Clone(old), fresh...), expired)
}

// TestBoundRecheckCountsDeletesThatFailedToCommit is the half the fix must
// not erase: a chunk whose delete never committed is still retained, however
// far retention got with it (here it is already flagged retention-pending),
// so the bound stays capped until a sweep's delete does commit.
func TestBoundRecheckCountsDeletesThatFailedToCommit(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	ids := c.seed(4, time.Now())
	rules := countBoundRules(2)

	c.log.setRejectRequestDelete(true)
	c.runner.sweep(rules)

	if !c.guard.vaultChunkCountBoundCapped(c.vaultID) {
		t.Fatal("a sweep whose deletes failed to commit must leave the count bound capped")
	}
	if err := c.leader().orch.vaultAdmissionGate(c.vaultID); err == nil {
		t.Fatal("admission must be refused while the uncommitted deletes leave the bound violated")
	}
	if !c.spy.has(c.countKey()) {
		t.Fatal("vault-bound-capped must stand for the count bound")
	}
	if got := c.leader().inst.ListRetentionPending(); !sameChunkSet(got, ids[:2]) {
		t.Fatalf("fixture: the over-bound chunks must be flagged retention-pending, got %v", got)
	}
	c.assertManifest("deletes rejected", ids, nil)

	c.log.setRejectRequestDelete(false)
	c.log.hold(fourNodes...)
	c.runner.sweep(rules)
	c.assertUncapped("deletes committed")
	c.assertManifest("deletes committed", ids, ids[:2])
}

// TestBoundRecheckWithholdingNodeUnderSteadyIngest is the node that never
// acks — down, or partitioned — while ingest keeps sealing chunks past the
// bound. Its outstanding acks pin the expired chunks in the manifest; the
// bound must keep being enforced on the retained chunks alone, never capping
// over the withheld acks and never deleting a retained chunk in their place.
// The still-owed node is what the operator sees for those chunks.
func TestBoundRecheckWithholdingNodeUnderSteadyIngest(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	rules := countBoundRules(2)
	c.log.hold("node-4")

	var all []chunk.ChunkID
	for round := range 5 {
		all = append(all, c.seed(2, time.Now())...)
		c.runner.sweep(rules)
		c.assertUncapped(fmt.Sprintf("round %d", round))

		kept := all[len(all)-2:]
		expired := all[:len(all)-2]
		c.log.waitProposed(expired, fourNodes)
		c.assertManifest(fmt.Sprintf("round %d", round), all, expired)

		acks := c.leader().orch.PendingDeleteAcks(c.vaultID)
		for _, id := range expired {
			if !slices.Equal(acks[id], []string{"node-4"}) {
				t.Fatalf("round %d: chunk %s must report node-4 as the node still owing its ack, got %v", round, id, acks[id])
			}
		}
		for _, id := range kept {
			if _, ok := acks[id]; ok {
				t.Fatalf("round %d: retained chunk %s must not be deleted while older deletes await node-4", round, id)
			}
		}
	}
}

// TestRetentionSweepSkipsChunksWithCommittedDeletes covers a delete the
// sweep did not issue — an operator deleting the newest chunk — awaiting its
// acks. The leader has already removed its own copy, so the chunk reaches the
// sweep only from the manifest; it must neither count toward the count bound
// nor push a retained chunk out in its place.
func TestRetentionSweepSkipsChunksWithCommittedDeletes(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	ids := c.seed(3, time.Now())
	newest := ids[2:]
	rules := countBoundRules(2)

	c.log.hold(fourNodes...)
	c.log.mustCommit(vaultctlfsm.MarshalRequestDelete(newest[0], time.Now(), "manual-delete-rpc", fourNodes))
	c.log.waitProposed(newest, fourNodes)

	c.runner.sweep(rules)
	c.assertUncapped("operator delete pending")
	c.assertManifest("operator delete pending", ids, newest)
}

// TestBoundRecheckEdgeCases covers the boundaries of the retained set.
func TestBoundRecheckEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty-vault", func(t *testing.T) {
		t.Parallel()
		c := newBoundCluster(t, fourNodes, 4)
		c.runner.sweep(countBoundRules(2))
		c.assertUncapped("empty vault")
		c.assertManifest("empty vault", nil, nil)
	})

	t.Run("exactly-at-the-bound", func(t *testing.T) {
		t.Parallel()
		c := newBoundCluster(t, fourNodes, 4)
		ids := c.seed(2, time.Now())
		c.log.hold(fourNodes...)
		c.runner.sweep(countBoundRules(2))
		c.assertUncapped("at the bound")
		c.assertManifest("at the bound", ids, nil)

		more := c.seed(1, time.Now())
		c.runner.sweep(countBoundRules(2))
		c.assertUncapped("one over, delete committed")
		c.assertManifest("one over, delete committed", append(slices.Clone(ids), more...), ids[:1])
	})

	t.Run("pending-chunk-sealed-again", func(t *testing.T) {
		t.Parallel()
		c := newBoundCluster(t, fourNodes, 4)
		ids := c.seed(3, time.Now())
		rules := countBoundRules(2)
		c.log.hold(fourNodes...)
		c.runner.sweep(rules)
		c.log.waitProposed(ids[:1], fourNodes)

		e := c.leader().fsm.Get(ids[0])
		if e == nil {
			t.Fatal("fixture: the pending chunk must still be in the manifest")
		}
		c.log.mustCommit(vaultctlfsm.MarshalSealChunk(e.ID, e.WriteEnd, e.RecordCount, e.Bytes,
			e.IngestStart, e.IngestEnd, e.SourceEnd, e.IngestTSMonotonic, time.Now()))

		c.runner.sweep(rules)
		c.assertUncapped("pending chunk sealed again")
		c.assertManifest("pending chunk sealed again", ids, ids[:1])
	})
}

// TestBoundRecheckSurvivesLeaderRestart restarts the placement leader while
// a node still owes its acks. The pending deletes come back with the FSM;
// the fresh process's first sweep must read them the same way.
func TestBoundRecheckSurvivesLeaderRestart(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	ids := c.seed(5, time.Now())
	expired, kept := ids[:3], ids[3:]
	rules := countBoundRules(2)

	c.log.hold("node-4")
	c.runner.sweep(rules)
	c.log.waitProposed(expired, fourNodes)

	c.restartLeader()
	if !sameChunkSet(c.pendingOnLeader(), expired) {
		t.Fatalf("fixture: the restarted leader must restore the pending deletes, got %v", c.pendingOnLeader())
	}
	c.runner.sweep(rules)
	c.assertUncapped("after restart, node-4 acks held")
	c.assertManifest("after restart, node-4 acks held", ids, expired)

	c.log.release("node-4")
	c.assertManifest("after restart, all acked", kept, nil)
	c.runner.sweep(rules)
	c.assertUncapped("after restart, finalized")
}

// TestBoundRecheckWithAcksLandingConcurrently lets the acks land while a
// sweep runs: whichever side of the re-check each ack falls on, the bound
// must come out clear and the retained chunks untouched.
func TestBoundRecheckWithAcksLandingConcurrently(t *testing.T) {
	t.Parallel()
	c := newBoundCluster(t, fourNodes, 4)
	ids := c.seed(6, time.Now())
	expired, kept := ids[:4], ids[4:]
	rules := countBoundRules(2)

	c.log.hold(fourNodes...)
	c.runner.sweep(rules)
	c.log.waitProposed(expired, fourNodes)

	var wg sync.WaitGroup
	wg.Go(func() { c.log.release(fourNodes...) })
	c.runner.sweep(rules)
	wg.Wait()

	c.assertUncapped("acks landed during the sweep")
	c.assertManifest("acks landed during the sweep", kept, nil)
}
