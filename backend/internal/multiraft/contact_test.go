package multiraft

import (
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// Coverage for gastrolog-1lbifx: the Raft transport is the source of the
// per-peer reachability evidence that replaced the dedicated cluster
// heartbeat broadcast. These tests pin the two facts the cluster layer
// distinguishes — a PROBE (we asked) versus CONTACT (they answered) — because
// the entire liveness rule rests on being able to tell them apart.

type contactEvent struct {
	kind  string // "probe" | "contact"
	peer  string
	group string
}

type recordingContact struct {
	mu     sync.Mutex
	events []contactEvent
	ch     chan contactEvent
}

func newRecordingContact() *recordingContact {
	return &recordingContact{ch: make(chan contactEvent, 256)}
}

func (r *recordingContact) RecordRaftContact(peer, group string, _ time.Time) {
	r.record(contactEvent{kind: "contact", peer: peer, group: group})
}

func (r *recordingContact) RecordRaftProbe(peer, group string, _ time.Time) {
	r.record(contactEvent{kind: "probe", peer: peer, group: group})
}

func (r *recordingContact) record(e contactEvent) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
	select {
	case r.ch <- e:
	default:
	}
}

func (r *recordingContact) count(kind, peer string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.kind == kind && e.peer == peer {
			n++
		}
	}
	return n
}

// await blocks until an event matching kind+peer arrives, so the assertion is
// driven by the event itself rather than by a sleep long enough to "probably"
// cover the RPC.
func (r *recordingContact) await(t *testing.T, kind, peer string) contactEvent {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-r.ch:
			if e.kind == kind && e.peer == peer {
				return e
			}
		case <-deadline:
			t.Fatalf("no %s event for peer %q arrived", kind, peer)
		}
	}
}

// A successful outbound AppendEntries yields BOTH facts: we asked (probe) and
// the peer answered (contact). The receiving node independently records
// contact for the sender, identified from the RPCHeader that hraft stamps on
// every request — that inbound half is what lets a follower know its leader is
// alive without any liveness broadcast.
func TestTransport_ContactRecordedBothDirections(t *testing.T) {
	nodes := makeTestCluster(t, 2)

	senderRec := newRecordingContact()
	receiverRec := newRecordingContact()
	nodes[0].transport.SetContactRecorder(senderRec)
	nodes[1].transport.SetContactRecorder(receiverRec)

	const group = "cluster-ctl"
	tp0 := nodes[0].transport.GroupTransport(group)
	tp1 := nodes[1].transport.GroupTransport(group)

	go func() {
		for rpc := range tp1.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
		}
	}()

	req := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{ProtocolVersion: 3, ID: []byte("node-0"), Addr: []byte("addr-0")},
		Term:      1,
	}
	var resp raft.AppendEntriesResponse
	if err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress, req, &resp); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	if got := senderRec.count("probe", "node-1"); got != 1 {
		t.Errorf("sender probes for node-1 = %d, want 1", got)
	}
	if got := senderRec.count("contact", "node-1"); got != 1 {
		t.Errorf("sender contacts for node-1 = %d, want 1 (peer answered)", got)
	}

	e := receiverRec.await(t, "contact", "node-0")
	if e.group != group {
		t.Errorf("inbound contact group = %q, want %q", e.group, group)
	}
	// Inbound traffic is never a probe: we did not initiate it, so its
	// absence later must not read as evidence the sender died.
	if got := receiverRec.count("probe", "node-0"); got != 0 {
		t.Errorf("receiver recorded %d probes for node-0, want 0 — inbound is contact-only", got)
	}
}

// A failed outbound RPC still records the probe. This is the load-bearing
// case: without it, "we are asking and getting nothing back" would be
// indistinguishable from "we have no Raft edge to this peer at all", and the
// cluster layer could never turn silence into an unreachable verdict.
func TestTransport_FailedRPCRecordsProbeWithoutContact(t *testing.T) {
	nodes := makeTestCluster(t, 2)

	rec := newRecordingContact()
	nodes[0].transport.SetContactRecorder(rec)

	const group = "cluster-ctl"
	tp0 := nodes[0].transport.GroupTransport(group)
	// Deliberately no group registered on node 1 and no consumer: the RPC
	// reaches the server and comes back an error, the same shape a wedged
	// peer produces once its deadline expires.
	nodes[1].transport.RemoveGroup(group)

	req := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{ProtocolVersion: 3, ID: []byte("node-0")},
		Term:      1,
	}
	var resp raft.AppendEntriesResponse
	if err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress, req, &resp); err == nil {
		t.Fatal("AppendEntries to an unregistered group: want error, got nil")
	}

	if got := rec.count("probe", "node-1"); got != 1 {
		t.Errorf("probes for node-1 = %d, want 1 (a failed attempt is still an attempt)", got)
	}
	if got := rec.count("contact", "node-1"); got != 0 {
		t.Errorf("contacts for node-1 = %d, want 0 (nothing came back)", got)
	}
}

// An unreachable target — no such peer in the pool, so the RPC never leaves
// the node — must also record a probe. Connection-acquisition failure is the
// dominant failure mode for a node that has gone away entirely, and skipping
// the probe there would leave exactly that peer looking "not probed" and
// therefore falling back to stale broadcast freshness.
func TestTransport_UnreachableTargetRecordsProbe(t *testing.T) {
	nodes := makeTestCluster(t, 1)

	rec := newRecordingContact()
	nodes[0].transport.SetContactRecorder(rec)

	tp0 := nodes[0].transport.GroupTransport("cluster-ctl")
	var resp raft.AppendEntriesResponse
	err := tp0.AppendEntries("node-gone", "no-such-address", &raft.AppendEntriesRequest{Term: 1}, &resp)
	if err == nil {
		t.Fatal("AppendEntries to an undialable address: want error, got nil")
	}

	if got := rec.count("probe", "node-gone"); got != 1 {
		t.Errorf("probes for node-gone = %d, want 1", got)
	}
	if got := rec.count("contact", "node-gone"); got != 0 {
		t.Errorf("contacts for node-gone = %d, want 0", got)
	}
}

// Contact is folded across groups by the consumer, so the transport must tag
// each event with the group it came from and must keep reporting on every
// group independently — a node leading two groups probes the same peer twice
// per round, and both count.
func TestTransport_ContactTaggedPerGroup(t *testing.T) {
	nodes := makeTestCluster(t, 2)

	rec := newRecordingContact()
	nodes[0].transport.SetContactRecorder(rec)

	groups := []string{"cluster-ctl", "vault/abc/ctl"}
	for _, g := range groups {
		tp1 := nodes[1].transport.GroupTransport(g)
		go func() {
			for rpc := range tp1.Consumer() {
				rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
			}
		}()
	}
	for _, g := range groups {
		tp0 := nodes[0].transport.GroupTransport(g)
		var resp raft.AppendEntriesResponse
		if err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress,
			&raft.AppendEntriesRequest{Term: 1}, &resp); err != nil {
			t.Fatalf("AppendEntries on %s: %v", g, err)
		}
	}

	seen := map[string]bool{}
	rec.mu.Lock()
	for _, e := range rec.events {
		if e.kind == "contact" && e.peer == "node-1" {
			seen[e.group] = true
		}
	}
	rec.mu.Unlock()
	for _, g := range groups {
		if !seen[g] {
			t.Errorf("no contact recorded for group %q; saw %v", g, seen)
		}
	}
}

// The pipelined append path carries no ServerID per request, so the peer is
// captured when the pipeline opens. A queued send is a probe only — the
// contact lands when the response actually comes back.
func TestTransport_PipelineProbeOnSendContactOnResponse(t *testing.T) {
	nodes := makeTestCluster(t, 2)

	rec := newRecordingContact()
	nodes[0].transport.SetContactRecorder(rec)

	const group = "cluster-ctl"
	tp1 := nodes[1].transport.GroupTransport(group)
	go func() {
		for rpc := range tp1.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
		}
	}()

	tp0 := nodes[0].transport.GroupTransport(group)
	pipeline, err := tp0.AppendEntriesPipeline("node-1", nodes[1].transport.localAddress)
	if err != nil {
		t.Fatalf("AppendEntriesPipeline: %v", err)
	}
	defer func() { _ = pipeline.Close() }()

	if _, err := pipeline.AppendEntries(&raft.AppendEntriesRequest{Term: 1}, nil); err != nil {
		t.Fatalf("pipeline AppendEntries: %v", err)
	}
	// Opening the pipeline is itself a probe, and so is the send; the
	// contact only arrives with the response.
	<-pipeline.Consumer()
	rec.await(t, "contact", "node-1")

	if got := rec.count("probe", "node-1"); got < 2 {
		t.Errorf("pipeline probes for node-1 = %d, want >= 2 (open + send)", got)
	}
}

// No recorder wired is the default and must stay a silent no-op: the boot
// window before the cluster layer attaches one, and every test transport,
// runs this path.
func TestTransport_NoRecorderIsNoOp(t *testing.T) {
	nodes := makeTestCluster(t, 2)

	const group = "cluster-ctl"
	tp1 := nodes[1].transport.GroupTransport(group)
	go func() {
		for rpc := range tp1.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
		}
	}()

	tp0 := nodes[0].transport.GroupTransport(group)
	var resp raft.AppendEntriesResponse
	if err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress,
		&raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{ID: []byte("node-0")},
			Term:      1,
		}, &resp); err != nil {
		t.Fatalf("AppendEntries with no recorder: %v", err)
	}
}
