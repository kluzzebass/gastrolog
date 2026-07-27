package multiraft

import (
	"time"

	"github.com/hashicorp/raft"
)

// contact.go: per-peer Raft reachability evidence emitted by the transport.
//
// Raft already puts a liveness signal on the wire for every group: a leader
// probes each of its followers roughly every HeartbeatTimeout/10, and any
// inbound Raft RPC is proof the sender's process is up and serving. This file
// exposes that evidence so the cluster layer can derive peer liveness from it
// instead of running a second, dedicated liveness broadcast (gastrolog-1lbifx).
//
// Two distinct facts are reported, and the distinction is the whole point:
//
//   - CONTACT is positive evidence: peer P answered us, or spoke to us, in
//     group G at time T. It proves P was alive at T.
//   - PROBE is evidence that an EDGE EXISTS: this node attempted an outbound
//     Raft RPC to P in G, whether or not it succeeded. Only while we are
//     probing P does the ABSENCE of contact mean anything — a leader that
//     hears nothing back from a follower it is actively heartbeating has
//     learned something; two co-followers that never exchange Raft traffic
//     have not.
//
// Raft's topology is a star per group: followers talk only to their leader.
// So there is no Raft edge at all between two nodes that neither lead a group
// containing the other. That is not a failure mode to work around — it is why
// the recorder reports probes separately, so the consumer can tell "silent
// because dead" from "silent because there is nothing to say".
type ContactRecorder interface {
	// RecordRaftContact reports positive evidence that peerID was reachable
	// at time `at`, observed on Raft group groupID. Emitted when an outbound
	// Raft RPC to peerID completes without a transport error, and when an
	// inbound Raft RPC sent by peerID is handled.
	RecordRaftContact(peerID, groupID string, at time.Time)

	// RecordRaftProbe reports that this node ATTEMPTED an outbound Raft RPC
	// to peerID on group groupID at time `at` — success or failure. It marks
	// the Raft edge to peerID as currently live-and-probing, which is what
	// licenses a consumer to read a lapse in RecordRaftContact as "peer is
	// gone" rather than "no edge exists".
	//
	// Every successful RPC emits both a probe and a contact; a failed one
	// emits only the probe. A node that stops leading a group stops probing
	// its members, so the probe timestamp decays on its own — no leadership
	// bookkeeping is needed to retract the authority.
	RecordRaftProbe(peerID, groupID string, at time.Time)
}

// SetContactRecorder wires the reachability recorder. Safe to call before or
// after groups exist; nil (the default) disables recording entirely, which is
// what test transports and the pre-wiring boot window use.
func (t *Transport[K]) SetContactRecorder(r ContactRecorder) {
	t.contactMu.Lock()
	t.contact = r
	t.contactMu.Unlock()
}

// ContactRecorder returns the wired reachability recorder, or nil if none is
// attached. Exported as the symmetric read of SetContactRecorder so callers
// can assert the wiring actually happened — an unattached recorder is silent
// by design, which makes it exactly the kind of mistake nothing would notice.
func (t *Transport[K]) ContactRecorder() ContactRecorder {
	t.contactMu.RLock()
	r := t.contact
	t.contactMu.RUnlock()
	return r
}

// recordProbe notes an outbound RPC attempt to peerID and, when err is nil,
// the positive contact that came with it. Both timestamps are taken after the
// RPC returns: a successful call proves the peer was alive at that moment, and
// dating the probe at completion (not at dispatch) keeps a long-blocking call
// against a paused peer from looking like a fresh probe for its whole duration.
func (t *Transport[K]) recordProbe(peerID raft.ServerID, groupID K, err error) {
	r := t.ContactRecorder()
	if r == nil || peerID == "" {
		return
	}
	now := time.Now()
	gid := t.groupIDString(groupID)
	r.RecordRaftProbe(string(peerID), gid, now)
	if err == nil {
		r.RecordRaftContact(string(peerID), gid, now)
	}
}

// recordInbound notes that peerID sent us a Raft RPC on groupID. Inbound
// traffic is positive contact only, never a probe: we did not initiate it, so
// its absence tells us nothing (a leader that steps down simply stops
// heartbeating us, and must not read as dead).
func (t *Transport[K]) recordInbound(peerID string, groupID K) {
	r := t.ContactRecorder()
	if r == nil || peerID == "" {
		return
	}
	r.RecordRaftContact(peerID, t.groupIDString(groupID), time.Now())
}
