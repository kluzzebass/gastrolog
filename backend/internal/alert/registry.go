package alert

import "time"

// AlarmType is one row of the static alarm catalog: the documented
// consequence × urgency verdict (Priority) plus operator guidance for a
// single alarm type. The catalog is the code half of the table in
// docs/alarm-management-design.md — the two must agree.
//
// DelayOn, DelayOff and Latching are ENFORCED by the collector — call
// sites just Raise/Clear the raw condition. Note the distinction the
// suppression sweep drew: a duration in the CONDITION definition
// (chunking-underreplicated's "below the minimum ≥2min" is a predicate
// over each segment's FSM PublishedAt; node-unreachable's grace measures
// the FSM's replicated StateSince) is state the raiser evaluates, not
// chattering suppression, and stays at the call site.
type AlarmType struct {
	// IDPrefix is the stable type ID ("vault-leaderless", "wal-reserve").
	// The full alarm ID is IDPrefix for node-scoped types, or
	// "IDPrefix:<instanceKey>" for instance-scoped ones.
	IDPrefix string
	// Priority is the cataloged consequence × urgency verdict. Zero for
	// software faults, which sit outside the scale.
	Priority Priority
	// Source is the component name stamped on raised alarms.
	Source string
	// Cause describes the condition in operator terms.
	Cause string
	// Response is what the operator should do, surfaced in the UI.
	Response string
	// DelayOn is how long the condition must persist before the alarm
	// activates; conditions that flap below it never annunciate.
	DelayOn time.Duration
	// DelayOff is how long the condition must stay clear before the alarm
	// auto-clears; a re-raise inside it is the same occurrence.
	DelayOff time.Duration
	// Latching alarms are plain sticky: they stay standing after the
	// condition clears, until process restart. There is no release path,
	// and that is intentional — the response to a software fault is
	// report + restart, and a fault that "went away" is still a fault.
	Latching bool
	// SoftwareFault marks a defect tripwire rather than a process alarm.
	// EEMUA 191 / ISA-18.2 treat instrument and system faults as a class
	// apart: a fault's response is to REPORT the defect, not to fix an
	// operating condition, so it carries no consequence × urgency priority.
	SoftwareFault bool
}

// catalog is the static alarm-type registry. One entry per alarm type the
// system can raise; every entry carries non-empty Cause and Response.
var catalog = []AlarmType{
	// ------------------------------------------------------------------
	// Critical — data loss in progress or scheduled.
	// ------------------------------------------------------------------
	{
		IDPrefix: "segmentation-writer",
		Priority: Critical,
		Source:   "segmentation",
		Cause:    "A durable segment commit failed on this node and the working segment was abandoned for crash recovery — accepted records are at risk and ingest acks are failing.",
		Response: "Free disk space or replace the volume on the named node; ingest acks are failing until resolved.",
	},
	{
		IDPrefix: "chunking-unplannable-segment",
		Priority: Critical,
		Source:   "chunking",
		Cause:    "A segment's on-disk indexes are unreadable, so its records stay unchunked and their head copies cannot be purged.",
		Response: "Investigate segment file corruption on the named node. If the vault has a delete-disposition retention TTL these records are released unchunked at expiry — the loss is scheduled, not hypothetical.",
	},
	{
		IDPrefix: "wal-reserve",
		Priority: Critical,
		Source:   "storage",
		Cause:    "The Raft WAL's disk space reserve was lost.",
		Response: "Free disk space now — without the reserve, a full volume crashes consensus on this node.",
	},

	// ------------------------------------------------------------------
	// Software fault — defect tripwire, outside the priority scale.
	// ------------------------------------------------------------------
	{
		IDPrefix:      "orchestrator-lock-leak",
		SoftwareFault: true,
		Latching:      true, // a leaked hold can never self-clear
		Source:        "orchestrator",
		Cause:         "The orchestrator registry lock has been held or write-stuck past one minute — a lock-discipline defect, not an operating condition. The node is likely wedging.",
		Response:      "This should never fire. Capture the acquisition stack from this node's log and file it. Restarting the node is a workaround to recover service, not the response.",
	},

	// ------------------------------------------------------------------
	// High — durability or availability degraded, will compound.
	// ------------------------------------------------------------------
	{
		IDPrefix: "chunking-build-blocked",
		Priority: High,
		Source:   "chunking",
		// Collection normally materializes missing segments within seconds
		// of a seal; anything blocked minutes is stuck, not catching up.
		// (Also holds back the ghost-released-segment raise: the discard it
		// proposes self-heals the queue well inside the window, and a
		// self-healing wedge is the transition-edge Error log's story, not
		// an operator's.)
		DelayOn:  2 * time.Minute,
		Cause:    "The head-of-queue chunk is blocked on segments no local holder can supply (or a manifest referenced a released segment); later chunks cannot seal until it clears.",
		Response: "Restore a node holding the named segments, or accept the gap.",
	},
	{
		IDPrefix: "chunking-underreplicated",
		Priority: High,
		Source:   "chunking",
		Cause:    "Segments have stayed below the replication minimum; chunk planning is gated until a second node holds a copy.",
		Response: "Check that all placement nodes are up and replication is progressing. If the origin node is permanently lost, the affected records exist only there.",
	},
	{
		IDPrefix: "chunking-glcb-corrupt",
		Priority: High,
		Source:   "chunking",
		// Both heal paths (rebuild from source segments on the next build
		// pass; peer re-pull by the GLCB catch-up sweep) land within a
		// couple of sweep cycles. Corruption still standing after minutes
		// means healing is failing — that is the actionable condition.
		DelayOn:  5 * time.Minute,
		Cause:    "A sealed chunk's GLCB was unreadable on this node and was quarantined with a .corrupt suffix.",
		Response: "Heals on its own — rebuilt from source segments or re-pulled from a peer home. Only actionable if it persists: investigate disk health on this node and replica health on the vault's other homes.",
	},
	{
		IDPrefix: "chunk-unreadable",
		Priority: High,
		Source:   "retention",
		// The retry backoff schedule starts at 5m: a window past the first
		// automatic retry means a transient blip that clears on retry never
		// annunciates, while a read failure that survives it does.
		DelayOn:  10 * time.Minute,
		Cause:    "A chunk was unreadable during retention processing; a backoff retry is scheduled.",
		Response: "Retries automatically. Only actionable once retries stop resolving it: investigate disk health on this node.",
	},
	{
		IDPrefix: "cloud-backfill-stuck",
		Priority: High,
		Source:   "cloud-health",
		// Same backoff schedule as chunk-unreadable (5m first retry, same
		// unreadableBackoff schedule reused directly): the DelayOn window
		// keeps a blip that clears on the very next retry from ever
		// annunciating, while a chunk still failing past it does.
		DelayOn:  10 * time.Minute,
		Cause:    "A sealed chunk's cloud-backfill upload has kept failing after a chunk-manager registration repair was attempted; the chunk is not converging to cloud-backed on its own.",
		Response: "Read the alarm detail for the last upload error. If the chunk's GLCB is missing on disk, restore it from a peer holder or accept the gap; otherwise check cloud store credentials/connectivity and disk health on the named node.",
	},
	{
		IDPrefix: "retention-route-deferred",
		Priority: High,
		Source:   "retention",
		// The consecutive-sweep count at the call site is the condition
		// definition (like chunking-underreplicated's window), so no DelayOn.
		Cause:    "Route-disposition retention on this vault has been unable to fan out for consecutive sweeps — the only mechanism that drains the vault is deferred, so expired chunks accumulate and any size caps stay engaged.",
		Response: "Read the alarm detail for the deferral cause: free space on the starved volume (the drain resumes once free clears the floor band), drain or grow the destination vault, or — last resort, discards the routed records — set the vault's retention disposition to delete.",
	},
	{
		IDPrefix: "retention-unenforceable",
		Priority: High,
		Source:   "retention",
		// The condition is config-derived and static (not a transient
		// mid-election or mid-flap state), so no DelayOn -- unlike
		// vault-leaderless, a trigger-less policy doesn't resolve itself.
		Cause:    "The vault has retention_rules configured, but every referenced retention policy resolves with no trigger set (no maxAge, maxSize, or maxChunks) -- the vault's only drain never runs. Expired data accumulates and any size caps stay engaged until this is fixed.",
		Response: "Read the alarm detail for which policies resolved with no trigger. Add a maxAge, maxSize, or maxChunks to at least one referenced policy, or remove the vault's retention_rules if enforcement isn't intended.",
	},
	{
		IDPrefix: "chunk-suspect",
		Priority: High,
		Source:   "cloud-reconcile",
		Cause:    "A cloud-backed chunk was not found in the blob store; after the grace period it is removed from the index.",
		Response: "Check the blob store for the named chunk. After the grace period it leaves the index — restore it from a peer or accept the loss.",
	},
	{
		IDPrefix: "unknown-orphan",
		Priority: High,
		Source:   "vault",
		Cause:    "A chunk exists on disk with records but is not recognized by the replicated vault state; it is preserved for recovery.",
		Response: "Decide restore vs delete for the named chunk. Do not delete it manually without review — the cluster has no other copy.",
	},
	{
		IDPrefix: "cloud-store",
		Priority: High,
		Source:   "cloud",
		Cause:    "The vault's cloud store is unreachable; uploads have stopped.",
		Response: "Check cloud credentials, endpoint and network; sealed chunks accumulate locally until the store is restored.",
	},
	{
		IDPrefix: "vault-leaderless",
		Priority: High,
		Source:   "placement",
		DelayOn:  60 * time.Second, // placement edits legitimately resolve to no leader for a tick or two
		Cause:    "The vault's placements resolve to no leader beyond the self-healing window; retention, rotation and replication-target refresh are stopped.",
		Response: "Fix the vault's placements or the node storage configurations they reference.",
	},
	{
		IDPrefix: "vault-underreplicated",
		Priority: High,
		Source:   "placement",
		Cause:    "Placed replicas are below the vault's desired replication factor — not enough eligible storages.",
		Response: "Restore nodes or reduce the replication factor; the durability target is unmet.",
	},
	{
		IDPrefix: "vault-storage-class-missing",
		Priority: High,
		Source:   "placement",
		Cause:    "The node selected for the vault's leader has no storage of the vault's required class; the leader placement was refused.",
		Response: "Add storage of the required class on the named node, or change the vault's storage class.",
	},
	{
		IDPrefix: "vault-no-eligible-node",
		Priority: High,
		Source:   "placement",
		Cause:    "No currently-eligible node can host the vault; existing placements are retained.",
		Response: "Restore an eligible node, or relax the vault's storage requirements.",
	},
	{
		IDPrefix: "vault-soft-offline-leader",
		Priority: High,
		Source:   "placement",
		Cause:    "The vault leader's heartbeat is lost while its node is still Live, or the leader sits on an Unreachable or Maintenance node; rotation is gated.",
		Response: "Investigate the named leader node.",
	},
	{
		IDPrefix: "node-unreachable",
		Priority: High,
		Source:   "node-lifecycle",
		Cause:    "A peer node has been Unreachable past the grace period.",
		Response: "Investigate or restart the node. Removal is operator-initiated, never automatic.",
	},
	{
		IDPrefix: "vault-init",
		Priority: High,
		Source:   "orchestrator",
		Cause:    "The vault instance failed to construct from its configuration; the vault is not serving.",
		Response: "Fix the named configuration error.",
	},
	{
		IDPrefix: "pipeline-backlog-capped",
		Priority: High, // refused ingest is not lost data
		Source:   "storage",
		Cause:    "The vault's pipeline backlog is at its budget — chunking has not kept pace with ingest, and new records for this vault are refused.",
		Response: "Check chunking throughput, raise the budget, or reduce the ingest rate. This vault is refusing records now; others are unaffected.",
	},
	{
		IDPrefix: "vault-max-size-capped",
		Priority: High, // refused ingest is not lost data
		Source:   "storage",
		Cause:    "The vault is at its size budget; new records for this vault are refused.",
		Response: "Raise the budget or shorten retention. This vault is refusing records now; others are unaffected.",
	},
	{
		IDPrefix: "disk-space-exhausted",
		Priority: High, // suspended admission is not lost data
		Source:   "storage",
		Cause:    "The vault's volume is out of space; admission for this vault is suspended.",
		Response: "Free space, add capacity, raise the vault's threshold, or shorten its retention.",
	},
	{
		IDPrefix: "node-disk-space-exhausted",
		Priority: High, // suspended admission is not lost data
		Source:   "storage",
		Cause:    "This node's volume is out of space; ingest admission is suspended on this node.",
		Response: "Free space, add capacity, or shorten retention. Retention and deletes keep running.",
	},

	// ------------------------------------------------------------------
	// Low — needs attention on a human timescale.
	// ------------------------------------------------------------------
	// ingester-not-running was demoted to a log (operator razor): the
	// convergence sweep re-dispatches every tick and failed runs retry with
	// backoff — the system is already doing everything an operator could
	// ask, and the actionable detail (build/start errors) is already in the
	// log. An alarm whose response is "check the log" is a log.
	{
		IDPrefix: "pipeline-backlog-approaching",
		Priority: Low,
		Source:   "storage",
		Cause:    "The vault's pipeline backlog is approaching its budget; chunking is not keeping pace with ingest.",
		Response: "Check chunking throughput, raise the budget, or reduce the ingest rate — before records start being refused.",
	},
	{
		IDPrefix: "vault-max-size-approaching",
		Priority: Low,
		Source:   "storage",
		Cause:    "The vault is approaching its size budget.",
		Response: "Raise the budget or shorten retention — before records start being refused.",
	},
	{
		IDPrefix: "disk-space-low",
		Priority: Low,
		Source:   "storage",
		Cause:    "The vault's volume is below its free-space warn band.",
		Response: "Free space, add capacity, raise the vault's threshold, or shorten its retention.",
	},
	{
		IDPrefix: "retention-rate",
		Priority: Low,
		Source:   "retention",
		// The threshold is a product constant (orchestrator wiring): more
		// than 10 retention deletes per second sustained over a 30-second
		// window. The rate window at the call site is the condition
		// definition — the sustained-rate predicate plus its natural
		// hysteresis — not chattering suppression, so no DelayOn here.
		Cause:    "The vault has sustained more than 10 retention deletes per second over a 30-second window — usually a pathological rotation or retention configuration churning tiny chunks.",
		Response: "Review the vault's rotation and retention policy; a configuration this aggressive degrades throughput until corrected.",
	},
	{
		IDPrefix: "node-disk-space-low",
		Priority: Low,
		Source:   "storage",
		Cause:    "This node's volume is below its free-space warn band.",
		Response: "Free space, add capacity, or shorten retention.",
	},
	{
		// Verdict still open in the design doc (unique value: naming WHICH
		// vault a node-level disk condition affects). Kept as a Low alarm
		// until the razor decides — backfill is automatic when the topology
		// has a spare, so nothing more urgent waits on an operator.
		IDPrefix: "vault-home-cannot-store",
		Priority: Low,
		Source:   "placement",
		Cause:    "A home node of this vault is disk-protected; collection and chunk builds are paused there.",
		Response: "Free space on the named home node. Healthy replicas are backfilled automatically when the topology has a spare; if storable members are below the replication factor, admission for this vault throttles at the source until space frees.",
	},
}

// byID indexes the catalog by type ID.
var byID = func() map[string]AlarmType {
	m := make(map[string]AlarmType, len(catalog))
	for _, t := range catalog {
		if _, dup := m[t.IDPrefix]; dup {
			panic("alert: duplicate alarm type in catalog: " + t.IDPrefix)
		}
		m[t.IDPrefix] = t
	}
	return m
}()

// TypeByID looks up an alarm type in the catalog.
func TypeByID(typeID string) (AlarmType, bool) {
	t, ok := byID[typeID]
	return t, ok
}

// Types returns the full catalog (copy), for tests and documentation sync.
func Types() []AlarmType {
	out := make([]AlarmType, len(catalog))
	copy(out, catalog)
	return out
}

// unregisteredAlarmType is the fallback stamped on a Raise for a type ID
// missing from the catalog — itself a software fault in the raising
// component. Surfacing it (rather than dropping the raise) keeps both the
// underlying condition and the defect visible.
func unregisteredAlarmType(typeID string) AlarmType {
	return AlarmType{
		IDPrefix:      typeID,
		SoftwareFault: true,
		Source:        "alarm-system",
		Cause:         "A component raised an alarm type that is not in the alarm catalog. The underlying condition is real (see the detail text) but its priority and guidance are undocumented — a software defect in the raising component.",
		Response:      "Report this, quoting the alarm ID and detail text. Treat the detail text as the condition description until the catalog entry exists.",
	}
}
