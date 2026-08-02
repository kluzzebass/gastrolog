package alert

import (
	"strconv"
	"time"
)

// Label names an entity in alarm text the way the operator knows it: the
// quoted display name, or the bare ID when no name resolves. Raisers hold an
// ID and look the name up, and that lookup can miss — an entity that has left
// this node's registry still has to be nameable in the alarm that outlives it,
// and must never be announced as an empty pair of quotes.
func Label(name, id string) string {
	if name == "" {
		return id
	}
	return strconv.Quote(name)
}

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
		Cause:    "A segment's index file cannot be read on this node. Chunking reads that index to build chunks from, so the records in those segments can never reach a chunk from here.",
		Response: "Check disk health and segment file integrity on the named node. If this vault's retention disposition is delete, these records are discarded when their retention expires — that loss is scheduled, not hypothetical, so act before then.",
	},
	{
		IDPrefix: "chunking-retention-giveup",
		Priority: Critical,
		Source:   "chunking",
		// Raised on the first give-up pass, cleared when the vault next seals
		// a chunk. In a HEALTHY vault an occasional island-origin give-up is
		// cleared by the next seal inside this window, so it never annunciates;
		// a STARVED vault seals nothing, so the raise stands past DelayOn and
		// annunciates instead of hiding inside the vault's routine
		// retention-noise WARNs.
		DelayOn:  2 * time.Minute,
		Cause:    "Records are reaching this vault but leaving it without ever being written into a chunk. A published segment has a limited window to get chunked; when that window expires the segment is released and its records are discarded. This vault keeps hitting the window, so it is losing records continuously — and on a cloud-backed vault they never reached cloud either.",
		Response: "Chunking will not chunk a segment until a second node also holds a copy of it, so this usually means copies are not reaching the vault's other home nodes. Check that those nodes are up and that replication is progressing. Lengthening the vault's retention window buys time but does not fix the cause. Clears once the vault seals a chunk again.",
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
		// Raised per stranded chunk once the seal-resume retry budget is
		// exhausted. No DelayOn: the budget is already the patience, so the
		// condition has by definition stopped self-healing by the time it
		// annunciates. High rather than Critical — the records are still on
		// disk; what is lost is their path to durability.
		IDPrefix: "seal-stranded",
		Priority: High,
		Source:   "vault-lifecycle",
		Cause:    "A chunk stopped part-way through sealing and has exhausted its retries. It is neither writable nor durable-complete, so its records cannot be queried and nothing downstream — replication, cloud upload, retention — will touch them. They sit on this node with no path forward.",
		Response: "Read the alarm detail for the vault and chunk, then check disk health and free space on the named node. Do not delete the chunk directory without review: its records have not reached a sealed chunk anywhere.",
	},
	{
		// Raised when the announce applier keeps failing. No DelayOn: the raise
		// already sits behind the applier's own retries, and a divergence
		// between local disk and the replicated manifest does not heal on a
		// timer. High rather than Critical — nothing is being destroyed, but
		// the drift compounds with every seal.
		IDPrefix: "vault-announce-failing",
		Priority: High,
		Source:   "vault-lifecycle",
		Cause:    "This node seals and uploads chunks successfully but cannot commit that fact to the vault's replicated metadata. The cluster's record of the vault is drifting behind what is actually on this node's disk — chunks that exist here are invisible to queries routed elsewhere, and retention cannot account for them.",
		Response: "Read the alarm detail for the failing operation and error. Check this node's vault-ctl Raft group: whether it has a leader, whether this node is a member, and whether it is committing. Do not decommission this node while this stands — its chunks are not represented anywhere else.",
	},
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
		Cause:    "A chunk cannot be built because segment bytes it references are not on this node and no reachable holder has supplied them. Chunks seal in order within a vault, so one stuck chunk holds up every chunk behind it.",
		Response: "Bring back a node that holds the named segments. If that node is gone for good, those records exist nowhere else — the alternative is to accept the gap.",
	},
	{
		IDPrefix: "chunking-underreplicated",
		Priority: High,
		Source:   "chunking",
		Cause:    "Segments have sat with fewer than two copies in the cluster. Chunking deliberately waits for a second copy before chunking a segment, so that losing one node mid-build cannot take the only copy with it. Until replication catches up, these records stay unchunked.",
		Response: "Check that every node in this vault's placement is up and that replication is progressing. If the node that originated these segments is permanently lost, the records exist nowhere else.",
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
		Cause:    "A sealed chunk's GLCB — the on-disk chunk file — failed to read on this node and was renamed aside so it is not served. Queries answered here skip those chunks; the vault's other homes still serve their own copies.",
		Response: "This normally heals itself: the chunk is rebuilt from its source segments or re-pulled from another home. Only act if it keeps standing — check disk health on this node, and whether the vault's other homes still hold good copies.",
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
		IDPrefix: "retention-deferred",
		Priority: High,
		Source:   "retention",
		// The consecutive-sweep count at the call site is the condition
		// definition (like chunking-underreplicated's window), so no DelayOn.
		// Covers both non-delete dispositions that can stall: route fan-out
		// and transfer.
		Cause:    "Retention on this vault has been unable to complete its configured disposition (route fan-out or transfer) for consecutive sweeps — the only mechanism that drains the vault is deferred, so expired chunks accumulate and any size caps stay engaged.",
		Response: "Read the alarm detail for the deferral cause and disposition: free space on the starved volume (the drain resumes once free clears the floor band), drain or grow the destination/target vault, or — last resort, discards the records — set the vault's retention disposition to delete.",
	},
	{
		IDPrefix: "retention-unenforceable",
		Priority: High,
		Source:   "retention",
		// The condition is config-derived and static (not a transient
		// mid-election or mid-flap state), so no DelayOn -- unlike
		// vault-leaderless, a trigger-less policy doesn't resolve itself.
		Cause:    "The vault has retention_rules configured, but every referenced retention policy resolves with no trigger set (no maxAge, maxSize, or maxChunks) -- the vault's only drain never runs, so it grows until the volume's free-space thresholds (the storage's diskFreeWarn / diskFreeFloor) engage. There is no per-vault size default to catch this: a vault with no stated bound is bounded only by its volume.",
		Response: "Read the alarm detail for which policies resolved with no trigger. Add a maxAge, maxSize, or maxChunks to at least one referenced policy -- maxSize alone is enough to both bound the vault and enable draining (it drains oldest chunks past the bound regardless of the refuse flag); add refuse=true to also refuse admission while over it, since refuse defaults off. Do NOT remove the vault's retention_rules to silence this -- detaching every policy leaves the vault with no drain at all, growing until the volume's free-space thresholds engage.",
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
		Response: "Investigate or restart the node.",
	},
	{
		IDPrefix: "vault-init",
		Priority: High,
		Source:   "orchestrator",
		Cause:    "The vault instance failed to construct from its configuration; the vault is not serving.",
		Response: "Fix the named configuration error.",
	},
	{
		// The config dispatcher applies each committed config mutation to
		// this node's orchestrator inside FSM.Apply. When
		// a side effect fails (AddVault, reconcile, policy reload, TLS reload,
		// membership refresh, …) the mutation is already durable in Raft but
		// this node's running state diverges from it. The failure becomes a
		// standing per-entity reconcile obligation and this alarm. No DelayOn:
		// the apply does not self-heal on a timer — retry is event-driven (the
		// next dispatch touching the entity, or startup replay) — so an
		// unresolved divergence must annunciate immediately, like vault-init.
		// Non-latching: it clears the moment a later apply of the same entity
		// succeeds.
		IDPrefix: "config-side-effect-failed",
		Priority: High,
		Source:   "config-dispatch",
		Cause:    "A committed configuration change could not be applied to this node's orchestrator; this node's running state diverges from the replicated config. The condition is per-entity and node-scoped — other nodes may have applied the same change cleanly.",
		Response: "Read the alarm detail for the failing entity, operation and error. It retries automatically on the next config change touching that entity and on node restart (startup reconcile). If it persists, resolve the named error on this node (e.g. disk, storage configuration, or a construction error).",
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
		Cause:    "The vault is at its max-size bound; new records for this vault are refused.",
		Response: "Raise the bound (set a larger max size on an attached retention policy) or shorten retention. This vault is refusing records now; others are unaffected.",
	},
	{
		// Refusal on max-age and max-chunks bounds; max-size has its own
		// alarm pair above. One type, cause named in the detail text and
		// disambiguated in the entity key (<vault>/age, <vault>/count) so
		// both can stand on one vault at once. No "approaching" variant —
		// unlike max-size this isn't an instantaneous measurement to lead
		// with, it's a sweep verdict (violated, swept, still violated),
		// and alarms-no-ceremony argues against inventing a lead-in for
		// something clock-free.
		IDPrefix: "vault-bound-capped",
		Priority: High, // refused ingest is not lost data
		Source:   "retention",
		Cause:    "A retention policy's max-age or max-chunks bound is still violated after retention swept and attempted to clear it; new records for this vault are refused. Only happens when the stating policy has refuse enabled explicitly — refuse defaults off, so a plain drain-only policy never refuses.",
		Response: "Read the alarm detail for which bound and vault. If retention-deferred is also standing for this vault, that names why the sweep isn't clearing it; otherwise raise the bound, shorten it enough that draining can keep up, or turn the policy's refuse flag off (or simply leave it unset) to accept drain-only.",
	},
	{
		// The free-space thresholds live on the storage entity a vault's
		// placements reference, and the guard evaluates each storage ONCE:
		// the instance key is the storage ID, and the detail text names the
		// storage and its node, since every vault placed there refuses
		// together for the same physical condition.
		IDPrefix: "disk-space-exhausted",
		Priority: High, // suspended admission is not lost data
		Source:   "storage",
		Cause:    "A storage is out of space; admission for every vault placed on it is suspended.",
		Response: "Free space, add capacity, raise the storage's threshold, or shorten retention for the vaults placed on it.",
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
		Cause:    "The vault is approaching its max-size bound.",
		Response: "Raise the bound (set a larger max size on an attached retention policy) or shorten retention — before records start being refused.",
	},
	{
		// Instance key is the storage ID — see disk-space-exhausted's
		// comment.
		IDPrefix: "disk-space-low",
		Priority: Low,
		Source:   "storage",
		Cause:    "A storage is below its free-space warn band.",
		Response: "Free space, add capacity, raise the storage's threshold, or shorten retention for the vaults placed on it.",
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
