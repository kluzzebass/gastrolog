package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	"gastrolog/internal/units"
)

// Disk-space guard (independently shippable slice of the pipeline
// backpressure epic). The 2026-07-08 incident: unbounded pipeline backlog
// filled the volume over ~4.5h with zero operator signal; the first symptom
// was three simultaneous node panics on Raft WAL fsync ENOSPC — the worst
// possible outcome, because it kills consensus first.
//
// Two thresholds, both on the minimum free space across the node's data
// paths:
//
//   - WARN: raise the disk-space alarm (operator action: free space, add
//     capacity, or shorten retention). Cleared with hysteresis.
//   - FLOOR: additionally enter protect mode — the node stops ACCEPTING new
//     work (ingest admission rejects, catch-up pulls pause, pressure-aware
//     ingesters see Critical) while consensus and the paths that FREE space
//     (retention expunge, delete acks) keep running.
const (
	diskGuardJobName  = "disk-guard"
	diskGuardSchedule = "*/15 * * * * *" // every 15 seconds

	// Alarm type IDs raised by the disk guard. Two setpoints on one
	// measurement are two alarms, each with its own cataloged priority
	// (the HI/HIHI pattern): the approaching/low rows are Low, the
	// capped/exhausted rows are High.
	alarmNodeDiskLow          = "node-disk-space-low"
	alarmNodeDiskExhausted    = "node-disk-space-exhausted"
	alarmStorageDiskLow       = "disk-space-low"
	alarmStorageDiskExhausted = "disk-space-exhausted"
	alarmBacklogApproaching   = "pipeline-backlog-approaching"
	alarmBacklogCapped        = "pipeline-backlog-capped"
	alarmMaxSizeApproaching   = "vault-max-size-approaching"
	alarmMaxSizeCapped        = "vault-max-size-capped"

	// alarmVaultBoundCapped covers the two generalized refuse-eligible
	// bounds gastrolog-5yfaqj added (age, chunk-count): one alarm type,
	// cause named in the detail text and disambiguated in the entity key
	// (<vaultID>/age, <vaultID>/count) so both bounds can stand at once on
	// the same vault without colliding. Deliberately NOT merged with
	// alarmMaxSizeCapped — that pair is instantaneous and already shipped;
	// these two are sweep-verdict-driven, and no "approaching" variant
	// exists for them (no continuous measurement to lead with, and
	// alarms-no-ceremony argues against inventing one).
	alarmVaultBoundCapped = "vault-bound-capped"

	// Node-default thresholds, as expressions an operator could type into a
	// disk-free-warn / disk-free-floor field themselves. A default must be
	// expressible in the field's own vocabulary — the earlier
	// max(fraction·volume, hardBytes)-with-clamp formula was not typeable
	// and therefore not a legitimate default (operator directive; see
	// docs/product-defaults-policy-design.md). Percentages scale with the
	// volume and resolve per node through the shared resolver.
	defaultDiskFreeWarn  = "10%"
	defaultDiskFreeFloor = "3%"
)

// clearAbove is the hysteresis exit bound: thresholds release only once
// free space exceeds them by 25% (integer math — a float factor here
// crashes gosec's range analyzer, and deadbands shouldn't need floats).
func clearAbove(threshold uint64) uint64 {
	return threshold + threshold/4
}

// fmtBytes formats a byte count for log fields. Disk free-space and thresholds
// are physical quantities always within int64.
func fmtBytes(b uint64) string {
	return units.FormatBytesDisplay(int64(b)) //nolint:gosec // disk bytes, display only
}

// admissionResumeAbove is the free-space level ingest admission must clear to
// resume after a floor breach: the WARN band plus hysteresis, never below the
// drain gate's resume level. Admission ENGAGES at the floor but RESUMES only
// here — the wide asymmetric deadband reserves headroom for the release burst.
// Reopening the firehose at floor+hysteresis let the deferred backlog re-cross
// the floor within one 15s sample, walking troughs down (944MB->20MB) until
// nodes panicked on WAL ENOSPC (gastrolog-67gvjo).
func admissionResumeAbove(floorAt, warnAt uint64) uint64 {
	return max(clearAbove(warnAt), clearAbove(floorAt))
}

// ErrDiskProtect rejects new work while the node is below its free-space
// floor. Producers treat it as retryable backpressure.
var ErrDiskProtect = errors.New("node is out of disk space: ingest admission suspended until space is freed")

// diskGuard samples free space on the node's data paths and drives the
// alarm + protect state. sample is injectable for tests.
type diskGuard struct {
	paths  []string
	sample func(path string) (free, total uint64, err error)
	logger *slog.Logger

	// Node-level threshold expressions ("10%", "10GB"). Never empty: set to
	// the typeable defaults at construction. Per-storage overrides come from
	// the config store (system.FileStorage.DiskFreeWarn/DiskFreeFloor,
	// gastrolog-9akebz — moved off VaultConfig) — there is deliberately no
	// env-var channel, which would be invisible at runtime and silently
	// per-node divergent (gastrolog-2mrfdw).
	warnExpr  string
	floorExpr string

	// protect is the ADMISSION GATE — ingest admission, retention
	// re-routing, replica catch-up. It engages at the floor and resumes
	// only above the WARN band (admissionResumeAbove), reserving headroom
	// for the release burst so the deferred backlog cannot re-cross the
	// floor in one sampling window (the ratchet, gastrolog-67gvjo).
	protect atomic.Bool
	// deferWrites is the DRAIN GATE — chunking builds and the collection
	// pulls that feed them. It engages at the floor but resumes EARLIER
	// (floor + hysteresis) than admission: builds seal the backlog into
	// chunks retention can expunge to free space, so the delete-disposition
	// drain would deadlock if they waited for the admission gate.
	deferWrites atomic.Bool

	// vaultFootprint measures a vault's whole local disk claim for the
	// max-size bound. Injected by the orchestrator (chunk + index bytes
	// plus pipeline segment backlog); injectable for tests.
	vaultFootprint func(glid.GLID) int64

	// vaultBacklogBytes measures a vault's pipeline backlog: unreleased
	// completed-segment bytes in the vault-ctl registry. FSM-replicated, so
	// every node computes the same value — no peer broadcast needed (unlike
	// disk state, which only the owning node can sample). Injected by the
	// orchestrator; injectable for tests.
	vaultBacklogBytes func(glid.GLID) int64
	// backlogBudget is the cluster-global per-vault backlog budget
	// (ClusterSettings.pipeline_backlog_max_bytes). 0 = unbounded. Refreshed
	// each guard tick from server settings.
	backlogBudget atomic.Uint64

	mu          sync.Mutex
	alarmRaised bool
	// storages holds per-storage disk-guard state (gastrolog-9akebz): each
	// LOCALLY-hosted storage is evaluated ONCE — one statfs, one warn/floor
	// verdict, one alarm pair — regardless of how many vaults are placed on
	// it. Replaces the old per-vault statfs duplication (N vaults on one
	// volume each independently sampling and alarming the same condition).
	// Guarded by mu; keyed by storage ID.
	storages map[string]*storageDiskGuard
	// vaults holds per-vault guard state for the OTHER cap-and-refuse
	// dimensions (max-size, backlog, age/count bounds) plus the storage IDs
	// this vault's LOCAL placements reference — used to derive
	// vaultStorageProtected from the `storages` map. Guarded by mu.
	vaults map[glid.GLID]*vaultDiskGuard
}

// storageDiskGuard is one storage's disk-guard state — evaluated ONCE per
// storage regardless of how many vaults are placed on it (gastrolog-9akebz).
// Only LOCALLY-hosted storages get an entry: only the node that owns the
// path can statfs it. name/node feed the alarm text and the admission
// detail signal.
type storageDiskGuard struct {
	path string
	name string
	node string
	// Threshold expressions ("10%", "10GB"); empty inherits the node level.
	// Kept as expressions because a percentage can only be resolved against
	// the volume actually sampled at evaluation time.
	warnExpr  string
	floorExpr string
	protect   atomic.Bool

	// lastFree/lastFloor cache the most recent sample so the admission
	// detail signal can name "free vs floor" honestly (facts before
	// speculation) without re-sampling on the hot admission path.
	lastFree  atomic.Uint64
	lastFloor atomic.Uint64

	alarmRaised bool
}

// vaultDiskGuard is one vault's disk-guard state for the cap-and-refuse
// dimensions that stay per-vault (max-size, backlog, age/count bounds). The
// free-space protect dimension moved to storageDiskGuard (gastrolog-9akebz);
// storageIDs is this vault's link to it.
type vaultDiskGuard struct {
	name string

	// storageIDs are the storage IDs this vault's LOCAL placements
	// reference (placements hosted on THIS node) — used to derive
	// vaultStorageProtected by looking up each ID in the `storages` map.
	// Placements hosted on OTHER nodes are covered by the peer-broadcast
	// fallback in vaultAdmissionCauses: the node that hosts the storage
	// detects the breach and broadcasts the vault ID, so this node never
	// needs to resolve a remote storage ID to anything.
	storageIDs []string

	// ageBoundLabel/chunkCountBoundLabel cache the effective refuse-eligible
	// bound's operator-facing expression (e.g. "3d", "100"), resolved from
	// config at the same discovery tick as maxSizeBytes — for the admission
	// detail signal (gastrolog-9akebz). Empty when no refuse-eligible policy
	// states that dimension; the detail then falls back to a label-less
	// sentence rather than fabricating a number.
	ageBoundLabel        string
	chunkCountBoundLabel string

	// Max-size bound (cap-and-refuse): the vault's whole local disk claim
	// — sealed chunks, indexes, pipeline segment backlog — measured against
	// maxSizeBytes. 0 means no refuse bound: the vault's only size policies
	// are refuse=false (soft — drain-only, operator's explicit opt-out,
	// gastrolog-5yfaqj); evaluateVaults clears any standing cap/alarm for it.
	// Otherwise non-zero: refuse-eligible policy min, else the creation
	// default (gastrolog-1epfgb). Distinct from the free-space thresholds:
	// those protect the VOLUME, this bound covers the VAULT.
	maxSizeBytes    uint64
	capped          atomic.Bool
	sizeAlarmRaised bool

	// Pipeline backlog budget (cap-and-refuse): unreleased registry segment
	// bytes measured against the cluster-global backlogBudget. The OPERATING
	// bound — engages well before disk pressure so the backlog is bounded by
	// policy, not by the volume filling up (design-notes R2). Distinct from
	// maxSizeBytes: the max-size bound covers everything the vault RETAINS,
	// the backlog budget covers what chunking has not yet drained.
	backlogCapped      atomic.Bool
	backlogAlarmRaised bool

	// Age/chunk-count bounds (gastrolog-5yfaqj): cap-and-refuse like
	// max-size, but NOT instantaneous. Normal operation transiently
	// violates both between a chunk's seal and the next retention sweep,
	// so refusing on that transient would be pure flapping. These flags
	// are set by the retention runner's post-sweep bound check
	// (retention.go), which recomputes each bound AFTER a full sweep has
	// run and attempted to clear it: refusal-worthy only once the sweep
	// has swept-and-failed, never on the pre-sweep transient. The guard
	// just folds whatever the runner last reported into
	// vaultAdmissionCauses/NodeStats — no clock, no evaluateVaults tick
	// involvement (contrast reconcileVaultSizeCap's instantaneous check).
	ageBoundCapped        atomic.Bool
	ageBoundAlarmRaised   bool
	chunkCountBoundCapped atomic.Bool
	chunkCountAlarmRaised bool
}

func newDiskGuardWithLogger(paths []string, logger *slog.Logger) *diskGuard {
	g := newDiskGuard(paths)
	g.logger = logger
	return g
}

func newDiskGuard(paths []string) *diskGuard {
	return &diskGuard{
		paths:     paths,
		sample:    statfsSample,
		warnExpr:  defaultDiskFreeWarn,
		floorExpr: defaultDiskFreeFloor,
	}
}

func statfsSample(path string) (uint64, uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bsize := uint64(st.Bsize)
	return st.Bavail * bsize, st.Blocks * bsize, nil
}

// worstFree returns the minimum free bytes and its volume total across the
// guarded paths (deduplicating exact repeats is unnecessary — sampling the
// same volume twice yields the same numbers).
func (g *diskGuard) worstFree() (free, total uint64, ok bool) {
	return g.worstFreeOf(g.paths)
}

// resolveThreshold resolves a disk-free threshold expression against the
// sampled volume size through the shared resolver, inheriting fallbackExpr
// when expr is unset. fallbackExpr is always a validated expression (the
// typeable defaults or the checked env override), so it parses; a malformed
// expr — impossible after write-time validation — falls back to it rather
// than to a silent 0 threshold.
func resolveThreshold(expr, fallbackExpr string, total uint64) uint64 {
	if t, err := system.ResolveSizeOrPercent(expr, total, fallbackExpr); err == nil {
		return t
	}
	t, _ := system.ResolveSizeOrPercent("", total, fallbackExpr)
	return t
}

func (g *diskGuard) warnThreshold(total uint64) uint64 {
	return resolveThreshold(g.warnExpr, defaultDiskFreeWarn, total)
}

func (g *diskGuard) floorThreshold(total uint64) uint64 {
	return resolveThreshold(g.floorExpr, defaultDiskFreeFloor, total)
}

// SetVaultGuard registers (or updates) a vault's guard entry. storageIDs are
// the storage IDs this vault's LOCAL placements reference (the link to the
// `storages` map for deriving vaultStorageProtected); maxSizeBytes of 0
// means no budget (non-file vaults). Called from the discovery refresh;
// removal via RemoveVaultGuard / retainVaultGuards.
func (g *diskGuard) SetVaultGuard(vaultID glid.GLID, name string, storageIDs []string, maxSizeBytes uint64, ageBoundLabel, chunkCountBoundLabel string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.vaults == nil {
		g.vaults = make(map[glid.GLID]*vaultDiskGuard)
	}
	v := g.vaults[vaultID]
	if v == nil {
		v = &vaultDiskGuard{}
		g.vaults[vaultID] = v
	}
	v.name = name
	v.storageIDs = storageIDs
	v.maxSizeBytes = maxSizeBytes
	v.ageBoundLabel = ageBoundLabel
	v.chunkCountBoundLabel = chunkCountBoundLabel
}

// RemoveVaultGuard drops a vault's guard entry (vault deleted / no longer local).
func (g *diskGuard) RemoveVaultGuard(vaultID glid.GLID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.vaults, vaultID)
}

// retainVaultGuards drops every entry not in keep. Pairs with the
// discovery-based refresh: vaults removed from config or no longer placed
// on this node fall out on the next tick. A pruned entry's standing alarm
// is cleared — nothing would ever clear it once the entry stops being
// evaluated. gastrolog-9akebz: the disk-space-low/exhausted alarm no longer
// lives here — it moved to storageDiskGuard/retainStorageGuards, keyed by
// storage ID instead of vault ID.
func (g *diskGuard) retainVaultGuards(keep map[glid.GLID]bool, alerts alert.Sink) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, v := range g.vaults {
		if keep[id] {
			continue
		}
		if v.sizeAlarmRaised && alerts != nil {
			alerts.Clear(alarmMaxSizeApproaching, id.String())
			alerts.Clear(alarmMaxSizeCapped, id.String())
		}
		if v.backlogAlarmRaised && alerts != nil {
			alerts.Clear(alarmBacklogApproaching, id.String())
			alerts.Clear(alarmBacklogCapped, id.String())
		}
		if v.ageBoundAlarmRaised && alerts != nil {
			alerts.Clear(alarmVaultBoundCapped, id.String()+"/age")
		}
		if v.chunkCountAlarmRaised && alerts != nil {
			alerts.Clear(alarmVaultBoundCapped, id.String()+"/count")
		}
		delete(g.vaults, id)
	}
}

// SetStorageGuard registers (or updates) a LOCALLY-hosted storage's guard
// entry. node is the operator-facing display name of the hosting node,
// already resolved by the caller (nodeDisplayName) — never a raw ID by the
// time it lands here, so every alarm/detail string built from it reads a
// name. path is the storage's filesystem path; warnExpr/floorExpr are
// size-or-percent expressions ("10GB", "10%"), empty inherits the node
// level. Called from the discovery refresh; removal via retainStorageGuards
// (gastrolog-9akebz).
func (g *diskGuard) SetStorageGuard(storageID, name, node, path, warnExpr, floorExpr string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.storages == nil {
		g.storages = make(map[string]*storageDiskGuard)
	}
	s := g.storages[storageID]
	if s == nil {
		s = &storageDiskGuard{}
		g.storages[storageID] = s
	}
	s.path = path
	s.name = name
	s.node = node
	s.warnExpr = warnExpr
	s.floorExpr = floorExpr
}

// retainStorageGuards drops every storage entry not in keep — a storage
// removed from config, or no longer hosted on this node, falls out on the
// next tick. A pruned entry's standing alarm is cleared so it cannot strand
// (retainVaultGuards precedent — gastrolog-9akebz sibling for storages).
func (g *diskGuard) retainStorageGuards(keep map[string]bool, alerts alert.Sink) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, s := range g.storages {
		if keep[id] {
			continue
		}
		if s.alarmRaised && alerts != nil {
			alerts.Clear(alarmStorageDiskLow, id)
			alerts.Clear(alarmStorageDiskExhausted, id)
		}
		delete(g.storages, id)
	}
}

// vaultStorageProtected reports whether this vault refuses admission
// because a storage backing one of its LOCAL placements is below its
// free-space floor (gastrolog-9akebz). Placements hosted on other nodes are
// covered by the peer-broadcast fallback in vaultAdmissionCauses — the node
// that hosts the storage detects the breach and broadcasts the vault ID.
// False for unknown vaults.
func (g *diskGuard) vaultStorageProtected(vaultID glid.GLID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[vaultID]
	if v == nil {
		return false
	}
	for _, sid := range v.storageIDs {
		if s := g.storages[sid]; s != nil && s.protect.Load() {
			return true
		}
	}
	return false
}

// vaultStorageProtectDetail returns terse detail text for vaultID's
// storage-protect cause when a LOCAL placement's storage is the one
// currently below floor: the storage's name, node, and free-vs-floor
// numbers — facts this node sampled directly, not a client-side guess.
// Empty when no local storage is protecting this vault (the cause came
// from a peer broadcast instead — see vaultAdmissionCauses).
func (g *diskGuard) vaultStorageProtectDetail(vaultID glid.GLID) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[vaultID]
	if v == nil {
		return ""
	}
	for _, sid := range v.storageIDs {
		s := g.storages[sid]
		if s == nil || !s.protect.Load() {
			continue
		}
		return fmt.Sprintf("storage %s on node %s: %s free, floor %s",
			s.name, s.node, fmtBytes(s.lastFree.Load()), fmtBytes(s.lastFloor.Load()))
	}
	return ""
}

// vaultMaxSizeBoundDetail names the max-size bound value for the admission
// detail signal. The bound is a pure function of replicated config
// (resolveVaultSizeBoundSource at refresh time), so it is honest whether
// this node's own footprint sample or a peer's tripped the cap. Empty for
// unknown vaults.
func (g *diskGuard) vaultMaxSizeBoundDetail(vaultID glid.GLID) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[vaultID]
	if v == nil {
		return ""
	}
	return "max-size bound: " + fmtBytes(v.maxSizeBytes)
}

// backlogBudgetDetail names the cluster-global backlog budget for the
// admission detail signal — same value on every node (ClusterSettings), so
// no peer lookup is needed for the detail text.
func (g *diskGuard) backlogBudgetDetail() string {
	return "backlog budget: " + fmtBytes(g.backlogBudget.Load())
}

// vaultAgeBoundDetail names the effective max-age bound's operator-facing
// expression for the admission detail signal, cached at refresh time from
// config (see refreshVaultDiskGuards' attachedAgeBound). Empty when no
// refuse-eligible policy states a usable MaxAge, or the vault is unknown —
// callers fall back to a label-less sentence rather than fabricate a value.
func (g *diskGuard) vaultAgeBoundDetail(vaultID glid.GLID) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[vaultID]
	if v == nil || v.ageBoundLabel == "" {
		return ""
	}
	return fmt.Sprintf("max-age bound (%s) still violated after a sweep", v.ageBoundLabel)
}

// vaultChunkCountBoundDetail is vaultAgeBoundDetail's max-chunks sibling.
func (g *diskGuard) vaultChunkCountBoundDetail(vaultID glid.GLID) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[vaultID]
	if v == nil || v.chunkCountBoundLabel == "" {
		return ""
	}
	return fmt.Sprintf("max-chunks bound (%s) still violated after a sweep", v.chunkCountBoundLabel)
}

// vaultSizeCapped reports whether this vault's local disk claim has reached
// its max-size bound. Lock-free read; false for unknown vaults.
func (g *diskGuard) vaultSizeCapped(vaultID glid.GLID) bool {
	g.mu.Lock()
	v := g.vaults[vaultID]
	g.mu.Unlock()
	return v != nil && v.capped.Load()
}

// vaultBacklogCapped reports whether this vault's pipeline backlog has
// reached the cluster-global budget. False for unknown vaults.
func (g *diskGuard) vaultBacklogCapped(vaultID glid.GLID) bool {
	g.mu.Lock()
	v := g.vaults[vaultID]
	g.mu.Unlock()
	return v != nil && v.backlogCapped.Load()
}

// vaultAgeBoundCapped reports whether this vault's max-age retention bound
// is still violated after the retention runner's last sweep, on a policy
// with refuse=true. False for unknown vaults (no file-vault guard entry —
// see refreshVaultDiskGuards) and for vaults where no sweep has yet run.
func (g *diskGuard) vaultAgeBoundCapped(vaultID glid.GLID) bool {
	g.mu.Lock()
	v := g.vaults[vaultID]
	g.mu.Unlock()
	return v != nil && v.ageBoundCapped.Load()
}

// vaultChunkCountBoundCapped is vaultAgeBoundCapped's max-chunks sibling.
func (g *diskGuard) vaultChunkCountBoundCapped(vaultID glid.GLID) bool {
	g.mu.Lock()
	v := g.vaults[vaultID]
	g.mu.Unlock()
	return v != nil && v.chunkCountBoundCapped.Load()
}

// setVaultAgeBoundCapped records the retention runner's post-sweep verdict
// for the max-age bound and raises/clears the shared vault-bound-capped
// alarm on the transition. Silently no-ops for a vault with no guard
// entry — age/count refusal is scoped to the same file vaults max-size
// refusal already covers (refreshVaultDiskGuards only guards file vaults);
// a memory vault or a vault whose guard entry hasn't been registered yet
// has nothing to fold the verdict into.
//
// Holds g.mu for the WHOLE operation — map lookup, atomic Swap, and the
// alarm Raise/Clear — rather than releasing it between the Swap and the
// alarm call (gastrolog-5yfaqj review fix, minor: the earlier two-lock
// version left a window where a concurrent retainVaultGuards prune could
// delete this vault's entry and, reading raisedFlag before this call had
// set it, skip clearing an alarm this call was about to raise — stranding
// a Raise that fires into the void right after prune). One lock for the
// whole sequence matches every other guard reconcile function in this
// file (reconcileVaultSizeAlarm, reconcileVaultBacklogAlarm,
// reconcileVaultAlarm all call alerts.Raise/Clear while holding g.mu) and
// makes the two prune paths mutually exclusive with this one: either this
// completes first and retainVaultGuards' next pass sees + clears the
// raised alarm, or retainVaultGuards runs first and this call's own map
// lookup returns nil before touching the alarm at all. This is IN
// ADDITION to the C1 runner-GC fix (retentionSweepAll calling
// SetVaultAgeBoundCapped(false) before deleting a pruned runner) — that
// fix closes the leadership-loss strand at the retention-runner layer;
// this one closes a structurally different race at the disk-guard-entry
// layer (retainVaultGuards prunes on vault-removed-from-config/placement-
// moved, a separate discovery sweep from retention's own runner GC), so
// relying on C1 alone would not have covered it.
func (g *diskGuard) setVaultAgeBoundCapped(alerts alert.Sink, id glid.GLID, capped bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[id]
	if v == nil {
		return
	}
	was := v.ageBoundCapped.Swap(capped)
	if was == capped {
		return
	}
	g.reconcileVaultBoundAlarmLocked(alerts, id, v, "age", capped, &v.ageBoundAlarmRaised,
		fmt.Sprintf("Vault %s's max-age retention bound is still violated after retention swept and attempted to clear it — "+
			"new records for this vault are REFUSED (the stating policy has refuse enabled). "+
			"Read the retention-deferred alarm, if standing, for why the sweep isn't clearing it.", v.name),
		fmt.Sprintf("vault max-age bound engaged — admission refused for vault %s (swept and still violated)", v.name),
		"vault max-age bound released — admission resumed for vault "+v.name)
}

// setVaultChunkCountBoundCapped is setVaultAgeBoundCapped's max-chunks
// sibling — same single-lock-for-the-whole-operation contract.
func (g *diskGuard) setVaultChunkCountBoundCapped(alerts alert.Sink, id glid.GLID, capped bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.vaults[id]
	if v == nil {
		return
	}
	was := v.chunkCountBoundCapped.Swap(capped)
	if was == capped {
		return
	}
	g.reconcileVaultBoundAlarmLocked(alerts, id, v, "count", capped, &v.chunkCountAlarmRaised,
		fmt.Sprintf("Vault %s's max-chunks retention bound is still violated after retention swept and attempted to clear it — "+
			"new records for this vault are REFUSED (the stating policy has refuse enabled). "+
			"Read the retention-deferred alarm, if standing, for why the sweep isn't clearing it.", v.name),
		fmt.Sprintf("vault chunk-count bound engaged — admission refused for vault %s (swept and still violated)", v.name),
		"vault chunk-count bound released — admission resumed for vault "+v.name)
}

// reconcileVaultBoundAlarmLocked raises/clears the shared
// vault-bound-capped alarm for one cause ("age"/"count") on a transition,
// and logs it — bound transitions are otherwise silent, the same razor
// refreshVaultDiskGuards' max-size change log follows. cause disambiguates
// the entity key so age and chunk-count can stand on the same vault at
// once without colliding on one alarm slot.
//
// Caller must already hold g.mu for the duration — see
// setVaultAgeBoundCapped's doc comment for why this must not re-lock (or
// release and re-lock) internally.
func (g *diskGuard) reconcileVaultBoundAlarmLocked(alerts alert.Sink, id glid.GLID, v *vaultDiskGuard, cause string, capped bool, raisedFlag *bool, detail, engagedLog, releasedLog string) {
	key := id.String() + "/" + cause
	*raisedFlag = capped
	if capped {
		if g.logger != nil {
			g.logger.Warn("retention: " + engagedLog)
		}
		if alerts != nil {
			alerts.Raise(alarmVaultBoundCapped, key, detail)
		}
		return
	}
	if g.logger != nil {
		g.logger.Info("retention: " + releasedLog)
	}
	if alerts != nil {
		alerts.Clear(alarmVaultBoundCapped, key)
	}
}

// sizeCappedVaults lists the vaults currently at their max-size bound.
// Broadcast in NodeStats so peers' admission gates honor it.
func (g *diskGuard) sizeCappedVaults() []glid.GLID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []glid.GLID
	for id, v := range g.vaults {
		if v.capped.Load() {
			out = append(out, id)
		}
	}
	return out
}

// storageProtectedVaults lists the vaults currently refusing admission
// because a LOCALLY-hosted storage backing one of their placements is below
// its free-space floor (gastrolog-9akebz). Broadcast in NodeStats so peers'
// admission gates honor it — the node that samples a storage is the only
// one that can compute this for the vaults placed on it.
func (g *diskGuard) storageProtectedVaults() []glid.GLID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []glid.GLID
	for id, v := range g.vaults {
		for _, sid := range v.storageIDs {
			if s := g.storages[sid]; s != nil && s.protect.Load() {
				out = append(out, id)
				break
			}
		}
	}
	return out
}

// ageBoundCappedVaults lists the vaults whose max-age bound is currently
// refusing admission on this node. Broadcast in NodeStats so peers'
// admission gates honor it, same pattern as sizeCappedVaults.
func (g *diskGuard) ageBoundCappedVaults() []glid.GLID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []glid.GLID
	for id, v := range g.vaults {
		if v.ageBoundCapped.Load() {
			out = append(out, id)
		}
	}
	return out
}

// chunkCountBoundCappedVaults is ageBoundCappedVaults' max-chunks sibling.
func (g *diskGuard) chunkCountBoundCappedVaults() []glid.GLID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []glid.GLID
	for id, v := range g.vaults {
		if v.chunkCountBoundCapped.Load() {
			out = append(out, id)
		}
	}
	return out
}

// evaluateVaults runs the per-vault guard pass: the cap-and-refuse
// dimensions that stay per-vault (max-size, backlog). The free-space
// protect dimension moved to evaluateStorages (gastrolog-9akebz) — one
// statfs per storage, not one per vault sharing it. Caller is the
// scheduler job.
func (g *diskGuard) evaluateVaults(alerts alert.Sink) {
	g.mu.Lock()
	entries := maps.Clone(g.vaults)
	g.mu.Unlock()

	for id, v := range entries {
		if v.maxSizeBytes > 0 && g.vaultFootprint != nil {
			used := footprintBytes(g.vaultFootprint(id))
			g.reconcileVaultSizeCap(id, v, used)
			g.reconcileVaultSizeAlarm(alerts, id, v, used)
		} else {
			g.clearVaultSizeState(alerts, id, v)
		}
		if budget := g.backlogBudget.Load(); budget > 0 && g.vaultBacklogBytes != nil {
			used := footprintBytes(g.vaultBacklogBytes(id))
			g.reconcileVaultBacklogCap(id, v, used, budget)
			g.reconcileVaultBacklogAlarm(alerts, id, v, used, budget)
		} else {
			g.clearVaultBacklogState(alerts, id, v)
		}
	}
}

// evaluateStorages runs the per-storage guard pass: ONE statfs, ONE
// warn/floor verdict, ONE alarm pair per LOCALLY-hosted storage —
// regardless of how many vaults are placed on it (gastrolog-9akebz;
// previously this ran once per vault, duplicating the statfs and the alarm
// once per vault sharing a volume). Caller is the scheduler job.
func (g *diskGuard) evaluateStorages(alerts alert.Sink) {
	g.mu.Lock()
	entries := maps.Clone(g.storages)
	g.mu.Unlock()

	for id, s := range entries {
		free, total, ok := g.worstFreeOf([]string{s.path})
		if !ok {
			// No trustworthy sample this tick (unmounted, permissions): skip
			// reconciliation entirely and keep whatever protect/alarm verdict
			// was last derived from a successful sample — inherited behavior
			// from the node-level guard's own worstFree/reconcileProtect
			// (evaluate), unchanged by the move to per-storage evaluation.
			continue
		}
		warnAt := resolveThreshold(s.warnExpr, g.warnExpr, total)
		floorAt := resolveThreshold(s.floorExpr, g.floorExpr, total)
		s.lastFree.Store(free)
		s.lastFloor.Store(floorAt)
		g.reconcileStorageProtect(id, s, free, floorAt, warnAt)
		g.reconcileStorageAlarm(alerts, id, s, free, total, warnAt)
	}
}

// reconcileVaultBacklogCap flips the backlog cap: refuse admission at the
// budget, resume as soon as chunking drains the registry below it. Like the
// size cap it is enforced at ADMISSION only — already-accepted records keep
// draining through the pipeline (the backlog may overshoot modestly while
// in-flight segments complete; they count toward the measure so admission
// stays shut meanwhile). Pure state, no clocks: same registry ⇒ same verdict
// on every node, every evaluation.
func (g *diskGuard) reconcileVaultBacklogCap(id glid.GLID, v *vaultDiskGuard, used, budget uint64) {
	switch {
	case v.backlogCapped.Load() && used < budget:
		v.backlogCapped.Store(false)
		if g.logger != nil {
			g.logger.Info("vault backlog budget released — admission resumed for vault",
				"vault", id, "name", v.name,
				"backlog", units.FormatBytesDisplay(int64(used)), "budget", units.FormatBytesDisplay(int64(budget))) //nolint:gosec // display only
		}
	case !v.backlogCapped.Load() && used >= budget:
		v.backlogCapped.Store(true)
		if g.logger != nil {
			g.logger.Warn("vault backlog budget engaged — admission refused for vault until chunking drains the backlog",
				"vault", id, "name", v.name,
				"backlog", units.FormatBytesDisplay(int64(used)), "budget", units.FormatBytesDisplay(int64(budget))) //nolint:gosec // display only
		}
	}
}

func (g *diskGuard) reconcileVaultBacklogAlarm(alerts alert.Sink, id glid.GLID, v *vaultDiskGuard, used, budget uint64) {
	if alerts == nil {
		return
	}
	approachAt := sizeApproachThreshold(budget)
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case used >= approachAt:
		// Two setpoints, two alarms: approaching (Low) below the budget,
		// capped (High) at it. Exactly one is active at a time.
		if v.backlogCapped.Load() {
			alerts.Clear(alarmBacklogApproaching, id.String())
			alerts.Raise(alarmBacklogCapped, id.String(), fmt.Sprintf(
				"Vault %s pipeline backlog is at its budget: %s of %s — new records for this vault are REFUSED until chunking drains the backlog.",
				v.name, units.FormatBytesDisplay(int64(used)), units.FormatBytesDisplay(int64(budget)))) //nolint:gosec // display only
		} else {
			alerts.Clear(alarmBacklogCapped, id.String())
			alerts.Raise(alarmBacklogApproaching, id.String(), fmt.Sprintf(
				"Vault %s pipeline backlog is approaching its budget: %s of %s.",
				v.name, units.FormatBytesDisplay(int64(used)), units.FormatBytesDisplay(int64(budget)))) //nolint:gosec // display only
		}
		v.backlogAlarmRaised = true
	case v.backlogAlarmRaised && used < approachAt-approachAt/10:
		alerts.Clear(alarmBacklogApproaching, id.String())
		alerts.Clear(alarmBacklogCapped, id.String())
		v.backlogAlarmRaised = false
	}
}

// clearVaultBacklogState releases a standing cap/alarm when the budget is
// unset (0) — otherwise a vault capped under an old budget would refuse
// admission forever after the operator disables the bound.
func (g *diskGuard) clearVaultBacklogState(alerts alert.Sink, id glid.GLID, v *vaultDiskGuard) {
	if v.backlogCapped.Load() {
		v.backlogCapped.Store(false)
		if g.logger != nil {
			g.logger.Info("vault backlog budget disabled — admission resumed for vault", "vault", id, "name", v.name)
		}
	}
	g.mu.Lock()
	raised := v.backlogAlarmRaised
	v.backlogAlarmRaised = false
	g.mu.Unlock()
	if raised && alerts != nil {
		alerts.Clear(alarmBacklogApproaching, id.String())
		alerts.Clear(alarmBacklogCapped, id.String())
	}
}

// clearVaultSizeState releases a standing size cap/alarm when the bound is
// unset (0) — otherwise a vault capped under a since-removed or
// since-softened max-size bound would refuse admission forever.
// gastrolog-5yfaqj made 0 reachable for the first time: previously
// resolveVaultSizeBoundSource always resolved to either a stated bound or
// the default floor, never 0; now a vault whose only attached size
// policies are all refuse=false (soft bound — drains, never refuses)
// correctly resolves to 0 (no refuse bound, and the default floor must
// NOT re-engage over the operator's explicit opt-out). Mirrors
// clearVaultBacklogState.
func (g *diskGuard) clearVaultSizeState(alerts alert.Sink, id glid.GLID, v *vaultDiskGuard) {
	if v.capped.Load() {
		v.capped.Store(false)
		if g.logger != nil {
			g.logger.Info("vault max-size bound unset or soft-only — admission resumed for vault", "vault", id, "name", v.name)
		}
	}
	g.mu.Lock()
	raised := v.sizeAlarmRaised
	v.sizeAlarmRaised = false
	g.mu.Unlock()
	if raised && alerts != nil {
		alerts.Clear(alarmMaxSizeApproaching, id.String())
		alerts.Clear(alarmMaxSizeCapped, id.String())
	}
}

// footprintBytes clamps a measured footprint to unsigned; a negative
// measurement (unreachable store mid-teardown) reads as empty.
func footprintBytes(n int64) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// sizeApproachThreshold is where the max-size alarm raises: 90% of the
// budget. Clears with hysteresis one further tenth below, so chunk-granular
// retention drains don't flap it.
func sizeApproachThreshold(maxSize uint64) uint64 {
	return maxSize - maxSize/10
}

// reconcileVaultSizeCap flips the cap: refuse at the budget, resume as soon
// as retention or segment releases drain below it. The budget is enforced at
// ADMISSION only — already-accepted records keep draining through the
// pipeline into durable chunks (the claim may overshoot modestly; the
// backlog counts toward the footprint so admission stays shut meanwhile).
func (g *diskGuard) reconcileVaultSizeCap(id glid.GLID, v *vaultDiskGuard, used uint64) {
	switch {
	case v.capped.Load() && used < v.maxSizeBytes:
		v.capped.Store(false)
		if g.logger != nil {
			g.logger.Info("vault max-size cap released — admission resumed for vault",
				"vault", id, "name", v.name,
				"used", units.FormatBytesDisplay(int64(used)), "max", units.FormatBytesDisplay(int64(v.maxSizeBytes))) //nolint:gosec // display only
		}
	case !v.capped.Load() && used >= v.maxSizeBytes:
		v.capped.Store(true)
		if g.logger != nil {
			g.logger.Warn("vault max-size cap engaged — admission refused for vault until space drains",
				"vault", id, "name", v.name,
				"used", units.FormatBytesDisplay(int64(used)), "max", units.FormatBytesDisplay(int64(v.maxSizeBytes))) //nolint:gosec // display only
		}
	}
}

func (g *diskGuard) reconcileVaultSizeAlarm(alerts alert.Sink, id glid.GLID, v *vaultDiskGuard, used uint64) {
	if alerts == nil {
		return
	}
	approachAt := sizeApproachThreshold(v.maxSizeBytes)
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case used >= approachAt:
		// Two setpoints, two alarms: approaching (Low) below the budget,
		// capped (High) at it. Exactly one is active at a time.
		if v.capped.Load() {
			alerts.Clear(alarmMaxSizeApproaching, id.String())
			alerts.Raise(alarmMaxSizeCapped, id.String(), fmt.Sprintf(
				"Vault %s is at its max-size bound: %s of %s used — new records for this vault are REFUSED until space drains below the bound.",
				v.name, units.FormatBytesDisplay(int64(used)), units.FormatBytesDisplay(int64(v.maxSizeBytes)))) //nolint:gosec // display only
		} else {
			alerts.Clear(alarmMaxSizeCapped, id.String())
			alerts.Raise(alarmMaxSizeApproaching, id.String(), fmt.Sprintf(
				"Vault %s is approaching its max-size bound: %s of %s used.",
				v.name, units.FormatBytesDisplay(int64(used)), units.FormatBytesDisplay(int64(v.maxSizeBytes)))) //nolint:gosec // display only
		}
		v.sizeAlarmRaised = true
	case v.sizeAlarmRaised && used < approachAt-approachAt/10:
		alerts.Clear(alarmMaxSizeApproaching, id.String())
		alerts.Clear(alarmMaxSizeCapped, id.String())
		v.sizeAlarmRaised = false
	}
}

// reconcileStorageProtect gates every vault placed on this storage
// (gastrolog-9akebz — replaces reconcileVaultProtect, which duplicated this
// once per vault sharing a volume). Like the node admission gate it engages
// at the storage's floor and resumes only above its WARN band — same
// ratchet-avoidance headroom. There is no per-storage drain gate: builds
// and pulls are gated node-globally by deferWrites.
func (g *diskGuard) reconcileStorageProtect(id string, s *storageDiskGuard, free, floorAt, warnAt uint64) {
	resumeAt := admissionResumeAbove(floorAt, warnAt)
	switch {
	case s.protect.Load() && free > resumeAt:
		s.protect.Store(false)
		if g.logger != nil {
			g.logger.Info("storage disk protect released — admission resumed for every vault placed here",
				"storage", id, "name", s.name, "node", s.node, "free", fmtBytes(free), "resumeAbove", fmtBytes(resumeAt))
		}
	case !s.protect.Load() && free < floorAt:
		s.protect.Store(true)
		if g.logger != nil {
			g.logger.Warn("storage disk protect engaged — admission suspended for every vault placed here until free space clears its low-disk alarm band",
				"storage", id, "name", s.name, "node", s.node,
				"free", fmtBytes(free), "floor", fmtBytes(floorAt), "resumeAbove", fmtBytes(resumeAt))
		}
	}
}

// reconcileStorageAlarm is reconcileVaultAlarm's storage-keyed replacement:
// one alarm pair per storage, naming the storage and its node, instead of
// one per vault sharing it (gastrolog-9akebz).
func (g *diskGuard) reconcileStorageAlarm(alerts alert.Sink, id string, s *storageDiskGuard, free, total, warnAt uint64) {
	if alerts == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case free < warnAt:
		// Two setpoints, two alarms: low (Low) inside the warn band,
		// exhausted (High) once protect suspends admission.
		if s.protect.Load() {
			alerts.Clear(alarmStorageDiskLow, id)
			alerts.Raise(alarmStorageDiskExhausted, id, fmt.Sprintf(
				"Out of disk space on storage %s (node %s): %s free — admission for every vault placed here is SUSPENDED until space frees.",
				s.name, s.node, units.FormatBytesDisplay(int64(free)))) //nolint:gosec // display only
		} else {
			alerts.Clear(alarmStorageDiskExhausted, id)
			alerts.Raise(alarmStorageDiskLow, id, fmt.Sprintf(
				"Low disk space on storage %s (node %s): %s free of %s.",
				s.name, s.node, units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total)))) //nolint:gosec // display only
		}
		s.alarmRaised = true
	case s.alarmRaised && free > clearAbove(warnAt):
		alerts.Clear(alarmStorageDiskLow, id)
		alerts.Clear(alarmStorageDiskExhausted, id)
		s.alarmRaised = false
	}
}

// worstFreeOf is worstFree over an explicit path set.
func (g *diskGuard) worstFreeOf(paths []string) (free, total uint64, ok bool) {
	first := true
	for _, p := range paths {
		f, t, err := g.sample(p)
		if err != nil || t == 0 {
			continue
		}
		if first || f < free {
			free, total = f, t
			first = false
		}
	}
	return free, total, !first
}

// evaluate runs one guard pass: updates protect mode and raises/clears the
// disk-space alarm on the given collector. Scheduler-driven.
func (g *diskGuard) evaluate(alerts alert.Sink) {
	free, total, ok := g.worstFree()
	if !ok {
		return // no sampleable path; nothing trustworthy to act on
	}
	warnAt := g.warnThreshold(total)
	floorAt := g.floorThreshold(total)

	g.reconcileProtect(free, floorAt, warnAt)

	if alerts == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case free < warnAt:
		// Two setpoints, two alarms: low (Low) inside the warn band,
		// exhausted (High) once protect suspends ingest admission.
		if g.protect.Load() {
			alerts.Clear(alarmNodeDiskLow, "")
			alerts.Raise(alarmNodeDiskExhausted, "", fmt.Sprintf(
				"Out of disk space: %s free of %s — ingest admission is SUSPENDED on this node until space is freed.",
				units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total)))) //nolint:gosec // display only
		} else {
			alerts.Clear(alarmNodeDiskExhausted, "")
			alerts.Raise(alarmNodeDiskLow, "", fmt.Sprintf(
				"Low disk space: %s free of %s.",
				units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total)))) //nolint:gosec // display only
		}
		g.alarmRaised = true
	case g.alarmRaised && free > clearAbove(warnAt):
		alerts.Clear(alarmNodeDiskLow, "")
		alerts.Clear(alarmNodeDiskExhausted, "")
		g.alarmRaised = false
	}
}

// reconcileProtect drives the two staged gates. Both gates engage
// together at the floor; on recovery the drain gate releases first (so the
// pipeline seals backlog and retention frees space) and the admission gate
// releases last, only above the WARN band. Transitions log — admission nacks
// are silent in the pump, so without these lines the guard's staging is
// invisible in the log record.
func (g *diskGuard) reconcileProtect(free, floorAt, warnAt uint64) {
	// Drain gate: pause at floor, resume just above it.
	switch {
	case g.deferWrites.Load() && free > clearAbove(floorAt):
		g.deferWrites.Store(false)
		if g.logger != nil {
			g.logger.Info("disk protect: pipeline builds and pulls resumed", "free", fmtBytes(free))
		}
	case !g.deferWrites.Load() && free < floorAt:
		g.deferWrites.Store(true)
		if g.logger != nil {
			g.logger.Warn("disk protect: pipeline builds and pulls paused below the floor",
				"free", fmtBytes(free), "floor", fmtBytes(floorAt))
		}
	}
	// Admission gate: pause at floor, resume only above the WARN band.
	resumeAt := admissionResumeAbove(floorAt, warnAt)
	switch {
	case g.protect.Load() && free > resumeAt:
		g.protect.Store(false)
		if g.logger != nil {
			g.logger.Info("disk protect released — ingest admission resumed",
				"free", fmtBytes(free), "resumeAbove", fmtBytes(resumeAt))
		}
	case !g.protect.Load() && free < floorAt:
		g.protect.Store(true)
		if g.logger != nil {
			g.logger.Warn("disk protect engaged — ingest admission suspended until free space clears the low-disk alarm band",
				"free", fmtBytes(free), "floor", fmtBytes(floorAt), "resumeAbove", fmtBytes(resumeAt))
		}
	}
}

// diskProtectActive reports whether the node is refusing new work for lack
// of disk space (the admission gate). Consulted by ingest admission, the
// catch-up puller, and retention re-routing — all net consumers that resume
// only above the WARN band.
func (o *Orchestrator) diskProtectActive() bool {
	return o.diskGuard != nil && o.diskGuard.protect.Load()
}

// diskDeferWrites reports whether the pipeline drain gate (chunking builds and
// collection pulls) is paused for lack of disk space. Resumes earlier than
// admission so the pipeline can seal backlog into expungeable chunks.
func (o *Orchestrator) diskDeferWrites() bool {
	return o.diskGuard != nil && o.diskGuard.deferWrites.Load()
}

// diskAdmissionGate is the supervisor's admission check: reject new records
// while the node is below its free-space floor.
func (o *Orchestrator) diskAdmissionGate() error {
	if o.diskProtectActive() {
		return ErrDiskProtect
	}
	return nil
}

// ErrStorageDiskProtect rejects records destined to a vault placed on a
// storage that is below its free-space floor. Scoped: other vaults on
// healthy storages keep ingesting. Renamed from ErrVaultDiskProtect
// (gastrolog-9akebz): the thresholds moved from VaultConfig to the storage
// entity a vault's placements reference — every vault sharing a below-floor
// storage refuses for the same reason.
var ErrStorageDiskProtect = errors.New("vault's storage is out of disk space: admission for this vault suspended until space is freed")

// ErrVaultMaxSize rejects records destined to a vault whose local disk claim
// has reached its per-node max-size bound on some node. Cap-and-refuse:
// everything already accepted is kept; the newest records are nacked as
// retryable backpressure until space drains below the bound. Not always
// "until retention drains it" — the bound may be the refuse-only creation-
// default floor with no attached policy to drain anything at all; space
// frees only via releases (deletes, transfers, cache eviction) in that case.
var ErrVaultMaxSize = errors.New("vault is at its max-size bound: admission for this vault refused until space drains below the bound")

// ErrVaultBacklogBudget rejects records destined to a vault whose pipeline
// backlog (unreleased registry segments) has reached the cluster-global
// budget. Retryable backpressure: chunking keeps draining, admission resumes
// below the budget. The operating bound that engages before disk pressure.
var ErrVaultBacklogBudget = errors.New("vault's pipeline backlog is at its budget: admission refused until chunking drains it")

// ErrVaultAgeBound rejects records destined to a vault whose max-age
// retention bound is still violated after a retention sweep attempted to
// clear it (gastrolog-5yfaqj), on a policy with refuse=true. Retryable
// backpressure: retention keeps trying every sweep, admission resumes the
// moment a sweep's post-check finds the bound clear.
var ErrVaultAgeBound = errors.New("vault's max-age retention bound is still violated after a sweep: admission refused until retention clears it")

// ErrVaultChunkCountBound is ErrVaultAgeBound's max-chunks sibling.
var ErrVaultChunkCountBound = errors.New("vault's max-chunks retention bound is still violated after a sweep: admission refused until retention clears it")

// VaultAdmissionCause identifies one reason a vault's admission gate is
// currently refusing new records for it. Defined here rather than imported
// from the generated proto package so orchestrator stays proto-free; the
// server package maps these to apiv1.VaultAdmissionCause for the
// VaultInfo.AdmissionRefused RPC field. Order of these constants carries no
// meaning — gate-check order lives in vaultAdmissionCauses, not here.
type VaultAdmissionCause int

const (
	// VaultAdmissionCauseStorageDiskProtect renamed from
	// VaultAdmissionCauseVaultDiskProtect (gastrolog-9akebz): the disk-free
	// thresholds moved from VaultConfig to the storage entity a vault's
	// placements reference — this cause fires for every vault placed on a
	// below-floor storage, not a per-vault threshold.
	VaultAdmissionCauseStorageDiskProtect VaultAdmissionCause = iota + 1
	VaultAdmissionCauseMaxSizeBound
	VaultAdmissionCauseBacklogBudget
	// VaultAdmissionCauseAgeBound and VaultAdmissionCauseChunkCountBound
	// (gastrolog-5yfaqj) generalize refusal from max-size to every
	// retention policy bound: they apply only when the stating policy has
	// refuse=true AND the retention runner has swept and failed to clear
	// the violation — never on the transient between a chunk's seal and
	// the next sweep. See disk_guard's ageBoundCapped/chunkCountBoundCapped.
	VaultAdmissionCauseAgeBound
	VaultAdmissionCauseChunkCountBound
)

// vaultAdmissionCauseEntry pairs a cause with the sentinel error
// vaultAdmissionGate returns for it, so the gate can consume
// vaultAdmissionCauses' output directly instead of re-deriving anything.
// Detail is the backend's own terse specifics for this cause (gastrolog-9akebz)
// — which storage and its free-vs-floor numbers, or the bound kind and
// value — never a client-side reconstruction.
type vaultAdmissionCauseEntry struct {
	Cause  VaultAdmissionCause
	Err    error
	Detail string
}

// vaultAdmissionCauses is the SOLE source of truth for per-vault admission
// causes: it collects every currently-applicable one, in gate-check order —
// storage disk protect (local OR any live peer's broadcast), max-size bound
// (local OR any live peer's broadcast), backlog budget (local only: the
// vault-ctl FSM registry is replicated to every node, so the local guard's
// verdict already IS the cluster verdict). Empty when the vault admits
// normally. vaultAdmissionGate takes causes[0]; the exported
// VaultAdmissionCauses wrapper reports ALL of them for the VaultInfo RPC
// field — both consume this one collector, so the gate and the UI-facing
// signal can never drift apart.
func (o *Orchestrator) vaultAdmissionCauses(vaultID glid.GLID) []vaultAdmissionCauseEntry {
	var causes []vaultAdmissionCauseEntry
	for _, check := range []func(glid.GLID) (vaultAdmissionCauseEntry, bool){
		o.storageDiskProtectCause,
		o.maxSizeBoundCause,
		o.backlogBudgetCause,
		o.ageBoundCause,
		o.chunkCountBoundCause,
	} {
		if entry, ok := check(vaultID); ok {
			causes = append(causes, entry)
		}
	}
	return causes
}

// storageDiskProtectCause checks the first gate-check-order cause
// (gastrolog-9akebz): local first (this node can name the storage and its
// free-vs-floor numbers honestly), else the peer broadcast (the reporting
// node's identity is the detail — this node has no local sample to attach
// numbers to).
func (o *Orchestrator) storageDiskProtectCause(vaultID glid.GLID) (vaultAdmissionCauseEntry, bool) {
	if o.diskGuard != nil && o.diskGuard.vaultStorageProtected(vaultID) {
		return vaultAdmissionCauseEntry{VaultAdmissionCauseStorageDiskProtect, ErrStorageDiskProtect, o.diskGuard.vaultStorageProtectDetail(vaultID)}, true
	}
	fn := o.remoteVaultStorageProtected.Load()
	if fn == nil || !(*fn)(vaultID) {
		return vaultAdmissionCauseEntry{}, false
	}
	detail := ""
	if nfn := o.remoteVaultStorageProtectedNodes.Load(); nfn != nil {
		if nodes := (*nfn)(vaultID); len(nodes) > 0 {
			detail = "reported by " + strings.Join(nodes, ", ")
		}
	}
	return vaultAdmissionCauseEntry{VaultAdmissionCauseStorageDiskProtect, ErrStorageDiskProtect, detail}, true
}

func (o *Orchestrator) maxSizeBoundCause(vaultID glid.GLID) (vaultAdmissionCauseEntry, bool) {
	sizeCapped := o.diskGuard != nil && o.diskGuard.vaultSizeCapped(vaultID)
	if !sizeCapped {
		if fn := o.remoteVaultSizeCapped.Load(); fn != nil && (*fn)(vaultID) {
			sizeCapped = true
		}
	}
	if !sizeCapped {
		return vaultAdmissionCauseEntry{}, false
	}
	detail := ""
	if o.diskGuard != nil {
		detail = o.diskGuard.vaultMaxSizeBoundDetail(vaultID)
	}
	return vaultAdmissionCauseEntry{VaultAdmissionCauseMaxSizeBound, ErrVaultMaxSize, detail}, true
}

// backlogBudgetCause needs no peer lookup: the measure is the vault-ctl FSM
// registry, replicated to every node, so the local guard's verdict is the
// cluster's verdict.
func (o *Orchestrator) backlogBudgetCause(vaultID glid.GLID) (vaultAdmissionCauseEntry, bool) {
	if o.diskGuard == nil || !o.diskGuard.vaultBacklogCapped(vaultID) {
		return vaultAdmissionCauseEntry{}, false
	}
	return vaultAdmissionCauseEntry{VaultAdmissionCauseBacklogBudget, ErrVaultBacklogBudget, o.diskGuard.backlogBudgetDetail()}, true
}

// ageBoundCause / chunkCountBoundCause (gastrolog-5yfaqj): only the
// retention leader for a vault instance runs the sweep that derives these,
// so — like storage disk protect and max-size — a peer that only hosts the
// front door for this vault must consult the NodeStats broadcast too. The
// bound LABEL, unlike the violated flag, is a pure function of replicated
// config (attachedAgeBound/attachedChunkCountBound at refresh time) — every
// node resolves the same label regardless of which one detected the
// violation, so no remote plumbing is needed for the detail text itself.
func (o *Orchestrator) ageBoundCause(vaultID glid.GLID) (vaultAdmissionCauseEntry, bool) {
	ageBound := o.diskGuard != nil && o.diskGuard.vaultAgeBoundCapped(vaultID)
	if !ageBound {
		if fn := o.remoteVaultAgeBoundCapped.Load(); fn != nil && (*fn)(vaultID) {
			ageBound = true
		}
	}
	if !ageBound {
		return vaultAdmissionCauseEntry{}, false
	}
	detail := "max-age bound still violated after a sweep"
	if o.diskGuard != nil {
		if d := o.diskGuard.vaultAgeBoundDetail(vaultID); d != "" {
			detail = d
		}
	}
	return vaultAdmissionCauseEntry{VaultAdmissionCauseAgeBound, ErrVaultAgeBound, detail}, true
}

// chunkCountBoundCause is ageBoundCause's max-chunks sibling.
func (o *Orchestrator) chunkCountBoundCause(vaultID glid.GLID) (vaultAdmissionCauseEntry, bool) {
	chunkCountBound := o.diskGuard != nil && o.diskGuard.vaultChunkCountBoundCapped(vaultID)
	if !chunkCountBound {
		if fn := o.remoteVaultChunkCountBoundCapped.Load(); fn != nil && (*fn)(vaultID) {
			chunkCountBound = true
		}
	}
	if !chunkCountBound {
		return vaultAdmissionCauseEntry{}, false
	}
	detail := "max-chunks bound still violated after a sweep"
	if o.diskGuard != nil {
		if d := o.diskGuard.vaultChunkCountBoundDetail(vaultID); d != "" {
			detail = d
		}
	}
	return vaultAdmissionCauseEntry{VaultAdmissionCauseChunkCountBound, ErrVaultChunkCountBound, detail}, true
}

// vaultAdmissionGate is the per-destination admission check: the first
// currently-applicable cause wins. It honors both the local guard and — via
// the NodeStats broadcast — every live peer's: the starved volume or
// over-budget claim backing a vault is usually on a different node than the
// front door accepting records for it. The gate consumes
// vaultAdmissionCauses so its behavior cannot drift from what that collector
// (and therefore the VaultInfo.AdmissionRefused RPC field) reports.
func (o *Orchestrator) vaultAdmissionGate(vaultID glid.GLID) error {
	causes := o.vaultAdmissionCauses(vaultID)
	if len(causes) == 0 {
		return nil
	}
	return causes[0].Err
}

// VaultAdmissionCauses reports every currently-applicable admission-refusal
// cause for vaultID (empty when the vault admits normally). Exported for the
// VaultInfo.AdmissionRefused RPC field (server package): the responding
// node's own view (local guard + its live-peer broadcasts) is the same
// cluster-aware input vaultAdmissionGate consults, so the UI's "refusing"
// signal is never a client-side derivation from other state.
func (o *Orchestrator) VaultAdmissionCauses(vaultID glid.GLID) []VaultAdmissionCause {
	entries := o.vaultAdmissionCauses(vaultID)
	out := make([]VaultAdmissionCause, len(entries))
	for i, e := range entries {
		out[i] = e.Cause
	}
	return out
}

// VaultAdmissionCauseDetail pairs a cause with its detail text — the
// specifics the backend knows at response time (which storage and its
// free-vs-floor numbers, which bound and its value), never a client-side
// reconstruction (gastrolog-9akebz).
type VaultAdmissionCauseDetail struct {
	Cause  VaultAdmissionCause
	Detail string
}

// VaultAdmissionCauseDetails is VaultAdmissionCauses plus each cause's
// detail text, for the VaultInfo.AdmissionRefused RPC field's per-cause
// message. Same underlying collector as VaultAdmissionCauses and
// vaultAdmissionGate, so the signal can never drift.
func (o *Orchestrator) VaultAdmissionCauseDetails(vaultID glid.GLID) []VaultAdmissionCauseDetail {
	entries := o.vaultAdmissionCauses(vaultID)
	out := make([]VaultAdmissionCauseDetail, len(entries))
	for i, e := range entries {
		out[i] = VaultAdmissionCauseDetail{Cause: e.Cause, Detail: e.Detail}
	}
	return out
}

// SizeCappedVaults lists vaults at their local max-size bound, for the
// NodeStats broadcast.
func (o *Orchestrator) SizeCappedVaults() []glid.GLID {
	if o.diskGuard == nil {
		return nil
	}
	return o.diskGuard.sizeCappedVaults()
}

// SetRemoteVaultSizeCapped installs the peer-state lookup for vaults capped
// on other nodes. Set once at app wiring, after cluster stats exist.
func (o *Orchestrator) SetRemoteVaultSizeCapped(fn func(glid.GLID) bool) {
	o.remoteVaultSizeCapped.Store(&fn)
}

// StorageProtectedVaults lists vaults with a placement on a LOCALLY-hosted
// storage under disk protect, for the NodeStats broadcast. Renamed from
// DiskProtectedVaults (gastrolog-9akebz).
func (o *Orchestrator) StorageProtectedVaults() []glid.GLID {
	if o.diskGuard == nil {
		return nil
	}
	return o.diskGuard.storageProtectedVaults()
}

// SetRemoteVaultStorageProtected installs the peer-state lookup consulted by
// vaultAdmissionGate. Set once at app wiring, after cluster stats exist.
// Renamed from SetRemoteVaultDiskProtected (gastrolog-9akebz).
func (o *Orchestrator) SetRemoteVaultStorageProtected(fn func(glid.GLID) bool) {
	o.remoteVaultStorageProtected.Store(&fn)
}

// SetRemoteVaultStorageProtectedNodes installs the peer-state lookup for
// WHICH nodes report a vault's storage under disk protect, already
// resolved to operator-facing NAMES (falling back to the raw ID per-peer
// when a name isn't known) and sorted for a stable join — the admission
// detail signal's "reported by <name>" text. Production wiring is
// PeerState.VaultStorageProtectedNodeNames, NOT the ID-returning
// VaultStorageProtectedNodes the placement manager uses for set-membership
// matching — do not swap them. Set once at app wiring.
func (o *Orchestrator) SetRemoteVaultStorageProtectedNodes(fn func(glid.GLID) []string) {
	o.remoteVaultStorageProtectedNodes.Store(&fn)
}

// AgeBoundCappedVaults lists vaults whose max-age retention bound is
// currently refusing admission on this node, for the NodeStats broadcast.
func (o *Orchestrator) AgeBoundCappedVaults() []glid.GLID {
	if o.diskGuard == nil {
		return nil
	}
	return o.diskGuard.ageBoundCappedVaults()
}

// SetRemoteVaultAgeBoundCapped installs the peer-state lookup for vaults
// age-bound-capped on other nodes. Set once at app wiring, after cluster
// stats exist.
func (o *Orchestrator) SetRemoteVaultAgeBoundCapped(fn func(glid.GLID) bool) {
	o.remoteVaultAgeBoundCapped.Store(&fn)
}

// ChunkCountBoundCappedVaults is AgeBoundCappedVaults' max-chunks sibling.
func (o *Orchestrator) ChunkCountBoundCappedVaults() []glid.GLID {
	if o.diskGuard == nil {
		return nil
	}
	return o.diskGuard.chunkCountBoundCappedVaults()
}

// SetRemoteVaultChunkCountBoundCapped is SetRemoteVaultAgeBoundCapped's
// max-chunks sibling.
func (o *Orchestrator) SetRemoteVaultChunkCountBoundCapped(fn func(glid.GLID) bool) {
	o.remoteVaultChunkCountBoundCapped.Store(&fn)
}

// SetVaultAgeBoundCapped records the retention runner's post-sweep verdict
// for a vault's max-age bound (gastrolog-5yfaqj) — the seam retention.go's
// sweep() calls after every sweep to report whether the bound is still
// violated. No-ops if the guard has no entry for this vault (a memory
// vault, or a file vault whose guard entry hasn't been registered yet by
// refreshVaultDiskGuards — the next sweep after registration retries).
func (o *Orchestrator) SetVaultAgeBoundCapped(id glid.GLID, capped bool) {
	if o.diskGuard == nil {
		return
	}
	o.diskGuard.setVaultAgeBoundCapped(o.alerts, id, capped)
}

// SetVaultChunkCountBoundCapped is SetVaultAgeBoundCapped's max-chunks
// sibling.
func (o *Orchestrator) SetVaultChunkCountBoundCapped(id glid.GLID, capped bool) {
	if o.diskGuard == nil {
		return
	}
	o.diskGuard.setVaultChunkCountBoundCapped(o.alerts, id, capped)
}

// nodeDisplayName resolves a node ID to its operator-facing name from the
// live NodeConfig list, falling back to the raw ID when unknown — the same
// contract as placementManager.nameOrID (backend/internal/app/placement.go),
// reimplemented here (rather than shared) because it operates on
// []system.NodeConfig directly for a single lookup per refresh tick, not a
// pre-built batch map.
func nodeDisplayName(nodes []system.NodeConfig, nodeID string) string {
	for _, n := range nodes {
		if n.ID.String() == nodeID && n.Name != "" {
			return n.Name
		}
	}
	return nodeID
}

// refreshVaultDiskGuards converges the guard's per-storage AND per-vault
// entries with the current config (gastrolog-9akebz): every LOCALLY-hosted
// storage gets ONE guard entry (the free-space thresholds now live there —
// system.FileStorage.DiskFreeWarn/DiskFreeFloor), and every file vault gets
// a guard entry recording which of those storage IDs its LOCAL placements
// reference plus the OTHER per-vault cap-and-refuse dimensions (max-size,
// age/count bound labels). Discovery-based like the rotation and retention
// sweeps — storage/vault add/update/remove and placement changes are all
// picked up on the next tick with no per-callsite lifecycle wiring.
func (o *Orchestrator) refreshVaultDiskGuards(ctx context.Context) {
	if o.diskGuard == nil {
		return
	}
	sys, err := o.loadSystem(ctx)
	if err != nil || sys == nil {
		return
	}
	rt := &sys.Runtime

	// One entry per LOCALLY-hosted storage: the guard samples each
	// storage's volume ONCE regardless of how many vaults share it. "Local"
	// means this node's own NodeStorageConfig — the only volumes it can
	// statfs. The node is resolved to its operator-facing name HERE, once
	// per refresh (config is already loaded, off the admission hot path) —
	// same fallback contract as placementManager.nameOrID: name when known,
	// the raw ID otherwise. Every alarm/detail string that names this
	// storage's node reads it pre-resolved.
	nodeName := nodeDisplayName(rt.Nodes, o.localNodeID)
	keepStorages := make(map[string]bool)
	for _, nsc := range rt.NodeStorageConfigs {
		if nsc.NodeID != o.localNodeID {
			continue
		}
		for _, fs := range nsc.FileStorages {
			sid := fs.ID.String()
			keepStorages[sid] = true
			o.diskGuard.SetStorageGuard(sid, fs.Name, nodeName, fs.Path, fs.DiskFreeWarn, fs.DiskFreeFloor)
		}
	}
	o.diskGuard.retainStorageGuards(keepStorages, o.alerts)

	keepVaults := make(map[glid.GLID]bool)
	for _, vc := range sys.Config.Vaults {
		if vc.Type != system.VaultTypeFile {
			continue
		}
		var storageIDs []string
		for _, p := range vc.Placements {
			if system.NodeIDForStorage(p.StorageID, rt.NodeStorageConfigs) != o.localNodeID {
				continue
			}
			if findFileStorageByID(rt, p.StorageID) != nil {
				storageIDs = append(storageIDs, p.StorageID)
			}
		}
		// Config→runtime boundary: the operator's expressions become numbers
		// here, once, through the shared resolver (gastrolog-etcjdx). The
		// bound itself now resolves from the vault's attached retention
		// policies rather than a vault-level field (gastrolog-33ul6h); see
		// resolveVaultSizeBoundSource for the min-wins / default-floor rule.
		maxSize, boundSource := resolveVaultSizeBoundSource(vc, sys.Config.RetentionPolicies)
		// A vault with no local placement still claims local disk through its
		// origin segment backlog, so a max-size bound registers it even when
		// there is no storage to sample here.
		if len(storageIDs) == 0 && maxSize == 0 {
			continue
		}
		// Bound transitions are otherwise silent: a size cap tightening or
		// loosening changes admission behavior with no operator-visible
		// signal until a cap/uncap actually fires. Log the CHANGE only — not
		// every tick's re-resolution, which would flood the log with the
		// steady-state case (gastrolog-33ul6h finding 4). First observation
		// (no prior entry) is not a transition, so it stays quiet too.
		if prev, existed := o.diskGuard.currentMaxSizeBytes(vc.ID); existed && prev != maxSize && o.diskGuard.logger != nil {
			o.diskGuard.logger.Info("vault max-size bound changed",
				"vault", vc.ID, "name", vc.Name,
				"old", fmtBytes(prev), "new", fmtBytes(maxSize),
				"source", boundSource)
		}
		keepVaults[vc.ID] = true
		ageLabel := attachedAgeBound(vc, sys.Config.RetentionPolicies)
		countLabel := attachedChunkCountBound(vc, sys.Config.RetentionPolicies)
		o.diskGuard.SetVaultGuard(vc.ID, vc.Name, storageIDs, maxSize, ageLabel, countLabel)
	}
	o.diskGuard.retainVaultGuards(keepVaults, o.alerts)
}

// attachedSizeBound scans the vault's RetentionRules for the tightest
// (minimum) usable MaxSize among REFUSE-ELIGIBLE referenced policies
// (gastrolog-5yfaqj: policy.RefuseEnabled() — a policy stating MaxSize
// with refuse=false still drains via its own SizeRetentionPolicy rule
// elsewhere, but contributes nothing to the instantaneous refuse bound
// this function resolves). A referenced policy that carries no MaxSize
// (nil, unset, or unparseable — defense in depth; PutRetentionPolicy
// validates parseability at write, so a parse failure here can only be a
// pre-change or bug-produced config) contributes nothing to the min
// either. Returns the winning policy (nil if none carries a usable
// refuse-eligible bound) so callers can build both the numeric bound and
// an operator-readable source label without re-scanning, plus anyStated:
// whether ANY attached policy — refuse-eligible or not — states a usable
// MaxSize at all, which the caller needs to decide whether the
// refuse-only default floor may apply (see resolveVaultSizeBoundSource).
func attachedSizeBound(vc system.VaultConfig, policies []system.RetentionPolicyConfig) (minBound uint64, winner *system.RetentionPolicyConfig, anyStated bool) {
	for _, rule := range vc.RetentionRules {
		policy := findRetentionPolicy(policies, rule.RetentionPolicyID)
		if policy == nil || policy.MaxSize == nil || system.IsQuantityUnset(*policy.MaxSize) {
			continue
		}
		size, err := system.ParseSize(*policy.MaxSize)
		if err != nil || size == 0 {
			continue
		}
		anyStated = true
		if !policy.RefuseEnabled() {
			continue // soft bound: drains (its own rule), never refuses
		}
		if winner == nil || size < minBound {
			minBound = size
			winner = policy
		}
	}
	return minBound, winner, anyStated
}

// attachedAgeBound scans the vault's RetentionRules for the tightest
// (minimum) usable MaxAge among refuse-eligible referenced policies — same
// min-wins/refuse-eligibility contract as attachedSizeBound, resolved at
// the same discovery tick and cached on vaultDiskGuard.ageBoundLabel for
// the admission detail signal (gastrolog-9akebz). Returns the operator's
// own duration expression verbatim ("3d"), not a reformatted value — the
// label is display text, not a quantity anything resolves again. Empty
// when no refuse-eligible policy states a usable MaxAge.
func attachedAgeBound(vc system.VaultConfig, policies []system.RetentionPolicyConfig) string {
	var winner string
	var minAge time.Duration
	for _, rule := range vc.RetentionRules {
		policy := findRetentionPolicy(policies, rule.RetentionPolicyID)
		if policy == nil || !policy.RefuseEnabled() || policy.MaxAge == nil || system.IsQuantityUnset(*policy.MaxAge) {
			continue
		}
		age, err := system.ParseDuration(*policy.MaxAge)
		if err != nil || age <= 0 {
			continue
		}
		if winner == "" || age < minAge {
			minAge = age
			winner = *policy.MaxAge
		}
	}
	return winner
}

// attachedChunkCountBound is attachedAgeBound's max-chunks sibling.
func attachedChunkCountBound(vc system.VaultConfig, policies []system.RetentionPolicyConfig) string {
	var winner string
	var minCount int64
	for _, rule := range vc.RetentionRules {
		policy := findRetentionPolicy(policies, rule.RetentionPolicyID)
		if policy == nil || !policy.RefuseEnabled() || policy.MaxChunks == nil || *policy.MaxChunks <= 0 {
			continue
		}
		count := *policy.MaxChunks
		if winner == "" || count < minCount {
			minCount = count
			winner = strconv.FormatInt(count, 10)
		}
	}
	return winner
}

// resolveVaultSizeBound computes the effective per-node disk-claim REFUSE
// bound for a file vault (gastrolog-33ul6h): attachedSizeBound's min-wins
// result over refuse-eligible policies, or the creation default
// (system.DefaultVaultMaxSize) as the refuse-only floor when NO attached
// policy states a usable bound at all — zero retention rules, zero
// policies, or only bound-less policies — so a file vault stays bounded
// with no operator diligence required. gastrolog-5yfaqj corner case: when
// at least one attached policy DOES state a usable bound but every one of
// them is refuse=false (soft), the result is 0 — no refuse bound applies,
// and the default floor does NOT re-engage either, because the operator
// explicitly opted every stating policy out of refusal; silently falling
// back to the floor would override that choice.
func resolveVaultSizeBound(vc system.VaultConfig, policies []system.RetentionPolicyConfig) uint64 {
	bound, _ := resolveVaultSizeBoundSource(vc, policies)
	return bound
}

// resolveVaultSizeBoundSource is resolveVaultSizeBound plus an
// operator-readable source label ("policy <name/id>", "default floor", or
// the soft-only corner case), for the bound-transition log
// (gastrolog-33ul6h finding 4): naming WHERE an effective bound change
// came from, not just the old/new numbers.
func resolveVaultSizeBoundSource(vc system.VaultConfig, policies []system.RetentionPolicyConfig) (uint64, string) {
	bound, winner, anyStated := attachedSizeBound(vc, policies)
	if winner != nil {
		return bound, "policy " + retentionPolicyLabel(*winner)
	}
	if anyStated {
		return 0, "no refuse-eligible policy (soft bound only)"
	}
	def, _ := system.ParseSize(system.DefaultVaultMaxSize)
	return def, "default floor"
}

// currentMaxSizeBytes returns the vault's currently-registered max-size
// bound and whether an entry exists yet. Used by refreshVaultDiskGuards to
// detect an effective-bound CHANGE (vs. first observation) before calling
// SetVaultGuard, which overwrites the value unconditionally.
func (g *diskGuard) currentMaxSizeBytes(vaultID glid.GLID) (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.vaults[vaultID]
	if !ok {
		return 0, false
	}
	return v.maxSizeBytes, true
}

// primeDiskGuard runs one synchronous NODE-level guard pass before ingest
// admission opens, closing the boot blind window: without it, a node
// restarting into an already-full volume admits and writes for up to one
// sampling interval (15s) with protect=false — exactly how the second wave of
// nodes died after restarting into the full volume (gastrolog-67gvjo). Only
// the node-level evaluate runs here (statfs + guard mutex, no o.mu); the
// per-vault pass touches o.mu via the footprint probe and would deadlock under
// Start's lock, so it converges on the first scheduler tick instead — the
// node-level floor is the hard backstop that matters for the WAL.
func (o *Orchestrator) primeDiskGuard() {
	if o.diskGuard == nil || len(o.diskGuard.paths) == 0 {
		return
	}
	o.diskGuard.evaluate(o.alerts)
}

// serverSettingsLoader is the optional capability the guard job uses to pick
// up the cluster-global backlog budget. The system store implements it; test
// fakes that only implement SystemLoader simply leave the budget at 0
// (disabled). Same discovery-based shape as refreshVaultDiskGuards — a
// settings change is honored on the next tick, no mutation-path wiring.
type serverSettingsLoader interface {
	LoadServerSettings(ctx context.Context) (system.ServerSettings, error)
}

// refreshBacklogBudget converges the guard's cluster-global backlog budget
// with the stored cluster settings.
func (o *Orchestrator) refreshBacklogBudget(ctx context.Context) {
	if o.diskGuard == nil {
		return
	}
	loader, ok := o.sysLoader.(serverSettingsLoader)
	if !ok {
		return
	}
	ss, err := loader.LoadServerSettings(ctx)
	if err != nil {
		return
	}
	// Resolve the backlog budget expression at use, via the shared parser
	// (gastrolog-etcjdx). Empty = unbounded (0); a malformed value was rejected
	// at write, so treat a parse failure as unbounded rather than guess.
	budget, err := system.SizeOrDefault(ss.Cluster.PipelineBacklogMax, 0)
	if err != nil {
		budget = 0
	}
	o.diskGuard.backlogBudget.Store(budget)
}

// startDiskGuard registers the guard's scheduler job. No-op without paths.
func (o *Orchestrator) startDiskGuard() error {
	if o.diskGuard == nil || len(o.diskGuard.paths) == 0 {
		return nil
	}
	if err := o.scheduler.AddJob(diskGuardJobName, diskGuardSchedule, o.diskGuardTick); err != nil {
		return fmt.Errorf("disk guard: %w", err)
	}
	o.scheduler.Describe(diskGuardJobName,
		"Free-space guard: raises the disk-space alarm and suspends ingest admission below the floor or when a vault's backlog budget or max-size bound is reached")
	return nil
}

// diskGuardTick is the disk guard's scheduler job body — a named method
// (not an inline closure) so a test can call the EXACT function object the
// scheduler holds directly, rather than re-implementing its steps and
// silently drifting from what actually runs in production. This is the
// fix for gastrolog-9akebz's review finding: evaluateStorages had no
// production call site (the closure only ran evaluate ->
// refreshVaultDiskGuards -> refreshBacklogBudget -> evaluateVaults), so no
// storage ever engaged protect/alarmed/broadcast on a live node — a
// regression from the per-vault free-space pass evaluateVaults used to run
// before the storage move.
func (o *Orchestrator) diskGuardTick() {
	o.diskGuard.evaluate(o.alerts)
	o.refreshVaultDiskGuards(context.Background())
	o.refreshBacklogBudget(context.Background())
	o.diskGuard.evaluateStorages(o.alerts)
	o.diskGuard.evaluateVaults(o.alerts)
}
