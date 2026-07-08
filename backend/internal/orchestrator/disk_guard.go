package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

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

	diskGuardAlertID = "disk-space"

	// Fractions of the volume, paired with absolute byte minimums. The
	// larger of the two governs — but the absolute minimums are CLAMPED
	// to a share of the volume so a small (or quota-capped) volume isn't
	// permanently in alarm: a 10GB test volume must not carry a 10GiB
	// warn threshold.
	diskFreeWarnFraction  = 0.10
	diskFreeFloorFraction = 0.03
	diskFreeWarnBytes     = uint64(10 << 30) // 10 GiB
	diskFreeFloorBytes    = uint64(3 << 30)  // 3 GiB
	diskFreeWarnMaxShare  = 0.25             // warn threshold ≤ 25% of the volume
	diskFreeFloorMaxShare = 0.10             // floor threshold ≤ 10% of the volume

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
// drain tier's resume level. Admission ENGAGES at the floor but RESUMES only
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

	warnFraction  float64
	floorFraction float64
	warnBytes     uint64
	floorBytes    uint64

	// protect gates the CONSUMER tier — ingest admission, retention
	// re-routing, replica catch-up. It engages at the floor and resumes
	// only above the WARN band (admissionResumeAbove), reserving headroom
	// for the release burst so the deferred backlog cannot re-cross the
	// floor in one sampling window (the ratchet, gastrolog-67gvjo).
	protect atomic.Bool
	// deferWrites gates the DRAIN tier — chunking builds and the collection
	// pulls that feed them. It engages at the floor but resumes EARLIER
	// (floor + hysteresis) than admission: builds seal the backlog into
	// chunks retention can expunge to free space, so the delete-disposition
	// drain would deadlock if they waited for the consumer tier.
	deferWrites atomic.Bool

	// vaultFootprint measures a vault's whole local disk claim for the
	// max-size budget. Injected by the orchestrator (chunk + index bytes
	// plus pipeline segment backlog); injectable for tests.
	vaultFootprint func(glid.GLID) int64

	mu          sync.Mutex
	alarmRaised bool
	// vaults holds per-vault guard state: each vault is evaluated against
	// its OWN backing paths and (optionally) config-overridden thresholds,
	// so one vault's starved volume suspends only that vault's admission
	// while vaults on healthy volumes keep ingesting. Keyed by vault ID.
	// Guarded by mu; the per-vault protect flags are read lock-free.
	vaults map[glid.GLID]*vaultDiskGuard
}

// vaultDiskGuard is one vault's disk-guard state.
type vaultDiskGuard struct {
	paths       []string
	warnBytes   uint64 // 0 = inherit node defaults
	floorBytes  uint64 // 0 = inherit node defaults
	protect     atomic.Bool
	alarmRaised bool
	name        string

	// Max-size budget (cap-and-refuse): the vault's whole local disk claim
	// — sealed chunks, indexes, pipeline segment backlog — measured against
	// maxSizeBytes. 0 = unlimited. Distinct from the free-space thresholds:
	// those protect the VOLUME, the budget bounds the VAULT.
	maxSizeBytes    uint64
	capped          atomic.Bool
	sizeAlarmRaised bool
}

func newDiskGuardWithLogger(paths []string, logger *slog.Logger) *diskGuard {
	g := newDiskGuard(paths)
	g.logger = logger
	return g
}

func newDiskGuard(paths []string) *diskGuard {
	g := &diskGuard{
		paths:         paths,
		sample:        statfsSample,
		warnFraction:  diskFreeWarnFraction,
		floorFraction: diskFreeFloorFraction,
		warnBytes:     diskFreeWarnBytes,
		floorBytes:    diskFreeFloorBytes,
	}
	// Operator overrides in whole GiB via the .env channel.
	if v, err := strconv.ParseUint(os.Getenv("GLOG_DISK_FREE_WARN_GB"), 10, 32); err == nil && v > 0 {
		g.warnBytes = v << 30
	}
	if v, err := strconv.ParseUint(os.Getenv("GLOG_DISK_FREE_FLOOR_GB"), 10, 32); err == nil && v > 0 {
		g.floorBytes = v << 30
	}
	return g
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

func (g *diskGuard) warnThreshold(total uint64) uint64 {
	t := max(uint64(float64(total)*g.warnFraction), g.warnBytes)
	return min(t, uint64(float64(total)*diskFreeWarnMaxShare))
}

func (g *diskGuard) floorThreshold(total uint64) uint64 {
	t := max(uint64(float64(total)*g.floorFraction), g.floorBytes)
	return min(t, uint64(float64(total)*diskFreeFloorMaxShare))
}

// SetVaultGuard registers (or updates) a vault's guard entry. paths are the
// vault's local backing directories (storage chunks dir, segment root);
// warn/floor of 0 inherit the node defaults with share clamps; maxSizeBytes
// of 0 means no budget. Called from the discovery refresh; removal via
// RemoveVaultGuard / retainVaultGuards.
func (g *diskGuard) SetVaultGuard(vaultID glid.GLID, name string, paths []string, warnBytes, floorBytes, maxSizeBytes uint64) {
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
	v.paths = paths
	v.name = name
	v.warnBytes = warnBytes
	v.floorBytes = floorBytes
	v.maxSizeBytes = maxSizeBytes
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
// evaluated.
func (g *diskGuard) retainVaultGuards(keep map[glid.GLID]bool, alerts AlertCollector) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, v := range g.vaults {
		if keep[id] {
			continue
		}
		if v.alarmRaised && alerts != nil {
			alerts.Clear("disk-space:" + id.String())
		}
		if v.sizeAlarmRaised && alerts != nil {
			alerts.Clear("vault-max-size:" + id.String())
		}
		delete(g.vaults, id)
	}
}

// vaultProtectActive reports whether admission for this vault destination is
// suspended. Lock-free read; false for unknown vaults.
func (g *diskGuard) vaultProtectActive(vaultID glid.GLID) bool {
	g.mu.Lock()
	v := g.vaults[vaultID]
	g.mu.Unlock()
	return v != nil && v.protect.Load()
}

// vaultSizeCapped reports whether this vault's local disk claim has reached
// its max-size budget. Lock-free read; false for unknown vaults.
func (g *diskGuard) vaultSizeCapped(vaultID glid.GLID) bool {
	g.mu.Lock()
	v := g.vaults[vaultID]
	g.mu.Unlock()
	return v != nil && v.capped.Load()
}

// sizeCappedVaults lists the vaults currently at their max-size budget.
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

// protectedVaults lists the vaults currently under local disk protect.
// Broadcast in NodeStats so peers' admission gates honor it.
func (g *diskGuard) protectedVaults() []glid.GLID {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []glid.GLID
	for id, v := range g.vaults {
		if v.protect.Load() {
			out = append(out, id)
		}
	}
	return out
}

// evaluateVaults runs the per-vault guard pass. Caller is the scheduler job.
func (g *diskGuard) evaluateVaults(alerts AlertCollector) {
	g.mu.Lock()
	entries := maps.Clone(g.vaults)
	g.mu.Unlock()

	for id, v := range entries {
		if free, total, ok := g.worstFreeOf(v.paths); ok {
			warnAt := g.warnThreshold(total)
			if v.warnBytes > 0 {
				warnAt = v.warnBytes
			}
			floorAt := g.floorThreshold(total)
			if v.floorBytes > 0 {
				floorAt = v.floorBytes
			}
			g.reconcileVaultProtect(id, v, free, floorAt, warnAt)
			g.reconcileVaultAlarm(alerts, id, v, free, total, warnAt)
		}
		if v.maxSizeBytes > 0 && g.vaultFootprint != nil {
			used := footprintBytes(g.vaultFootprint(id))
			g.reconcileVaultSizeCap(id, v, used)
			g.reconcileVaultSizeAlarm(alerts, id, v, used)
		}
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

func (g *diskGuard) reconcileVaultSizeAlarm(alerts AlertCollector, id glid.GLID, v *vaultDiskGuard, used uint64) {
	if alerts == nil {
		return
	}
	alertID := "vault-max-size:" + id.String()
	approachAt := sizeApproachThreshold(v.maxSizeBytes)
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case used >= approachAt:
		severity := alert.Warning
		msg := fmt.Sprintf(
			"Vault %s is approaching its size budget: %s of %s used. Raise the budget, shorten retention, or add a size retention policy to drain ahead of the cap.",
			v.name, units.FormatBytesDisplay(int64(used)), units.FormatBytesDisplay(int64(v.maxSizeBytes))) //nolint:gosec // display only
		if v.capped.Load() {
			severity = alert.Error
			msg = fmt.Sprintf(
				"Vault %s is at its size budget: %s of %s used — new records for this vault are REFUSED until retention drains it. Other vaults are unaffected.",
				v.name, units.FormatBytesDisplay(int64(used)), units.FormatBytesDisplay(int64(v.maxSizeBytes))) //nolint:gosec // display only
		}
		alerts.Set(alertID, severity, "storage", msg)
		v.sizeAlarmRaised = true
	case v.sizeAlarmRaised && used < approachAt-approachAt/10:
		alerts.Clear(alertID)
		v.sizeAlarmRaised = false
	}
}

// reconcileVaultProtect gates a single vault's per-destination admission. Like
// the node consumer tier it engages at the vault's floor and resumes only
// above its WARN band — same ratchet-avoidance headroom. There is no per-vault
// drain tier: builds and pulls are gated node-globally by deferWrites.
func (g *diskGuard) reconcileVaultProtect(id glid.GLID, v *vaultDiskGuard, free, floorAt, warnAt uint64) {
	resumeAt := admissionResumeAbove(floorAt, warnAt)
	switch {
	case v.protect.Load() && free > resumeAt:
		v.protect.Store(false)
		if g.logger != nil {
			g.logger.Info("vault disk protect released — admission resumed for vault",
				"vault", id, "name", v.name, "free", fmtBytes(free), "resumeAbove", fmtBytes(resumeAt))
		}
	case !v.protect.Load() && free < floorAt:
		v.protect.Store(true)
		if g.logger != nil {
			g.logger.Warn("vault disk protect engaged — admission suspended for vault until free space clears its low-disk alarm band",
				"vault", id, "name", v.name,
				"free", fmtBytes(free), "floor", fmtBytes(floorAt), "resumeAbove", fmtBytes(resumeAt))
		}
	}
}

func (g *diskGuard) reconcileVaultAlarm(alerts AlertCollector, id glid.GLID, v *vaultDiskGuard, free, total, warnAt uint64) {
	if alerts == nil {
		return
	}
	alertID := "disk-space:" + id.String()
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case free < warnAt:
		msg := fmt.Sprintf(
			"Low disk space for vault %s: %s free of %s on its volume. Free space, add capacity, raise the vault's threshold, or shorten its retention.",
			v.name, units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total))) //nolint:gosec // display only
		if v.protect.Load() {
			msg = fmt.Sprintf(
				"Out of disk space for vault %s: %s free — admission for this vault is SUSPENDED until space frees. Other vaults are unaffected.",
				v.name, units.FormatBytesDisplay(int64(free))) //nolint:gosec // display only
		}
		alerts.Set(alertID, alert.Error, "storage", msg)
		v.alarmRaised = true
	case v.alarmRaised && free > clearAbove(warnAt):
		alerts.Clear(alertID)
		v.alarmRaised = false
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
func (g *diskGuard) evaluate(alerts AlertCollector) {
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
		msg := fmt.Sprintf(
			"Low disk space: %s free of %s. Free space, add capacity, or shorten retention.",
			units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total))) //nolint:gosec // display only
		if g.protect.Load() {
			msg = fmt.Sprintf(
				"Out of disk space: %s free of %s — ingest admission is SUSPENDED on this node until space is freed. Retention and deletes keep running.",
				units.FormatBytesDisplay(int64(free)), units.FormatBytesDisplay(int64(total))) //nolint:gosec // display only
		}
		alerts.Set(diskGuardAlertID, alert.Error, "storage", msg)
		g.alarmRaised = true
	case g.alarmRaised && free > clearAbove(warnAt):
		alerts.Clear(diskGuardAlertID)
		g.alarmRaised = false
	}
}

// reconcileProtect drives the two-tier staged protect. Both tiers engage
// together at the floor; on recovery the DRAIN tier resumes first (so the
// pipeline seals backlog and retention frees space) and the CONSUMER tier
// resumes last, only above the WARN band. Transitions log — admission nacks
// are silent in the pump, so without these lines the guard's staging is
// invisible in the log record.
func (g *diskGuard) reconcileProtect(free, floorAt, warnAt uint64) {
	// Drain tier: pause at floor, resume just above it.
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
	// Consumer tier: pause at floor, resume only above the WARN band.
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
// of disk space (the CONSUMER tier). Consulted by ingest admission, the
// catch-up puller, and retention re-routing — all net consumers that resume
// only above the WARN band.
func (o *Orchestrator) diskProtectActive() bool {
	return o.diskGuard != nil && o.diskGuard.protect.Load()
}

// diskDeferWrites reports whether the pipeline DRAIN tier (chunking builds and
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

// ErrVaultDiskProtect rejects records destined to a vault whose backing
// volume is below that vault's free-space floor. Scoped: other vaults on
// healthy volumes keep ingesting.
var ErrVaultDiskProtect = errors.New("vault's volume is out of disk space: admission for this vault suspended until space is freed")

// ErrVaultMaxSize rejects records destined to a vault whose local disk claim
// has reached its per-node max-size budget on some node. Cap-and-refuse:
// everything already accepted is kept; the newest records are nacked as
// retryable backpressure until retention or releases drain below the budget.
var ErrVaultMaxSize = errors.New("vault is at its size budget: admission for this vault refused until retention drains it")

// vaultAdmissionGate is the per-destination admission check. It honors both
// the local guard and — via the NodeStats broadcast — every live peer's:
// the starved volume or over-budget claim backing a vault is usually on a
// different node than the front door accepting records for it.
func (o *Orchestrator) vaultAdmissionGate(vaultID glid.GLID) error {
	if o.diskGuard != nil && o.diskGuard.vaultProtectActive(vaultID) {
		return ErrVaultDiskProtect
	}
	if fn := o.remoteVaultDiskProtected.Load(); fn != nil && (*fn)(vaultID) {
		return ErrVaultDiskProtect
	}
	if o.diskGuard != nil && o.diskGuard.vaultSizeCapped(vaultID) {
		return ErrVaultMaxSize
	}
	if fn := o.remoteVaultSizeCapped.Load(); fn != nil && (*fn)(vaultID) {
		return ErrVaultMaxSize
	}
	return nil
}

// SizeCappedVaults lists vaults at their local max-size budget, for the
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

// DiskProtectedVaults lists vaults under local disk protect, for the
// NodeStats broadcast.
func (o *Orchestrator) DiskProtectedVaults() []glid.GLID {
	if o.diskGuard == nil {
		return nil
	}
	return o.diskGuard.protectedVaults()
}

// SetRemoteVaultDiskProtected installs the peer-state lookup consulted by
// vaultAdmissionGate. Set once at app wiring, after cluster stats exist.
func (o *Orchestrator) SetRemoteVaultDiskProtected(fn func(glid.GLID) bool) {
	o.remoteVaultDiskProtected.Store(&fn)
}

// refreshVaultDiskGuards converges the guard's per-vault entries with the
// current config: every file vault with a placement on this node is guarded
// against its local storage volume(s). Discovery-based like the rotation and
// retention sweeps — vault add/update/remove and placement changes are all
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
	keep := make(map[glid.GLID]bool)
	for _, vc := range sys.Config.Vaults {
		if vc.Type != system.VaultTypeFile {
			continue
		}
		var paths []string
		for _, p := range vc.Placements {
			if system.NodeIDForStorage(p.StorageID, rt.NodeStorageConfigs) != o.localNodeID {
				continue
			}
			if fs := findFileStorageByID(rt, p.StorageID); fs != nil {
				paths = append(paths, fs.Path)
			}
		}
		// A vault with no local placement still claims local disk through
		// its origin segment backlog, so a max-size budget registers it
		// even when there is no volume to sample here.
		if len(paths) == 0 && vc.MaxSizeBytes == 0 {
			continue
		}
		keep[vc.ID] = true
		o.diskGuard.SetVaultGuard(vc.ID, vc.Name, paths, vc.DiskFreeWarnBytes, vc.DiskFreeFloorBytes, vc.MaxSizeBytes)
	}
	o.diskGuard.retainVaultGuards(keep, o.alerts)
}

// startDiskGuard registers the guard's scheduler job. No-op without paths.
func (o *Orchestrator) startDiskGuard() error {
	if o.diskGuard == nil || len(o.diskGuard.paths) == 0 {
		return nil
	}
	if err := o.scheduler.AddJob(diskGuardJobName, diskGuardSchedule, func() {
		o.diskGuard.evaluate(o.alerts)
		o.refreshVaultDiskGuards(context.Background())
		o.diskGuard.evaluateVaults(o.alerts)
	}); err != nil {
		return fmt.Errorf("disk guard: %w", err)
	}
	o.scheduler.Describe(diskGuardJobName,
		"Free-space guard: raises the disk-space alarm and suspends ingest admission below the floor")
	return nil
}
