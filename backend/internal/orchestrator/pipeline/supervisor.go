// Package pipeline is the orchestrator-owned supervisor shell for the ingest
// pipeline. It owns the bounded inter-phase queue graph
//
//	ingest → digest → route → segment → distribute → collect → chunk
//
// and a reconcile surface (RegisterVault/UnregisterVault) that starts and stops
// the seven pipeline/* managers per vault home as placement changes bring vaults
// onto or off this node.
//
// The supervisor lifecycle and queue wiring landed first (gastrolog-3kx8v).
// Ingester factories, ack-after-durable semantics, vault-ctl feeds, and the
// leader planner are filled in by later Rubicon slices (gastrolog-214bz B→E).
// Injection points (publishers, appliers, FSM callbacks, pull/log/receipt
// clients, segment collectors) are supplied per vault via VaultSpec, so this package
// carries no opinion about how they are produced.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrAlreadyRunning is returned when Start is called on a running supervisor.
var ErrAlreadyRunning = errors.New("pipeline supervisor already running")

// ErrNotRunning is returned when Stop is called on a stopped supervisor.
var ErrNotRunning = errors.New("pipeline supervisor not running")

// ErrVaultRegistered is returned when a vault is registered twice.
var ErrVaultRegistered = errors.New("vault already registered")

// ErrVaultNotRegistered is returned by SubmitToVault when the target vault has
// no local origin (segmentation) writer on this node.
var ErrVaultNotRegistered = errors.New("vault not registered as origin on this node")

// Config configures the supervisor and its queue graph. All sizing fields have
// sane defaults; only Table is effectively required once records flow.
type Config struct {
	// AdmissionGate, when non-nil, is consulted before accepting a new
	// record on any intake (ingester pump, Submit, SubmitToVault). A
	// non-nil return rejects the record with that error — nacked to the
	// source, which treats it as retryable backpressure (disk-space
	// floor, and the coming per-stage backlog watermarks).
	AdmissionGate func() error

	// DeferWritesGate, when non-nil, pauses the heavy-write DRAIN stages
	// (chunking builds, collection pulls) when it returns true. Distinct
	// from AdmissionGate: the drain tier resumes EARLIER than ingest
	// admission on disk recovery so the pipeline can seal backlog into
	// chunks retention frees, rather than deadlocking behind the closed
	// front door (gastrolog-67gvjo staged release). Falls back to tracking
	// AdmissionGate when nil.
	DeferWritesGate func() bool

	// VaultAdmissionGate, when non-nil, is the per-destination admission
	// check: consulted for each matched vault at routing fan-out and for the
	// target vault on SubmitToVault. A non-nil return rejects the record for
	// ALL destinations (partial delivery would be silent loss for the gated
	// vault). Backed by the per-vault disk guard, local and peer-broadcast.
	VaultAdmissionGate func(glid.GLID) error

	NodeID glid.GLID
	Logger *slog.Logger
	// Alerts raises operator alerts for degraded pipeline components
	// (segmentation writers that lose their working segment). Nil disables.
	Alerts segmentation.AlertSink

	// Table is the shared, static routing table. Records matched against it are
	// fanned out to the per-vault segmentation queues registered via RegisterVault.
	Table *routing.Table

	// Digesters run in order on each ingestion message before record build.
	Digesters []digestion.Digester

	// Queue sizing. Zero selects the manager default.
	IngestionOutCapacity         int
	DigestionWorkers             int
	DigestionOutCapacity         int
	RoutingWorkers               int
	RoutingInCapacity            int
	SegmentationCompletedCap     int
	SegmentationEncodeCap        int
	DistributionPullQueueCap     int
	DistributionPublishQueueCap  int
	DistributionPublishWorkers   int
	DistributionPublishBatchSize int

	// Segmentation close policy and node-global commit/fsync defaults. Per-vault
	// overrides ride on VaultSpec.Commit.
	SegmentClosePolicy     segmentation.ClosePolicy
	SegmentSyncBatchSize   int
	SegmentSyncBatchWindow time.Duration
	SegmentMaxCommitDelay  time.Duration
	SegmentDisableFsync    bool

	// Ingestion hooks. PressureGate and OnCheckpoint are optional; the real
	// ingester factory and pressure wiring land in a later slice.
	OnCheckpoint func(id glid.GLID, data []byte)
	PressureGate *chanwatch.PressureGate
	// IngestionRetryDelay overrides the pause before a failed ingester run is
	// retried; consecutiveFailures counts error exits since the last clean
	// run. Nil uses the ingestion manager's default jittered exponential
	// backoff (3–5s first retry).
	IngestionRetryDelay func(consecutiveFailures int) time.Duration
	// IngestionCheckpointInterval overrides the period between checkpoint
	// saves for Checkpointable ingesters. Zero uses the ingestion manager's
	// default (5s).
	IngestionCheckpointInterval time.Duration
}

// VaultSpec describes the roles and per-vault dependencies for one vault on this
// node. A vault may be Origin (segmentation + distribution run here), Home
// (collection + chunking run here), or both. Later slices supply the real
// publishers/appliers/FSM; this slice only wires whatever the caller provides.
type VaultSpec struct {
	VaultID glid.GLID

	// Origin enables the write/distribute side for this vault on this node.
	Origin bool
	// OriginRoot is the vault storage root holding segmentation working/ and
	// completed/ directories. Required when Origin is set.
	OriginRoot string
	// Publisher publishes completed-segment metadata. Required when Origin is set.
	Publisher distribution.Publisher
	// LocalHolder reports whether this node holds the vault locally (completed→head
	// rename instead of remote pull). Optional; defaults to false.
	LocalHolder func() bool
	// Commit overrides this vault's commit/fsync tuning (group-commit coalesce,
	// fire-and-forget batch, DisableFsync). Zero fields inherit the node-global
	// SegmentSyncBatchSize/Window/MaxCommitDelay/DisableFsync defaults. Origin only.
	Commit segmentation.VaultConfig

	// Home enables the collect/chunk side for this vault on this node.
	Home bool
	// HomeRoot is the storage root for collected head segments and chunking.
	// Required when Home is set.
	HomeRoot string
	// ChunkRoot is where GLCB chunk blobs are written. Required when Home is set.
	ChunkRoot string
	// FSM is the vault-ctl state machine. Required when Home is set.
	FSM *vaultctlfsm.FSM
	// LookupFSM returns the live vault-ctl sub-FSM after snapshot Restore.
	LookupFSM func() *vaultctlfsm.FSM
	// Log, Pull, Receipts feed the collection manager. Required when Home is set.
	Log      collection.LogReader
	Pull     collection.PullClient
	Receipts collection.ReceiptCommitter
	// Locate resolves segment IDs to on-disk paths for chunk builds. Required when Home is set.
	Locate chunking.SegmentLocator
	// Applier proposes vault-ctl manifest edits (leader only). Required when Home is set.
	Applier chunking.VaultCtlApplier
	// IsLeader reports whether this node is the vault-ctl leader. Optional; defaults to false.
	IsLeader func() bool
	// ChunkPolicy controls open-chunk manifest rotation.
	ChunkPolicy chunking.ManifestRotationPolicy
	// NewChunkID overrides chunk ID minting (for tests). Optional.
	NewChunkID func() chunk.ChunkID
	// OnChunkBuilt fires after this node builds a sealed GLCB locally, so the
	// orchestrator can register it for queries when the FSM seal already
	// applied (build-finishes-last ordering). Optional.
	OnChunkBuilt func(chunk.ChunkID)
	// OnManifestOpened fires when CmdOpenChunkManifest applies. Optional.
	OnManifestOpened func(*vaultctlfsm.OpenChunkManifest)
	// ChunkRetentionGiveUpTTL returns the vault's delete-disposition retention
	// TTL for the segment give-up bound; ok=false disables it (no TTL rule, or
	// a route-disposition runner vetoes: those records must be routed, never
	// dropped). See chunking.VaultConfig.RetentionGiveUpTTL.
	ChunkRetentionGiveUpTTL func() (time.Duration, bool)
	// ChunkRequiredHolders returns placement member node IDs that must hold each
	// segment before the leader proposes ReleaseSegments. Optional.
	ChunkRequiredHolders func() []string
}

type vaultRoles struct {
	origin       bool
	home         bool
	collects     bool
	chunking     bool
	unsubRelease func()
}

// stagingHeadPurgeRoots returns distinct vault staging roots whose head/ copies
// must be dropped after ReleaseSegments. Origin and home often share one root
// in production; tests and misconfig can split them.
func stagingHeadPurgeRoots(originRoot, homeRoot string, origin, homeSide bool) []string {
	var roots []string
	if origin && originRoot != "" {
		roots = append(roots, originRoot)
	}
	if homeSide && homeRoot != "" && homeRoot != originRoot {
		roots = append(roots, homeRoot)
	}
	return roots
}

// Supervisor owns the pipeline queue graph and the seven phase managers, and
// drives per-vault start/stop on placement changes.
type Supervisor struct {
	cfg    Config
	logger *slog.Logger

	ingest *ingestion.Manager
	digest *digestion.Manager
	route  *routing.Manager
	seg    *segmentation.Manager
	dist   *distribution.Manager
	col    *collection.Manager
	chunk  *chunking.Manager

	ingestOut <-chan ingestion.IngestMessage
	digestOut <-chan digestion.Output
	completed <-chan segmentation.CompletedSegment
	routingIn chan routing.Input
	pullIn    chan<- distribution.PullRequest

	// sendMu guards routingIn against the multi-sender / single-close hazard:
	// pump and Submit both feed routingIn, but only pump closes it on Stop.
	// Senders take RLock around the closed check + send; the closer takes Lock.
	sendMu        sync.RWMutex
	routingClosed bool

	mu     sync.Mutex
	vaults map[glid.GLID]vaultRoles
	runCtx context.Context
	cancel context.CancelFunc

	running atomic.Bool
	wg      sync.WaitGroup
}

// New constructs the supervisor and all seven managers, wiring the inter-phase
// queues. Vaults may be registered before or after Start.
func New(cfg Config) *Supervisor {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	ingest, ingestOut := ingestion.New(ingestion.Config{
		NodeID:             cfg.NodeID,
		OutCapacity:        cfg.IngestionOutCapacity,
		Logger:             cfg.Logger,
		OnCheckpoint:       cfg.OnCheckpoint,
		PressureGate:       cfg.PressureGate,
		RetryDelay:         cfg.IngestionRetryDelay,
		CheckpointInterval: cfg.IngestionCheckpointInterval,
	})
	digest, digestOut := digestion.New(digestion.Config{
		Workers:     cfg.DigestionWorkers,
		OutCapacity: cfg.DigestionOutCapacity,
		Digesters:   cfg.Digesters,
	})
	route := routing.New(routing.Config{
		Workers:   cfg.RoutingWorkers,
		Table:     cfg.Table,
		VaultGate: cfg.VaultAdmissionGate,
	})
	dist, pullIn := distribution.New(distribution.Config{
		PullQueueCap:     cfg.DistributionPullQueueCap,
		PublishQueueCap:  cfg.DistributionPublishQueueCap,
		PublishWorkers:   cfg.DistributionPublishWorkers,
		PublishBatchSize: cfg.DistributionPublishBatchSize,
		Logger:           cfg.Logger,
	})
	// Heavy-write stages pause whenever admission is rejecting: builds and
	// inbound pulls create bytes, and under disk protect the last free
	// megabytes belong to the WAL (gastrolog-38bm9t — builds ENOSPC-looped
	// while protect held the front door).
	deferWrites := func() bool {
		if cfg.DeferWritesGate != nil {
			return cfg.DeferWritesGate()
		}
		// Pre-staged-release fallback: tie drain writes to admission.
		return cfg.AdmissionGate != nil && cfg.AdmissionGate() != nil
	}
	chunk := chunking.New(chunking.Config{Logger: cfg.Logger, Alerts: cfg.Alerts, DeferWrites: deferWrites})
	col := collection.New(collection.Config{
		Logger:      cfg.Logger,
		DeferWrites: deferWrites,
		OnPassComplete: func(vaultID glid.GLID) {
			chunk.NotifyVault(vaultID)
		},
	})
	seg, completed := segmentation.New(segmentation.Config{
		Logger:             cfg.Logger,
		Alerts:             cfg.Alerts,
		ClosePolicy:        cfg.SegmentClosePolicy,
		SyncBatchSize:      cfg.SegmentSyncBatchSize,
		SyncBatchWindow:    cfg.SegmentSyncBatchWindow,
		MaxCommitDelay:     cfg.SegmentMaxCommitDelay,
		DisableFsync:       cfg.SegmentDisableFsync,
		EncodeQueueCap:     cfg.SegmentationEncodeCap,
		CompletedCap:       cfg.SegmentationCompletedCap,
		OnCompletedDropped: dist.NotifyStranded,
	})

	routingCap := cfg.RoutingInCapacity
	if routingCap <= 0 {
		routingCap = 1000
	}

	return &Supervisor{
		cfg:       cfg,
		logger:    cfg.Logger,
		ingest:    ingest,
		digest:    digest,
		route:     route,
		seg:       seg,
		dist:      dist,
		col:       col,
		chunk:     chunk,
		ingestOut: ingestOut,
		digestOut: digestOut,
		completed: completed,
		routingIn: make(chan routing.Input, routingCap),
		pullIn:    pullIn,
		vaults:    make(map[glid.GLID]vaultRoles),
	}
}

// Start launches all seven phase managers and the digest→route pump. Ingestion is
// started last so the downstream graph is draining before any message is minted.
func (s *Supervisor) Start(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.runCtx = runCtx
	s.cancel = cancel
	s.mu.Unlock()

	s.wg.Go(func() { _ = s.seg.Run(runCtx) })
	s.wg.Go(func() { _ = s.dist.Run(runCtx, s.completed) })
	s.wg.Go(func() { _ = s.col.Run(runCtx) })
	s.wg.Go(func() { _ = s.chunk.Run(runCtx) })
	s.wg.Go(func() { _ = s.route.Run(runCtx, s.routingIn) })
	s.wg.Go(func() { _ = s.digest.Run(runCtx, s.ingestOut) })
	s.wg.Go(func() { s.pump() })

	if err := s.ingest.Start(runCtx); err != nil {
		cancel()
		s.wg.Wait()
		s.running.Store(false)
		return fmt.Errorf("start ingestion: %w", err)
	}
	return nil
}

// Stop drains the pipeline in order: ingestion stops (closing the ingest queue),
// which cascades through digest → pump → route as each upstream queue closes; then
// the run context is cancelled to stop segmentation, distribution, collection, and
// chunking. It blocks until every manager goroutine has exited.
func (s *Supervisor) Stop() error {
	if !s.running.CompareAndSwap(true, false) {
		return ErrNotRunning
	}
	_ = s.ingest.Stop()
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	return nil
}

// pump forwards digested records into the routing queue. Digest failures are
// nacked and dropped. For successful records the source ack rides on the routing
// Input and is fired downstream by the durable-commit path (ack-after-fsync), so
// pump only enqueues and never acks: blocking here per record would serialize the
// group commit and defeat batching. The ingester's ack channel must be buffered
// so the segmentation commit loop never stalls releasing it.
func (s *Supervisor) pump() {
	defer s.closeRouting()
	// Close-driven like the stages it bridges (gastrolog-5kcq5q): drains
	// digestOut until digestion closes it, then closes routingIn so the
	// routing workers exit. The pump sends directly — it is the sole
	// goroutine that closes routingIn, so its own sends can never race
	// that close; Submit (retention fan-out, the other producer) keeps
	// the guarded, ctx-bounded sendRouting path.
	for out := range s.digestOut {
		if out.Err != nil {
			if out.Ack != nil {
				out.Ack <- out.Err
			}
			continue
		}
		if err := s.admit(); err != nil {
			// Admission rejected (disk floor / backlog watermark):
			// nack so the source retries; an accepted-then-marooned
			// record would be ours to lose.
			if out.Ack != nil {
				out.Ack <- err
			}
			continue
		}
		in := routing.IngestInput(out.Record)
		in.Ack = out.Ack
		s.routingIn <- in
	}
}

// sendRouting delivers in to the routing queue, coordinating with closeRouting so
// concurrent Submit senders never write to a closed routingIn. (The pump sends
// directly — it closes routingIn itself, after its last send.) Returns false
// (nacking in.Ack) when the queue is already closed or ctx is cancelled before
// the send completes.
func (s *Supervisor) sendRouting(ctx context.Context, in routing.Input) bool {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.routingClosed {
		if in.Ack != nil {
			in.Ack <- ErrNotRunning
		}
		return false
	}
	select {
	case s.routingIn <- in:
		return true
	case <-ctx.Done():
		if in.Ack != nil {
			in.Ack <- ctx.Err()
		}
		return false
	}
}

// admit consults the configured admission gate; nil gate admits everything.
func (s *Supervisor) admit() error {
	if s.cfg.AdmissionGate == nil {
		return nil
	}
	return s.cfg.AdmissionGate()
}

// closeRouting closes routingIn exactly once under the write lock, after all
// in-flight sendRouting calls have drained. Called from pump's defer on Stop.
func (s *Supervisor) closeRouting() {
	s.sendMu.Lock()
	if !s.routingClosed {
		s.routingClosed = true
		close(s.routingIn)
	}
	s.sendMu.Unlock()
}

// ServePull streams a locally-held segment to a remote collector. Slice C wires
// this to the segment-pull RPC handler; it is exposed here as the orchestrator-owned
// seam so collectors on other nodes can fetch segments this node originated.
func (s *Supervisor) ServePull(req distribution.PullRequest) error {
	return s.dist.ServePull(req)
}

// CollectOnce runs one collection pass for a home vault: roll the assignment log,
// pull any missing segments, and promote them to head. Slice C drives this from
// publish/assignment signals; it is exposed as the orchestrator-owned collection
// seam (also used by chunking build materialization).
func (s *Supervisor) CollectOnce(ctx context.Context, vaultID glid.GLID) error {
	return s.col.CollectOnce(ctx, vaultID)
}

// ReconcileIngesters forwards an ingester assignment snapshot to the ingestion
// manager. The real assignment-driven factory lands in a later slice; this
// passthrough lets the supervisor be exercised end to end in the meantime.
func (s *Supervisor) ReconcileIngesters(specs []ingestion.IngesterSpec) error {
	return s.ingest.Reconcile(specs)
}

// SetRoutingTable atomically replaces the routing table the routing workers
// match against. The orchestrator recompiles the table whenever routes or vault
// placements change and publishes it here.
func (s *Supervisor) SetRoutingTable(t *routing.Table) {
	s.route.SetTable(t)
}

// Submit routes a single record through the pipeline routing stage exactly like
// a digested ingest record, but with a caller-supplied Source. It is the
// non-ingester entry for attr-routed writers — retention fan-out passes
// routing.RetentionSource(sourceVaultID, reason). The record is matched against
// the routing table and fanned out to every matched vault's segmentation queue;
// in.Ack, when non-nil, resolves after all matched targets durably commit (an
// unmatched record is a counted drop that still resolves the ack). Returns
// ErrNotRunning if the supervisor is stopped.
func (s *Supervisor) Submit(ctx context.Context, in routing.Input) error {
	if !s.running.Load() {
		return ErrNotRunning
	}
	if err := s.admit(); err != nil {
		return err
	}
	if !s.sendRouting(ctx, in) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrNotRunning
	}
	return nil
}

// SubmitToVault enqueues a record directly into a specific vault's segmentation
// queue on this node, bypassing the routing table. It is the direct-to-vault
// entry for writers that target a named vault and preserve the record's EventID
// (ImportRecords, export-to-vault). The vault must be Origin-registered on this
// node (reconcile origins every homed vault locally). ack, when non-nil,
// resolves after the durable commit. Returns ErrVaultNotRegistered when the
// vault has no local segmentation writer, or ErrNotRunning when stopped.
func (s *Supervisor) SubmitToVault(ctx context.Context, vaultID glid.GLID, rec *record.Record, ack chan<- error) error {
	if !s.running.Load() {
		return ErrNotRunning
	}
	if err := s.admit(); err != nil {
		return err
	}
	if s.cfg.VaultAdmissionGate != nil {
		if err := s.cfg.VaultAdmissionGate(vaultID); err != nil {
			return err
		}
	}
	err := s.seg.Submit(ctx, vaultID, segmentation.Input{Record: rec, Ack: ack})
	if errors.Is(err, segmentation.ErrUnknownVault) {
		return ErrVaultNotRegistered
	}
	return err
}

// RouteStats returns a snapshot of the routing manager's counters (global
// ingested/matched/unmatched totals plus per-vault and per-route matched counts).
// It is the pipeline source for the node's route-stats observability surface.
func (s *Supervisor) RouteStats() routing.StatsSnapshot {
	return s.route.Stats()
}

// RoutingActive reports whether a routing table is currently published, the
// pipeline analogue of the legacy "filter set active" flag.
func (s *Supervisor) RoutingActive() bool {
	return s.route.TableActive()
}

// RoutingTable returns the currently published routing table (nil when none has
// been set). The table is immutable; callers treat it as read-only. Exposed for
// observability and for tests that need to evaluate routing without driving the
// full async pipeline.
func (s *Supervisor) RoutingTable() *routing.Table {
	return s.route.Table()
}

// IngestQueueDepth reports the current depth of the ingestion→digestion queue,
// the backlog of minted-but-not-yet-digested messages. It is the analogue of
// the former ingest channel depth surfaced in node/health stats.
func (s *Supervisor) IngestQueueDepth() int { return len(s.ingestOut) }

// IngestQueueCapacity reports the capacity of the ingestion→digestion queue.
func (s *Supervisor) IngestQueueCapacity() int { return cap(s.ingestOut) }

// AppendStats returns per-vault cumulative segmentation throughput counters
// for the stats broadcast (gastrolog-4eh5ns).
func (s *Supervisor) AppendStats() []segmentation.AppendStats {
	return s.seg.AppendStats()
}

// CollectStats returns per-vault cumulative home-side collection counters
// (gastrolog-10n6k8).
func (s *Supervisor) CollectStats() []collection.VaultCollectStats {
	return s.col.CollectStats()
}

// SealStats returns per-vault cumulative GLCB seal counters
// (gastrolog-10n6k8).
func (s *Supervisor) SealStats() []chunking.VaultSealStats {
	return s.chunk.SealStats()
}

// RegisterVault starts the managers for the roles the vault holds on this node.
// Safe before or during Start. It is idempotent only in the sense that a second
// registration of the same vault returns ErrVaultRegistered.
//
//nolint:gocognit // wires origin, home, collection, chunking, and release hooks per vault
func (s *Supervisor) RegisterVault(spec VaultSpec) error {
	if spec.VaultID.IsZero() {
		return errors.New("vault spec missing ID")
	}
	if !spec.Origin && !spec.Home {
		return errors.New("vault spec must set Origin and/or Home")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.vaults[spec.VaultID]; ok {
		return ErrVaultRegistered
	}

	// The home side runs when this node collects (Home, with Log/Pull/Receipts)
	// and/or chunks (Locate). Chunking is independent of collection: a single-node
	// home chunks from its own head/ without a peer collector (Rubicon D).
	homeSide := spec.Home || spec.Locate != nil
	roles := vaultRoles{
		origin:   spec.Origin,
		home:     homeSide,
		collects: spec.Home,
		chunking: spec.Locate != nil,
	}

	if spec.Origin {
		if err := s.registerOrigin(spec); err != nil {
			return err
		}
	}
	if homeSide {
		if err := s.registerHome(spec); err != nil {
			if spec.Origin {
				s.unregisterOrigin(spec.VaultID)
			}
			return err
		}
	}

	releaseRoot := spec.OriginRoot
	if releaseRoot == "" {
		releaseRoot = spec.HomeRoot
	}
	if spec.FSM != nil && releaseRoot != "" {
		vaultID := spec.VaultID
		homeRoot := spec.HomeRoot
		purgeHomeHead := homeSide && homeRoot != ""
		originRoot := spec.OriginRoot
		headPurgeRoots := stagingHeadPurgeRoots(originRoot, homeRoot, spec.Origin, purgeHomeHead)
		roles.unsubRelease = spec.FSM.AddOnReleaseSegments(func(ids []glid.GLID) {
			if spec.Origin {
				s.dist.RetireSegments(vaultID, ids)
				for _, id := range ids {
					_ = paths.PurgeCompleted(releaseRoot, id)
				}
			}
			// Drop head/pre-head on every staging root this node uses. Distribution
			// promotes under OriginRoot; collection/chunking use HomeRoot when set.
			// Purging only HomeRoot left OriginRoot/head debris (gastrolog-3vlse).
			for _, id := range ids {
				for _, root := range headPurgeRoots {
					_ = paths.PurgeHeadStaging(root, id)
				}
			}
		})
	}

	s.vaults[spec.VaultID] = roles
	return nil
}

func (s *Supervisor) registerOrigin(spec VaultSpec) error {
	in, err := s.seg.RegisterVault(spec.VaultID, spec.OriginRoot, spec.Commit)
	if err != nil {
		return fmt.Errorf("segmentation register: %w", err)
	}
	s.route.RegisterVault(spec.VaultID, in)
	distCfg := distribution.VaultConfig{
		Publisher:   spec.Publisher,
		LocalHolder: spec.LocalHolder,
	}
	if spec.Home {
		vaultID := spec.VaultID
		distCfg.OnLocalHeadPromoted = func(segmentID glid.GLID) {
			s.col.NoteLocalHeadArrival(vaultID, segmentID)
			s.col.Notify(vaultID)
			s.chunk.NotifyVault(vaultID)
		}
		distCfg.OnPublishCommitted = func(glid.GLID) {
			s.col.Notify(vaultID)
		}
	}
	if err := s.dist.RegisterVault(spec.VaultID, spec.OriginRoot, distCfg); err != nil {
		s.route.UnregisterVault(spec.VaultID)
		s.seg.UnregisterVault(spec.VaultID)
		return fmt.Errorf("distribution register: %w", err)
	}
	return nil
}

func (s *Supervisor) registerHome(spec VaultSpec) error {
	// Collection subscribes to the vault-ctl publish fan-out (Rubicon C): a newly
	// published segment fires SetOnPublishCompletedSegment → triggerCollect, so
	// replication is event-driven, not timer-polled. The fan-out is additive, so
	// chunking (slice D) subscribes independently for its leader planner.
	//
	// Collection only registers when this node has peers to pull from (spec.Home
	// with Log/Pull/Receipts). A single-node home (no puller) skips collection
	// and chunks straight from its own head/.
	collectionRegistered := false
	if spec.Home {
		if err := s.col.RegisterVault(spec.VaultID, spec.HomeRoot, collection.VaultConfig{
			Log:      spec.Log,
			Pull:     spec.Pull,
			Receipts: spec.Receipts,
			FSM:      spec.FSM,
		}); err != nil {
			return fmt.Errorf("collection register: %w", err)
		}
		collectionRegistered = true
	}
	// Chunking is wired whenever Locate is supplied (Rubicon D): every home with
	// a vault-ctl handle plans (leader) and builds (all homes) chunks,
	// independent of the peer collector.
	if spec.Locate != nil {
		if err := s.chunk.RegisterVault(spec.VaultID, chunking.VaultConfig{
			VaultRoot:          spec.HomeRoot,
			ChunkRoot:          spec.ChunkRoot,
			FSM:                spec.FSM,
			LookupFSM:          spec.LookupFSM,
			Locate:             spec.Locate,
			Collector:          vaultSegmentCollector{mgr: s.col, vaultID: spec.VaultID},
			Applier:            spec.Applier,
			IsLeader:           spec.IsLeader,
			Policy:             spec.ChunkPolicy,
			NewChunkID:         spec.NewChunkID,
			OnBuilt:            spec.OnChunkBuilt,
			OnManifestOpened:   spec.OnManifestOpened,
			RequiredHolders:    spec.ChunkRequiredHolders,
			RetentionGiveUpTTL: spec.ChunkRetentionGiveUpTTL,
		}); err != nil {
			if collectionRegistered {
				s.col.UnregisterVault(spec.VaultID)
			}
			return fmt.Errorf("chunking register: %w", err)
		}
	}
	// A home registered after Start misses Run's one-shot startup catch-up, so
	// kick initial passes for whatever was registered: collect any segments
	// already in the registry, then plan/build any manifest work already sealed
	// or pending (e.g. this node just became a home, or restarted with a lagging
	// applied index). runCtx cancellation on Stop aborts in-flight work.
	if s.running.Load() && s.runCtx != nil {
		ctx := s.runCtx
		vid := spec.VaultID
		chunkCatchUp := spec.Locate != nil
		s.wg.Go(func() {
			if collectionRegistered {
				_ = s.col.CollectOnce(ctx, vid)
			}
			if chunkCatchUp {
				_ = s.chunk.RecoverOnce(ctx, vid)
				_ = s.chunk.PlanOnce(ctx, vid)
				_ = s.chunk.BuildOnce(ctx, vid)
			}
		})
	}
	return nil
}

// RecoverVault seals pipeline chunks whose local GLCB landed before CmdSealChunk
// applied — after controlled or uncontrolled process shutdown. Idempotent.
func (s *Supervisor) RecoverVault(ctx context.Context, vaultID glid.GLID) error {
	if s.chunk == nil {
		return nil
	}
	return s.chunk.RecoverOnce(ctx, vaultID)
}

// NotifyChunkingVault wakes the per-vault chunking worker (plan/build loop).
func (s *Supervisor) NotifyChunkingVault(vaultID glid.GLID) {
	if s.chunk != nil {
		s.chunk.NotifyVault(vaultID)
	}
}

// NotifyPublishRetry wakes distribution to drain staged vault-ctl publish retries.
func (s *Supervisor) NotifyPublishRetry() {
	if s.dist != nil {
		s.dist.NotifyPublishRetry()
	}
}

// NotifyCollectionVault wakes the per-vault collection worker.
func (s *Supervisor) NotifyCollectionVault(vaultID glid.GLID) {
	if s.col != nil {
		s.col.Notify(vaultID)
	}
}

// ChunkingRegistered reports whether chunking is active for a vault on this home.
func (s *Supervisor) ChunkingRegistered(vaultID glid.GLID) bool {
	return s.chunk != nil && s.chunk.HasVault(vaultID)
}

// RewireVaultAfterCtlRestore rebinds chunking and collection to the live vault-ctl
// sub-FSM after a group-level snapshot Restore. Returns ErrUnknownVault when the
// vault is not registered on this home yet (transient during startup).
func (s *Supervisor) RewireVaultAfterCtlRestore(vaultID glid.GLID, cfg RewireVaultConfig) error {
	if cfg.FSM == nil {
		return errors.New("vault-ctl FSM required")
	}
	chunkUnknown := false
	if s.chunk != nil {
		if err := s.chunk.RewireVaultFSM(vaultID, cfg.FSM, cfg.Applier); err != nil {
			if errors.Is(err, chunking.ErrUnknownVault) {
				chunkUnknown = true
			} else {
				return fmt.Errorf("chunking rewire: %w", err)
			}
		}
	}
	if s.col != nil && cfg.Log != nil && cfg.Pull != nil && cfg.Receipts != nil {
		if err := s.col.RewireVaultFSM(vaultID, collection.VaultConfig{
			FSM:      cfg.FSM,
			Log:      cfg.Log,
			Pull:     cfg.Pull,
			Receipts: cfg.Receipts,
		}); err != nil && !errors.Is(err, collection.ErrUnknownVault) {
			return fmt.Errorf("collection rewire: %w", err)
		}
	}
	if chunkUnknown {
		return chunking.ErrUnknownVault
	}
	return nil
}

// RewireVaultConfig carries live vault-ctl handles for post-restore pipeline rebind.
type RewireVaultConfig struct {
	FSM      *vaultctlfsm.FSM
	Applier  chunking.VaultCtlApplier
	Log      collection.LogReader
	Pull     collection.PullClient
	Receipts collection.ReceiptCommitter
}

// RotateChunkCron runs one leader-gated cron rotation step for a vault's open
// chunk manifest. It is the scheduler entry point for time-based (cron) sealing;
// the planner no-ops for non-leaders, and an unregistered/unknown vault is a
// no-op (the scheduler job may briefly outlive deregistration).
func (s *Supervisor) RotateChunkCron(ctx context.Context, vaultID glid.GLID) error {
	if err := s.chunk.RotateCron(ctx, vaultID); err != nil && !errors.Is(err, chunking.ErrUnknownVault) {
		return err
	}
	return nil
}

// UnregisterVault stops the managers for a vault that has left this node. It is a
// no-op for an unknown vault. Home managers are stopped before origin managers,
// and routing is detached before segmentation closes the input queue.
func (s *Supervisor) UnregisterVault(vaultID glid.GLID) {
	s.mu.Lock()
	roles, ok := s.vaults[vaultID]
	if ok {
		delete(s.vaults, vaultID)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	if roles.unsubRelease != nil {
		roles.unsubRelease()
	}
	if roles.home {
		s.chunk.UnregisterVault(vaultID)
		s.col.UnregisterVault(vaultID)
	}
	if roles.origin {
		s.unregisterOrigin(vaultID)
	}
}

func (s *Supervisor) unregisterOrigin(vaultID glid.GLID) {
	s.route.UnregisterVault(vaultID)
	s.dist.UnregisterVault(vaultID)
	s.seg.UnregisterVault(vaultID)
}

// vaultSegmentCollector adapts the collection manager to chunking's
// SegmentCollector — the build prerequisite that materializes manifest
// segment bytes on each home before GLCB merge.
type vaultSegmentCollector struct {
	mgr     *collection.Manager
	vaultID glid.GLID
}

func (n vaultSegmentCollector) CollectSegments(ctx context.Context, segmentIDs []glid.GLID) error {
	return n.mgr.CollectSegments(ctx, n.vaultID, segmentIDs)
}

// Nudge wakes the collection worker without waiting for a pass. The chunking
// worker must never block on collection (gastrolog-1b51yf); pass completion
// re-wakes chunking via OnPassComplete.
func (n vaultSegmentCollector) Nudge() {
	n.mgr.Notify(n.vaultID)
}
