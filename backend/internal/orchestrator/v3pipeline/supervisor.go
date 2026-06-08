// Package v3pipeline is the orchestrator-owned supervisor shell for the V3
// ingest pipeline. It owns the bounded inter-phase queue graph
//
//	ingest → digest → route → segment → distribute → collect → chunk
//
// and a reconcile surface (RegisterVault/UnregisterVault) that starts and stops
// the seven pipeline/* managers per vault home as placement changes bring vaults
// onto or off this node.
//
// This is the first Rubicon slice (gastrolog-3kx8v): it establishes the
// supervisor lifecycle and queue wiring. Real ingester factories, ack-after-durable
// semantics, vault-ctl feeds, and the leader planner are filled in by later slices
// (gastrolog-214bz B→E). Injection points (publishers, appliers, FSM callbacks,
// pull/log/receipt clients, nudge hooks) are supplied per vault via VaultSpec, so
// this package carries no opinion about how they are produced.
package v3pipeline

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
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrAlreadyRunning is returned when Start is called on a running supervisor.
var ErrAlreadyRunning = errors.New("v3 pipeline supervisor already running")

// ErrNotRunning is returned when Stop is called on a stopped supervisor.
var ErrNotRunning = errors.New("v3 pipeline supervisor not running")

// ErrVaultRegistered is returned when a vault is registered twice.
var ErrVaultRegistered = errors.New("vault already registered")

// Config configures the supervisor and its queue graph. All sizing fields have
// sane defaults; only Table is effectively required once records flow.
type Config struct {
	NodeID glid.GLID
	Logger *slog.Logger

	// Table is the shared, static routing table. Records matched against it are
	// fanned out to the per-vault segmentation queues registered via RegisterVault.
	Table *routing.Table

	// Digesters run in order on each ingestion message before record build.
	Digesters []digestion.Digester

	// Queue sizing. Zero selects the manager default.
	IngestionOutCapacity     int
	DigestionWorkers         int
	DigestionOutCapacity     int
	RoutingWorkers           int
	RoutingInCapacity        int
	SegmentationCompletedCap int
	SegmentationEncodeCap    int
	DistributionPullQueueCap int

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
}

type vaultRoles struct {
	origin bool
	home   bool
}

// Supervisor owns the V3 pipeline queue graph and the seven phase managers, and
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

	ingestOut <-chan ingestion.Message
	digestOut <-chan digestion.Output
	completed <-chan segmentation.CompletedSegment
	routingIn chan routing.Input
	pullIn    chan<- distribution.PullRequest

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
		NodeID:       cfg.NodeID,
		OutCapacity:  cfg.IngestionOutCapacity,
		Logger:       cfg.Logger,
		OnCheckpoint: cfg.OnCheckpoint,
		PressureGate: cfg.PressureGate,
	})
	digest, digestOut := digestion.New(digestion.Config{
		Workers:     cfg.DigestionWorkers,
		OutCapacity: cfg.DigestionOutCapacity,
		Digesters:   cfg.Digesters,
	})
	route := routing.New(routing.Config{
		Workers: cfg.RoutingWorkers,
		Table:   cfg.Table,
	})
	seg, completed := segmentation.New(segmentation.Config{
		ClosePolicy:     cfg.SegmentClosePolicy,
		SyncBatchSize:   cfg.SegmentSyncBatchSize,
		SyncBatchWindow: cfg.SegmentSyncBatchWindow,
		MaxCommitDelay:  cfg.SegmentMaxCommitDelay,
		DisableFsync:    cfg.SegmentDisableFsync,
		EncodeQueueCap:  cfg.SegmentationEncodeCap,
		CompletedCap:    cfg.SegmentationCompletedCap,
	})
	dist, pullIn := distribution.New(distribution.Config{
		PullQueueCap: cfg.DistributionPullQueueCap,
	})
	col := collection.New(collection.Config{})
	chunk := chunking.New(chunking.Config{})

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
	s.wg.Go(func() { s.pump(runCtx) })

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
func (s *Supervisor) pump(ctx context.Context) {
	defer close(s.routingIn)
	for {
		select {
		case <-ctx.Done():
			return
		case out, ok := <-s.digestOut:
			if !ok {
				return
			}
			if out.Err != nil {
				if out.Ack != nil {
					out.Ack <- out.Err
				}
				continue
			}
			in := routing.IngestInput(out.Record)
			in.Ack = out.Ack
			select {
			case s.routingIn <- in:
			case <-ctx.Done():
				return
			}
		}
	}
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
// seam (also used by the chunking build nudge).
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

// IngestQueueDepth reports the current depth of the ingestion→digestion queue,
// the backlog of minted-but-not-yet-digested messages. It is the V3 analogue of
// the former ingest channel depth surfaced in node/health stats.
func (s *Supervisor) IngestQueueDepth() int { return len(s.ingestOut) }

// IngestQueueCapacity reports the capacity of the ingestion→digestion queue.
func (s *Supervisor) IngestQueueCapacity() int { return cap(s.ingestOut) }

// RegisterVault starts the managers for the roles the vault holds on this node.
// Safe before or during Start. It is idempotent only in the sense that a second
// registration of the same vault returns ErrVaultRegistered.
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

	roles := vaultRoles{origin: spec.Origin, home: spec.Home}

	if spec.Origin {
		if err := s.registerOrigin(spec); err != nil {
			return err
		}
	}
	if spec.Home {
		if err := s.registerHome(spec); err != nil {
			if spec.Origin {
				s.unregisterOrigin(spec.VaultID)
			}
			return err
		}
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
	if err := s.dist.RegisterVault(spec.VaultID, spec.OriginRoot, distribution.VaultConfig{
		Publisher:   spec.Publisher,
		LocalHolder: spec.LocalHolder,
	}); err != nil {
		s.route.UnregisterVault(spec.VaultID)
		s.seg.UnregisterVault(spec.VaultID)
		return fmt.Errorf("distribution register: %w", err)
	}
	return nil
}

func (s *Supervisor) registerHome(spec VaultSpec) error {
	// Collection is registered without the FSM on purpose. The vault-ctl FSM has a
	// single SetOnPublishCompletedSegment slot, and chunking claims it (to drive the
	// leader planner). Collection is instead driven by the chunking build nudge
	// (CollectMissing → CollectOnce) and explicit CollectOnce passes. A later slice
	// (gastrolog-214bz C) introduces a publish fan-out that triggers both.
	if err := s.col.RegisterVault(spec.VaultID, spec.HomeRoot, collection.VaultConfig{
		Log:      spec.Log,
		Pull:     spec.Pull,
		Receipts: spec.Receipts,
	}); err != nil {
		return fmt.Errorf("collection register: %w", err)
	}
	if err := s.chunk.RegisterVault(spec.VaultID, chunking.VaultConfig{
		VaultRoot:  spec.HomeRoot,
		ChunkRoot:  spec.ChunkRoot,
		FSM:        spec.FSM,
		Locate:     spec.Locate,
		Nudge:      collectionNudge{mgr: s.col, vaultID: spec.VaultID},
		Applier:    spec.Applier,
		IsLeader:   spec.IsLeader,
		Policy:     spec.ChunkPolicy,
		NewChunkID: spec.NewChunkID,
	}); err != nil {
		s.col.UnregisterVault(spec.VaultID)
		return fmt.Errorf("chunking register: %w", err)
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

// collectionNudge adapts the collection manager to chunking's CollectionNudger so
// a chunk build that is missing segments locally can trigger a collect pass.
type collectionNudge struct {
	mgr     *collection.Manager
	vaultID glid.GLID
}

func (n collectionNudge) CollectMissing(ctx context.Context) error {
	return n.mgr.CollectOnce(ctx, n.vaultID)
}
