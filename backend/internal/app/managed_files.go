package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
)

const (
	pullTimeout          = 2 * time.Minute // per-peer pull timeout
	reconcileBaseDelay   = 5 * time.Second // initial retry delay
	reconcileMaxDelay    = 2 * time.Minute // max retry delay
	reconcileMaxAttempts = 10              // give up after this many rounds

	// managedFilesReconcileJobName is the operator-visible name shown
	// in the inspector's Scheduled view. Keep stable across releases.
	managedFilesReconcileJobName = "managed-files-reconcile"

	// managedFilesReconcileSchedule runs every 5 minutes on the
	// minute. 6-field cron (with-seconds). The drift check is
	// inexpensive — a manifest list + per-missing-file repair — so
	// minute-level cadence is plenty.
	managedFilesReconcileSchedule = "0 */5 * * * *"
)

// managedFileManager handles managed file distribution across cluster nodes.
// On put notifications, it checks whether the file exists locally and pulls
// it from a peer if missing. On delete notifications, it cleans up the local
// disk. File pulls are asynchronous to avoid blocking FSM.Apply.
type managedFileManager struct {
	homeDir     string
	cfgStore    system.Store
	transferrer *cluster.ManagedFileTransferrer
	peerIDs     func() []string // returns peer node IDs in the cluster
	fileExists  func(fileID string) bool
	logger      *slog.Logger
}

var _ ManagedFileHandler = (*managedFileManager)(nil)

// OnPut checks if the file exists locally; if not, starts an async pull from a peer.
func (m *managedFileManager) OnPut(_ context.Context, fileID glid.GLID) {
	fid := fileID.String()
	if m.fileExists(fid) {
		return // already have it (we're the uploader)
	}

	// Pull from a peer in the background — don't block Raft apply.
	go m.pullFromPeer(context.Background(), fid)
}

// OnDelete removes the managed file from local disk.
func (m *managedFileManager) OnDelete(fileID glid.GLID) {
	if m.homeDir == "" {
		return
	}
	hd := home.New(m.homeDir)
	dir := hd.ManagedFileDir(fileID.String())
	if err := os.RemoveAll(dir); err != nil {
		m.logger.Warn("cleanup managed file", "file_id", fileID, "error", err)
	} else {
		m.logger.Info("removed managed file", "file_id", fileID, "dir", dir)
	}
}

// pullFromPeer tries each peer until one can provide the file.
// Returns true if the file was pulled successfully.
func (m *managedFileManager) pullFromPeer(ctx context.Context, fileID string) bool {
	hd := home.New(m.homeDir)
	destPath := hd.ManagedFilePath(fileID)

	for _, peerID := range m.peerIDs() {
		pullCtx, cancel := context.WithTimeout(ctx, pullTimeout)
		err := m.transferrer.PullFile(pullCtx, peerID, fileID, destPath)
		cancel()
		if err != nil {
			m.logger.Debug("pull managed file from peer failed", "file_id", fileID, "peer", peerID, "error", err)
			continue
		}
		m.logger.Info("pulled managed file from peer", "file_id", fileID, "peer", peerID)
		return true
	}
	m.logger.Warn("failed to pull managed file from any peer", "file_id", fileID)
	return false
}

// RepairFile attempts to pull a specific file from a peer. Called on-demand
// when a managed file is in the manifest but missing from local disk.
// Returns true if the file was successfully repaired.
func (m *managedFileManager) RepairFile(fileID string) bool {
	if m.fileExists(fileID) {
		return true
	}
	m.logger.Info("on-demand repair: pulling missing managed file", "file_id", fileID)
	return m.pullFromPeer(context.Background(), fileID)
}

// wireManagedFiles wires the managed-file transfer handlers + the
// startup and periodic reconcile work. Called from app.go server
// setup; lives here to keep the orchestration code beside the
// related implementation and to keep the call site in app.go at a
// single line (flattens an otherwise nested guard block — nestif).
func wireManagedFiles(ctx context.Context, deps serverDeps, srv *server.Server) {
	if deps.ClusterSrv == nil || deps.Dispatcher == nil {
		return
	}
	mgr := wireManagedFileTransfer(deps.ClusterSrv, srv, deps.CfgStore, deps.HomeDir, deps.Logger)
	deps.Dispatcher.managedFileHandler = mgr

	// On-demand repair: when the server resolves a manifest entry
	// but the file is missing from disk, this pulls it from a peer
	// before returning "not found".
	srv.SetManagedFileRepair(mgr.RepairFile)

	// Export-to-vault: remote nodes can forward export jobs to the
	// node that owns the target vault.
	deps.ClusterSrv.SetExportToVaultExecutor(srv.ExportToVaultFunc())

	// Startup reconciliation with backoff — one-shot, retries
	// internally, not periodic. The periodic drift check below is
	// registered with the orchestrator job scheduler so the operator
	// sees it in the inspector.
	go reconcileManagedFilesStartup(ctx, mgr)
	if err := startManagedFilesReconcile(ctx, deps.Orch.Scheduler(), mgr); err != nil {
		deps.Logger.Warn("schedule managed-files-reconcile job", "error", err)
	}
}

// startManagedFilesReconcile registers the drift-check task with
// the supplied scheduler as a recurring job. Returns the AddJob
// error if any. Attaches a Describe text for the inspector's
// Scheduled view so the operator can see what the job does + its
// every-node semantics (each node compares the FSM-replicated
// manifest against its own disk and pulls any missing files from
// peers).
func startManagedFilesReconcile(ctx context.Context, scheduler scheduledJobRegistry, mgr *managedFileManager) error {
	task := func() { mgr.reconcileOnce(ctx) }
	if err := scheduler.AddJob(managedFilesReconcileJobName, managedFilesReconcileSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(managedFilesReconcileJobName,
		"Managed-files drift check. Runs on every node, every 5 minutes: lists the FSM-replicated managed-file manifest, compares against the local on-disk inventory, and pulls any missing files from peers via the ManagedFileTransferrer. Catches divergence between the manifest and the local disk that the OnPut/OnDelete hooks may have missed (e.g. a pull that failed during initial replication).")
	return nil
}

// reconcileOnce does a single manifest-vs-disk pass, pulling any missing files.
func (m *managedFileManager) reconcileOnce(ctx context.Context) {
	files, err := m.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		m.logger.Debug("periodic reconcile: list from store", "error", err)
		return
	}

	var missing []string
	for _, f := range files {
		fid := f.ID.String()
		if !m.fileExists(fid) {
			missing = append(missing, fid)
		}
	}
	if len(missing) == 0 {
		return
	}

	m.logger.Info("periodic reconcile: pulling missing files",
		"missing", len(missing), "total", len(files))

	for _, fid := range missing {
		if ctx.Err() != nil {
			return
		}
		m.pullFromPeer(ctx, fid)
	}
}

// reconcileManagedFilesStartup retries pulling missing files with exponential
// backoff. Peers may not be ready immediately after a cluster restart, so we
// keep trying. Once all files are present (or we give up), the periodic loop
// takes over.
func reconcileManagedFilesStartup(ctx context.Context, mgr *managedFileManager) {
	delay := reconcileBaseDelay

	for attempt := range reconcileMaxAttempts {
		if ctx.Err() != nil {
			return
		}

		remaining := mgr.missingFileCount(ctx)
		if remaining == 0 {
			if attempt > 0 {
				mgr.logger.Info("startup reconcile: all files present")
			}
			return
		}

		mgr.logger.Info("startup reconcile: pulling missing files",
			"missing", remaining, "attempt", attempt+1)
		mgr.reconcileOnce(ctx)

		// Re-check after pulling.
		if mgr.missingFileCount(ctx) == 0 {
			mgr.logger.Info("startup reconcile: all files present")
			return
		}

		mgr.logger.Info("startup reconcile: some files still missing, retrying",
			"delay", delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, reconcileMaxDelay)
	}

	mgr.logger.Error("startup reconcile: gave up after max attempts",
		"attempts", reconcileMaxAttempts)
}

// missingFileCount returns the number of manifest files not on local disk.
func (m *managedFileManager) missingFileCount(ctx context.Context) int {
	files, err := m.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		return 0
	}
	n := 0
	for _, f := range files {
		if !m.fileExists(f.ID.String()) {
			n++
		}
	}
	return n
}
