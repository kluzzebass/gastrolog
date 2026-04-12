package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
	"gastrolog/internal/home"

	"github.com/google/uuid"
)

const (
	pullTimeout            = 2 * time.Minute  // per-peer pull timeout
	reconcileBaseDelay     = 5 * time.Second  // initial retry delay
	reconcileMaxDelay      = 2 * time.Minute  // max retry delay
	reconcileMaxAttempts   = 10               // give up after this many rounds
	periodicReconcileEvery = 5 * time.Minute  // drift check interval
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
func (m *managedFileManager) OnPut(_ context.Context, fileID uuid.UUID) {
	fid := fileID.String()
	if m.fileExists(fid) {
		return // already have it (we're the uploader)
	}

	// Pull from a peer in the background — don't block Raft apply.
	go m.pullFromPeer(context.Background(), fid)
}

// OnDelete removes the managed file from local disk.
func (m *managedFileManager) OnDelete(fileID uuid.UUID) {
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
	destDir := hd.ManagedFileDir(fileID)

	for _, peerID := range m.peerIDs() {
		pullCtx, cancel := context.WithTimeout(ctx, pullTimeout)
		err := m.transferrer.PullFile(pullCtx, peerID, fileID, destDir)
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

// RunPeriodicReconciliation checks for manifest-vs-disk drift on a timer.
// Runs until ctx is cancelled.
func (m *managedFileManager) RunPeriodicReconciliation(ctx context.Context) {
	ticker := time.NewTicker(periodicReconcileEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcileOnce(ctx)
		}
	}
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
