package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/home"

	"github.com/google/uuid"
)

const (
	pullTimeout          = 2 * time.Minute  // per-peer pull timeout
	reconcileBaseDelay   = 5 * time.Second  // initial retry delay
	reconcileMaxDelay    = 2 * time.Minute  // max retry delay
	reconcileMaxAttempts = 10               // give up after this many rounds
)

// lookupFileManager handles lookup file distribution across cluster nodes.
// On put notifications, it checks whether the file exists locally and pulls
// it from a peer if missing. On delete notifications, it cleans up the local
// disk. File pulls are asynchronous to avoid blocking FSM.Apply.
type lookupFileManager struct {
	homeDir    string
	transferrer *cluster.LookupTransferrer
	peerIDs    func() []string // returns peer node IDs in the cluster
	fileExists func(fileID string) bool
	logger     *slog.Logger
}

var _ LookupFileHandler = (*lookupFileManager)(nil)

// OnPut checks if the file exists locally; if not, starts an async pull from a peer.
func (m *lookupFileManager) OnPut(_ context.Context, fileID uuid.UUID) {
	fid := fileID.String()
	if m.fileExists(fid) {
		return // already have it (we're the uploader)
	}

	// Pull from a peer in the background — don't block Raft apply.
	go m.pullFromPeer(context.Background(), fid)
}

// OnDelete removes the lookup file from local disk.
func (m *lookupFileManager) OnDelete(fileID uuid.UUID) {
	if m.homeDir == "" {
		return
	}
	hd := home.New(m.homeDir)
	dir := hd.LookupFileDir(fileID.String())
	if err := os.RemoveAll(dir); err != nil {
		m.logger.Warn("cleanup lookup file", "file_id", fileID, "error", err)
	} else {
		m.logger.Info("removed lookup file", "file_id", fileID, "dir", dir)
	}
}

// pullFromPeer tries each peer until one can provide the file.
// Returns true if the file was pulled successfully.
func (m *lookupFileManager) pullFromPeer(ctx context.Context, fileID string) bool {
	hd := home.New(m.homeDir)
	destDir := hd.LookupFileDir(fileID)

	for _, peerID := range m.peerIDs() {
		pullCtx, cancel := context.WithTimeout(ctx, pullTimeout)
		err := m.transferrer.PullFile(pullCtx, peerID, fileID, destDir)
		cancel()
		if err != nil {
			m.logger.Debug("pull lookup file from peer failed", "file_id", fileID, "peer", peerID, "error", err)
			continue
		}
		m.logger.Info("pulled lookup file from peer", "file_id", fileID, "peer", peerID)
		return true
	}
	m.logger.Warn("failed to pull lookup file from any peer", "file_id", fileID)
	return false
}

// reconcileLookupFiles loads the manifest from the config store and retries
// pulling missing files with exponential backoff. Peers may not be ready
// immediately after a cluster restart, so we keep trying.
func reconcileLookupFiles(ctx context.Context, cfgStore config.Store, mgr *lookupFileManager, logger *slog.Logger) {
	delay := reconcileBaseDelay

	for attempt := range reconcileMaxAttempts {
		if ctx.Err() != nil {
			return
		}

		files, err := cfgStore.ListLookupFiles(ctx)
		if err != nil {
			logger.Error("reconcile lookup files: list from store", "error", err)
			return
		}

		var missing []string
		for _, f := range files {
			fid := f.ID.String()
			if !mgr.fileExists(fid) {
				missing = append(missing, fid)
			}
		}
		if len(missing) == 0 {
			if attempt > 0 {
				logger.Info("reconcile lookup files: all files present")
			}
			return
		}

		logger.Info("reconcile lookup files: pulling missing files",
			"missing", len(missing), "total", len(files), "attempt", attempt+1)

		for _, fid := range missing {
			if ctx.Err() != nil {
				return
			}
			mgr.pullFromPeer(ctx, fid)
		}

		// Check if everything landed before sleeping.
		allPresent := true
		for _, fid := range missing {
			if !mgr.fileExists(fid) {
				allPresent = false
				break
			}
		}
		if allPresent {
			logger.Info("reconcile lookup files: all files present")
			return
		}

		logger.Info("reconcile lookup files: some files still missing, retrying",
			"delay", delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, reconcileMaxDelay)
	}

	logger.Error("reconcile lookup files: gave up after max attempts",
		"attempts", reconcileMaxAttempts)
}
