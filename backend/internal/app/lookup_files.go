package app

import (
	"context"
	"log/slog"
	"os"

	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	"gastrolog/internal/home"

	"github.com/google/uuid"
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
	go m.pullFromPeer(fid)
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
func (m *lookupFileManager) pullFromPeer(fileID string) {
	ctx := context.Background()
	hd := home.New(m.homeDir)
	destDir := hd.LookupFileDir(fileID)

	for _, peerID := range m.peerIDs() {
		if err := m.transferrer.PullFile(ctx, peerID, fileID, destDir); err != nil {
			m.logger.Debug("pull lookup file from peer failed", "file_id", fileID, "peer", peerID, "error", err)
			continue
		}
		m.logger.Info("pulled lookup file from peer", "file_id", fileID, "peer", peerID)
		return
	}
	m.logger.Error("failed to pull lookup file from any peer", "file_id", fileID)
}

// Reconcile compares local lookup files against the Raft manifest and pulls
// any missing files from peers. Called on startup after the orchestrator is ready.
func (m *lookupFileManager) Reconcile(ctx context.Context, manifestFileIDs []string) {
	for _, fid := range manifestFileIDs {
		if ctx.Err() != nil {
			return
		}
		if m.fileExists(fid) {
			continue
		}
		m.logger.Info("reconcile: missing lookup file, pulling from peer", "file_id", fid)
		m.pullFromPeer(fid)
	}
}

// reconcileLookupFiles loads the manifest from the config store and reconciles.
func reconcileLookupFiles(ctx context.Context, cfgStore config.Store, mgr *lookupFileManager, logger *slog.Logger) {
	files, err := cfgStore.ListLookupFiles(ctx)
	if err != nil {
		logger.Error("reconcile lookup files: list from store", "error", err)
		return
	}
	if len(files) == 0 {
		return
	}
	ids := make([]string, len(files))
	for i, f := range files {
		ids[i] = f.ID.String()
	}
	mgr.Reconcile(ctx, ids)
}
