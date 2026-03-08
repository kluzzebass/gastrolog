package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gastrolog/internal/config"
	"gastrolog/internal/home"

	"github.com/google/uuid"
)

const maxUploadSize = 256 << 20 // 256 MB

// handleManagedFileUpload handles multipart file uploads for managed files.
// The file is streamed to disk (never buffered in heap), then metadata is
// committed to Raft so all cluster nodes learn about the file.
func (s *Server) handleManagedFileUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Auth check: verify JWT from Authorization header (unless noAuth mode).
	if !s.noAuth && s.tokens != nil {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if _, err := s.tokens.Verify(token); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		http.Error(w, "file too large or invalid multipart form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing 'file' field in multipart form", http.StatusBadRequest)
		return
	}
	defer file.Close() //nolint:errcheck // best-effort close on multipart file

	// Stream the upload to a temp file.
	hd := home.New(s.homeDir)
	managedFilesDir := hd.ManagedFilesDir()
	if err := os.MkdirAll(managedFilesDir, 0o750); err != nil {
		http.Error(w, "create managed files directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	tmp, err := os.CreateTemp(managedFilesDir, ".upload-*")
	if err != nil {
		http.Error(w, "create temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // no-op after rename

	if _, err := io.Copy(tmp, file); err != nil {
		_ = tmp.Close()
		http.Error(w, "write upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_ = tmp.Close()

	// Give the temp file the original filename so RegisterFile can use it.
	namedTmp := filepath.Join(managedFilesDir, ".upload-"+filepath.Base(header.Filename))
	if err := os.Rename(tmpPath, namedTmp); err != nil { //nolint:gosec // trusted paths
		http.Error(w, "rename temp: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = os.Remove(namedTmp) }() // no-op after RegisterFile moves it

	lf, err := s.RegisterFile(r.Context(), namedTmp, header.Filename)
	if err != nil {
		http.Error(w, "register file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{ //nolint:errchkjson // best-effort response encoding
		"file_id":     lf.ID.String(),
		"name":        lf.Name,
		"sha256":      lf.SHA256,
		"size":        lf.Size,
		"uploaded_at": lf.UploadedAt.Format(time.RFC3339),
	})
}

// RegisterFile registers a local file as a managed file entity. It computes the
// SHA256 hash, moves the file into the managed directory structure
// (<home>/managed-files/<file_id>/<filename>), and commits metadata to Raft.
// The original file at srcPath is consumed (moved, not copied).
// If a file with the same name and SHA256 already exists, the duplicate is
// discarded and the existing entry is returned (deduplication).
// If name is empty, the base name of srcPath is used.
func (s *Server) RegisterFile(ctx context.Context, srcPath string, name string) (config.ManagedFileConfig, error) {
	if s.homeDir == "" {
		return config.ManagedFileConfig{}, errors.New("no home directory")
	}

	filename := name
	if filename == "" {
		filename = filepath.Base(srcPath)
	}

	// Compute SHA256 and size by streaming through the file.
	f, err := os.Open(srcPath) //nolint:gosec // path from trusted caller
	if err != nil {
		return config.ManagedFileConfig{}, fmt.Errorf("open %s: %w", srcPath, err)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	_ = f.Close()
	if err != nil {
		return config.ManagedFileConfig{}, fmt.Errorf("hash %s: %w", srcPath, err)
	}
	hash := hex.EncodeToString(h.Sum(nil))

	// Deduplicate: if a version with the same name and hash already exists, return it.
	existing, err := s.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		return config.ManagedFileConfig{}, fmt.Errorf("list managed files: %w", err)
	}
	for _, ef := range existing {
		if ef.Name == filename && ef.SHA256 == hash {
			_ = os.Remove(srcPath) //nolint:gosec,nolintlint // G703: srcPath from trusted caller
			return ef, nil
		}
	}

	fileID := uuid.Must(uuid.NewV7())
	hd := home.New(s.homeDir)
	fileDir := hd.ManagedFileDir(fileID.String())
	if err := os.MkdirAll(fileDir, 0o750); err != nil { //nolint:gosec // path from trusted home dir + UUID
		return config.ManagedFileConfig{}, fmt.Errorf("create dir: %w", err)
	}

	finalPath := filepath.Join(fileDir, filename)
	if err := os.Rename(srcPath, finalPath); err != nil { //nolint:gosec // trusted caller paths
		_ = os.RemoveAll(fileDir) //nolint:gosec // cleanup our own dir
		return config.ManagedFileConfig{}, fmt.Errorf("move file: %w", err)
	}

	now := time.Now().UTC()
	lf := config.ManagedFileConfig{
		ID:         fileID,
		Name:       filename,
		SHA256:     hash,
		Size:       size,
		UploadedAt: now,
	}
	if err := s.cfgStore.PutManagedFile(ctx, lf); err != nil {
		_ = os.RemoveAll(fileDir) //nolint:gosec // cleanup our own dir
		return config.ManagedFileConfig{}, fmt.Errorf("commit metadata: %w", err)
	}
	s.configSignal.Notify()

	return lf, nil
}

// SetManagedFileRepair sets the on-demand repair callback. When a managed file
// is in the manifest but missing from local disk, this function is called to
// pull it from a peer before returning "not found".
func (s *Server) SetManagedFileRepair(fn func(fileID string) bool) {
	s.repairManagedFile = fn
}

// ResolveManagedFilePath returns the on-disk path for a managed file entity
// matched by filename. If the file is in the manifest but missing from disk,
// it triggers an on-demand repair (pull from peer) before giving up.
// Returns empty string if not found.
func (s *Server) ResolveManagedFilePath(ctx context.Context, filename string) string {
	if s.homeDir == "" {
		return ""
	}
	files, err := s.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		return ""
	}
	// Walk in reverse order (UUIDv7 sorted = creation order) to prefer the latest.
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].Name == filename {
			hd := home.New(s.homeDir)
			fid := files[i].ID.String()
			dir := hd.ManagedFileDir(fid)
			path := filepath.Join(dir, filename)
			if _, err := os.Stat(path); err == nil {
				return path
			}
			// File is in manifest but missing from disk — try on-demand repair.
			if s.repairManagedFile != nil && s.repairManagedFile(fid) {
				if _, err := os.Stat(path); err == nil {
					return path
				}
			}
		}
	}
	return ""
}

// registerUploadHandler adds the managed file upload endpoint to the mux.
func (s *Server) registerUploadHandler(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/managed-files/upload", s.handleManagedFileUpload)
}

// handleManagedFileDelete is the local-disk cleanup triggered by FSM notification.
// Called on every node (including the uploader) when a managed file is deleted.
func (s *Server) cleanupManagedFile(fileID uuid.UUID) {
	if s.homeDir == "" {
		return
	}
	hd := home.New(s.homeDir)
	dir := hd.ManagedFileDir(fileID.String())
	if err := os.RemoveAll(dir); err != nil { //nolint:gosec,nolintlint // G703: trusted UUID path
		s.logger.Warn("cleanup managed file", "file_id", fileID, "error", err)
	} else {
		s.logger.Info("removed managed file", "file_id", fileID, "dir", dir)
	}
}

// ManagedFileExists checks whether a managed file exists on disk.
func (s *Server) ManagedFileExists(fileID string) bool {
	if s.homeDir == "" {
		return false
	}
	hd := home.New(s.homeDir)
	dir := hd.ManagedFileDir(fileID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	return len(entries) > 0
}

// ManagedFileIDs returns the IDs of managed files present on disk.
func (s *Server) ManagedFileIDs() []string {
	if s.homeDir == "" {
		return nil
	}
	hd := home.New(s.homeDir)
	entries, err := os.ReadDir(hd.ManagedFilesDir())
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			ids = append(ids, e.Name())
		}
	}
	return ids
}

// ManagedFileReader opens a managed file for reading.
func (s *Server) ManagedFileReader(fileID string) (name string, rc io.ReadCloser, sha256hex string, err error) {
	if s.homeDir == "" {
		return "", nil, "", errors.New("no home directory")
	}
	hd := home.New(s.homeDir)
	dir := hd.ManagedFileDir(fileID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, "", fmt.Errorf("read dir: %w", err)
	}
	if len(entries) == 0 {
		return "", nil, "", errors.New("empty file directory")
	}

	fname := entries[0].Name()
	path := filepath.Join(dir, fname)
	f, err := os.Open(path) //nolint:gosec // path constructed from trusted home dir
	if err != nil {
		return "", nil, "", err
	}

	// Compute SHA256.
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		_ = f.Close()
		return "", nil, "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return "", nil, "", err
	}

	return fname, f, hex.EncodeToString(h.Sum(nil)), nil
}
