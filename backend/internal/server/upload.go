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

// handleLookupFileUpload handles multipart file uploads for lookup tables.
// The file is streamed to disk (never buffered in heap), then metadata is
// committed to Raft so all cluster nodes learn about the file.
func (s *Server) handleLookupFileUpload(w http.ResponseWriter, r *http.Request) {
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
	lookupsDir := hd.LookupsDir()
	if err := os.MkdirAll(lookupsDir, 0o750); err != nil {
		http.Error(w, "create lookups directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	tmp, err := os.CreateTemp(lookupsDir, ".upload-*")
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
	namedTmp := filepath.Join(lookupsDir, ".upload-"+filepath.Base(header.Filename))
	if err := os.Rename(tmpPath, namedTmp); err != nil { //nolint:gosec // trusted paths
		http.Error(w, "rename temp: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = os.Remove(namedTmp) }() // no-op after RegisterFile moves it

	lf, err := s.RegisterFile(r.Context(), namedTmp)
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

// RegisterFile registers a local file as a lookup file entity. It computes the
// SHA256 hash, moves the file into the managed directory structure
// (<home>/lookups/<file_id>/<filename>), and commits metadata to Raft.
// The original file at srcPath is consumed (moved, not copied).
// If a lookup file with the same name already exists, it is replaced.
func (s *Server) RegisterFile(ctx context.Context, srcPath string) (config.LookupFileConfig, error) {
	if s.homeDir == "" {
		return config.LookupFileConfig{}, errors.New("no home directory")
	}

	filename := filepath.Base(srcPath)

	// Compute SHA256 and size by streaming through the file.
	f, err := os.Open(srcPath) //nolint:gosec // path from trusted caller
	if err != nil {
		return config.LookupFileConfig{}, fmt.Errorf("open %s: %w", srcPath, err)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	_ = f.Close()
	if err != nil {
		return config.LookupFileConfig{}, fmt.Errorf("hash %s: %w", srcPath, err)
	}
	hash := hex.EncodeToString(h.Sum(nil))

	// Delete any existing lookup file with the same name (replacement upload).
	if err := s.deleteExistingByName(ctx, filename); err != nil {
		return config.LookupFileConfig{}, fmt.Errorf("delete existing: %w", err)
	}

	fileID := uuid.Must(uuid.NewV7())
	hd := home.New(s.homeDir)
	fileDir := hd.LookupFileDir(fileID.String())
	if err := os.MkdirAll(fileDir, 0o750); err != nil { //nolint:gosec // path from trusted home dir + UUID
		return config.LookupFileConfig{}, fmt.Errorf("create dir: %w", err)
	}

	finalPath := filepath.Join(fileDir, filename)
	if err := os.Rename(srcPath, finalPath); err != nil { //nolint:gosec // trusted caller paths
		_ = os.RemoveAll(fileDir) //nolint:gosec // cleanup our own dir
		return config.LookupFileConfig{}, fmt.Errorf("move file: %w", err)
	}

	now := time.Now().UTC()
	lf := config.LookupFileConfig{
		ID:         fileID,
		Name:       filename,
		SHA256:     hash,
		Size:       size,
		UploadedAt: now,
	}
	if err := s.cfgStore.PutLookupFile(ctx, lf); err != nil {
		_ = os.RemoveAll(fileDir) //nolint:gosec // cleanup our own dir
		return config.LookupFileConfig{}, fmt.Errorf("commit metadata: %w", err)
	}
	s.configSignal.Notify()

	return lf, nil
}

// deleteExistingByName removes lookup file entities whose filename matches,
// so replacement uploads don't leave orphans in the manifest.
func (s *Server) deleteExistingByName(ctx context.Context, filename string) error {
	files, err := s.cfgStore.ListLookupFiles(ctx)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.Name == filename {
			if err := s.cfgStore.DeleteLookupFile(ctx, f.ID); err != nil {
				return err
			}
			// Clean up disk for the old file.
			s.cleanupLookupFile(f.ID)
		}
	}
	return nil
}

// SetLookupFileRepair sets the on-demand repair callback. When a lookup file
// is in the manifest but missing from local disk, this function is called to
// pull it from a peer before returning "not found".
func (s *Server) SetLookupFileRepair(fn func(fileID string) bool) {
	s.repairLookupFile = fn
}

// ResolveLookupFilePath returns the on-disk path for a lookup file entity
// matched by filename. If the file is in the manifest but missing from disk,
// it triggers an on-demand repair (pull from peer) before giving up.
// Returns empty string if not found.
func (s *Server) ResolveLookupFilePath(ctx context.Context, filename string) string {
	if s.homeDir == "" {
		return ""
	}
	files, err := s.cfgStore.ListLookupFiles(ctx)
	if err != nil {
		return ""
	}
	// Walk in reverse order (UUIDv7 sorted = creation order) to prefer the latest.
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].Name == filename {
			hd := home.New(s.homeDir)
			fid := files[i].ID.String()
			dir := hd.LookupFileDir(fid)
			path := filepath.Join(dir, filename)
			if _, err := os.Stat(path); err == nil {
				return path
			}
			// File is in manifest but missing from disk — try on-demand repair.
			if s.repairLookupFile != nil && s.repairLookupFile(fid) {
				if _, err := os.Stat(path); err == nil {
					return path
				}
			}
		}
	}
	return ""
}

// registerUploadHandler adds the lookup file upload endpoint to the mux.
func (s *Server) registerUploadHandler(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/lookup-files/upload", s.handleLookupFileUpload)
}

// handleLookupFileDelete is the local-disk cleanup triggered by FSM notification.
// Called on every node (including the uploader) when a lookup file is deleted.
func (s *Server) cleanupLookupFile(fileID uuid.UUID) {
	if s.homeDir == "" {
		return
	}
	hd := home.New(s.homeDir)
	dir := hd.LookupFileDir(fileID.String())
	if err := os.RemoveAll(dir); err != nil { //nolint:gosec,nolintlint // G703: trusted UUID path
		s.logger.Warn("cleanup lookup file", "file_id", fileID, "error", err)
	} else {
		s.logger.Info("removed lookup file", "file_id", fileID, "dir", dir)
	}
}

// LookupFileExists checks whether a lookup file exists on disk.
func (s *Server) LookupFileExists(fileID string) bool {
	if s.homeDir == "" {
		return false
	}
	hd := home.New(s.homeDir)
	dir := hd.LookupFileDir(fileID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	return len(entries) > 0
}

// LookupFileIDs returns the IDs of lookup files present on disk.
func (s *Server) LookupFileIDs() []string {
	if s.homeDir == "" {
		return nil
	}
	hd := home.New(s.homeDir)
	entries, err := os.ReadDir(hd.LookupsDir())
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

// LookupFileReader opens a lookup file for reading.
func (s *Server) LookupFileReader(fileID string) (name string, rc io.ReadCloser, sha256hex string, err error) {
	if s.homeDir == "" {
		return "", nil, "", errors.New("no home directory")
	}
	hd := home.New(s.homeDir)
	dir := hd.LookupFileDir(fileID)
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
