package server

import (
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

	hd := home.New(s.homeDir)
	lookupsDir := hd.LookupsDir()
	if err := os.MkdirAll(lookupsDir, 0o750); err != nil {
		http.Error(w, "create lookups directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Stream to temp file, computing SHA256 along the way.
	tmp, err := os.CreateTemp(lookupsDir, ".upload-*")
	if err != nil {
		http.Error(w, "create temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) // clean up on any error path; no-op after rename
	}()

	h := sha256.New()
	size, err := io.Copy(tmp, io.TeeReader(file, h))
	if err != nil {
		http.Error(w, "write upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_ = tmp.Close()

	hash := hex.EncodeToString(h.Sum(nil))
	fileID := uuid.Must(uuid.NewV7())

	// Move to final location: <home>/lookups/<file_id>/<original_filename>
	fileDir := hd.LookupFileDir(fileID.String())
	if err := os.MkdirAll(fileDir, 0o750); err != nil {
		http.Error(w, "create file directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	finalPath := filepath.Join(fileDir, filepath.Base(header.Filename))
	if err := os.Rename(tmpPath, finalPath); err != nil { //nolint:gosec // G703: paths constructed from trusted home dir + filepath.Base
		http.Error(w, "move upload: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Commit metadata to Raft.
	now := time.Now().UTC()
	lf := config.LookupFileConfig{
		ID:         fileID,
		Name:       filepath.Base(header.Filename),
		SHA256:     hash,
		Size:       size,
		UploadedAt: now,
	}
	if err := s.cfgStore.PutLookupFile(r.Context(), lf); err != nil {
		_ = os.RemoveAll(fileDir) // best-effort cleanup on Raft failure
		http.Error(w, "save metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.configSignal.Notify()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{ //nolint:errchkjson // best-effort response encoding
		"file_id":     fileID.String(),
		"name":        lf.Name,
		"sha256":      lf.SHA256,
		"size":        lf.Size,
		"uploaded_at": now.Format(time.RFC3339),
	})
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
	if err := os.RemoveAll(dir); err != nil {
		s.logger.Warn("cleanup lookup file", "file_id", fileID, "error", err)
	} else {
		s.logger.Info("removed lookup file", "file_id", fileID, "dir", dir)
	}
}

// lookupFileExists checks whether a lookup file exists on disk.
func (s *Server) lookupFileExists(fileID string) bool {
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

// lookupFileIDs returns the IDs of lookup files present on disk.
func (s *Server) lookupFileIDs() []string {
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

// lookupFileReader opens a lookup file for reading.
func (s *Server) lookupFileReader(fileID string) (name string, rc io.ReadCloser, sha256hex string, err error) {
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
