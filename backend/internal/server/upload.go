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

	"gastrolog/internal/system"
	"gastrolog/internal/home"

	"github.com/google/uuid"
)

const maxUploadSize = 256 << 20 // 256 MB

// managedFileName is the fixed filename used for all managed files on disk.
// The user-supplied name is stored only in config metadata, never in the filesystem.
const managedFileName = "data"

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
	if err := tmp.Close(); err != nil {
		http.Error(w, "flush upload: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// User-supplied filename is metadata only — the file on disk is always "data".
	displayName := filepath.Base(header.Filename) // sanitize: strip directory components
	lf, err := s.RegisterFile(r.Context(), tmpPath, displayName)
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
// (<home>/managed-files/<file_id>/data), and commits metadata to Raft.
// The user-supplied name is stored in config metadata only, never in the filesystem.
// The original file at srcPath is consumed (moved, not copied).
// If a file with the same name and SHA256 already exists, the duplicate is
// discarded and the existing entry is returned (deduplication).
func (s *Server) RegisterFile(ctx context.Context, srcPath string, name string) (system.ManagedFileConfig, error) {
	if s.homeDir == "" {
		return system.ManagedFileConfig{}, errors.New("no home directory")
	}

	displayName := name
	if displayName == "" {
		displayName = filepath.Base(srcPath)
	}

	// Compute SHA256 and size by streaming through the file.
	f, err := os.Open(filepath.Clean(srcPath)) // #nosec G703 — srcPath is from internal temp dir
	if err != nil {
		return system.ManagedFileConfig{}, fmt.Errorf("open source file: %w", err)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	_ = f.Close()
	if err != nil {
		return system.ManagedFileConfig{}, fmt.Errorf("hash source file: %w", err)
	}
	hash := hex.EncodeToString(h.Sum(nil))

	// Deduplicate: if a version with the same name and hash already exists, return it.
	existing, err := s.cfgStore.ListManagedFiles(ctx)
	if err != nil {
		return system.ManagedFileConfig{}, fmt.Errorf("list managed files: %w", err)
	}
	for _, ef := range existing {
		if ef.Name == displayName && ef.SHA256 == hash {
			_ = os.Remove(srcPath) // #nosec G703 — srcPath is from internal temp dir
			return ef, nil
		}
	}

	fileID := uuid.Must(uuid.NewV7())
	hd := home.New(s.homeDir)
	fileDir := hd.ManagedFileDir(fileID.String())
	if err := os.MkdirAll(fileDir, 0o750); err != nil { //nolint:gosec // G703: fileDir from trusted home + UUID
		return system.ManagedFileConfig{}, fmt.Errorf("create dir: %w", err)
	}

	// Store with a fixed filename — user-supplied name stays in metadata only.
	finalPath := filepath.Join(fileDir, managedFileName)
	if err := os.Rename(srcPath, finalPath); err != nil { //nolint:gosec // G703: finalPath uses constant filename, srcPath from trusted temp
		_ = os.RemoveAll(fileDir) //nolint:gosec // G703: cleanup our own dir
		return system.ManagedFileConfig{}, fmt.Errorf("move file: %w", err)
	}

	now := time.Now().UTC()
	lf := system.ManagedFileConfig{
		ID:         fileID,
		Name:       displayName,
		SHA256:     hash,
		Size:       size,
		UploadedAt: now,
	}
	if err := s.cfgStore.PutManagedFile(ctx, lf); err != nil {
		_ = os.RemoveAll(fileDir) //nolint:gosec // G703: cleanup our own dir
		return system.ManagedFileConfig{}, fmt.Errorf("commit metadata: %w", err)
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
// matched by display name. If the file is in the manifest but missing from disk,
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
			path := s.managedFilePath(files[i].ID.String())
			if _, err := os.Stat(path); err == nil {
				return path
			}
			// File is in manifest but missing from disk — try on-demand repair.
			fid := files[i].ID.String()
			if s.repairManagedFile != nil && s.repairManagedFile(fid) {
				if _, err := os.Stat(path); err == nil {
					return path
				}
			}
		}
	}
	return ""
}

// ResolveManagedFileByID returns the on-disk path for a managed file by its ID.
// Triggers on-demand repair if the file is missing from disk. Returns empty string
// if not found.
func (s *Server) ResolveManagedFileByID(ctx context.Context, fileID string) string {
	if s.homeDir == "" || fileID == "" {
		return ""
	}
	path := s.managedFilePath(fileID)
	if _, err := os.Stat(path); err == nil {
		return path
	}
	if s.repairManagedFile != nil && s.repairManagedFile(fileID) {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

// managedFilePath returns the on-disk path for a managed file by its UUID.
func (s *Server) managedFilePath(fileID string) string {
	hd := home.New(s.homeDir)
	return filepath.Join(hd.ManagedFileDir(fileID), managedFileName)
}

// registerUploadHandler adds the managed file upload endpoint to the mux.
func (s *Server) registerUploadHandler(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/managed-files/upload", s.handleManagedFileUpload)
}

// ManagedFileExists checks whether a managed file exists on disk.
func (s *Server) ManagedFileExists(fileID string) bool {
	if s.homeDir == "" {
		return false
	}
	path := s.managedFilePath(fileID)
	_, err := os.Stat(path)
	return err == nil
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
	path := s.managedFilePath(fileID)
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", nil, "", err
	}

	// Look up display name from system.
	files, listErr := s.cfgStore.ListManagedFiles(context.Background())
	displayName := managedFileName
	if listErr == nil {
		for _, mf := range files {
			if mf.ID.String() == fileID {
				displayName = mf.Name
				break
			}
		}
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

	return displayName, f, hex.EncodeToString(h.Sum(nil)), nil
}
