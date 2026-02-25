package lookup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadDB(t *testing.T) {
	// Build a minimal tar.gz containing a fake .mmdb file.
	mmdbContent := []byte("fake-mmdb-content-for-testing")
	archive := buildTarGz(t, "GeoLite2-City_20240101/GeoLite2-City.mmdb", mmdbContent)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "123456" || pass != "test-license-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/geoip/databases/GeoLite2-City/download" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		w.Write(archive)
	}))
	defer srv.Close()

	// Temporarily override the download URL by using a custom test helper.
	destDir := t.TempDir()

	// We can't easily override the hardcoded URL in DownloadDB, so test extractMMDB directly
	// and also do an integration test with a patched approach.
	t.Run("extractMMDB", func(t *testing.T) {
		tmpDir := t.TempDir()
		path, err := extractMMDB(bytes.NewReader(archive), tmpDir, "GeoLite2-City")
		if err != nil {
			t.Fatalf("extractMMDB: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read extracted file: %v", err)
		}
		if !bytes.Equal(got, mmdbContent) {
			t.Fatalf("content mismatch: got %q, want %q", got, mmdbContent)
		}
	})

	t.Run("DownloadDB_integration", func(t *testing.T) {
		// Use downloadDBWithURL to test full flow with our test server.
		err := downloadDBWithURL(context.Background(), "123456", "test-license-key", "GeoLite2-City", destDir, srv.URL)
		if err != nil {
			t.Fatalf("downloadDBWithURL: %v", err)
		}
		finalPath := filepath.Join(destDir, "GeoLite2-City.mmdb")
		got, err := os.ReadFile(finalPath)
		if err != nil {
			t.Fatalf("read final file: %v", err)
		}
		if !bytes.Equal(got, mmdbContent) {
			t.Fatalf("content mismatch: got %q, want %q", got, mmdbContent)
		}
	})

	t.Run("DownloadDB_bad_auth", func(t *testing.T) {
		err := downloadDBWithURL(context.Background(), "wrong", "wrong", "GeoLite2-City", t.TempDir(), srv.URL)
		if err == nil {
			t.Fatal("expected error for bad auth")
		}
	})
}

func buildTarGz(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Size: int64(len(content)),
		Mode: 0o644,
	}); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatalf("write tar content: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return buf.Bytes()
}
