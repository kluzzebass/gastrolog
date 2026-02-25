package lookup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const maxmindBaseURL = "https://download.maxmind.com"

// DownloadDB downloads a MaxMind database edition (e.g. "GeoLite2-City") to destDir.
// It authenticates with HTTP Basic Auth (account ID + license key), downloads the
// tar.gz archive, extracts the .mmdb file, and atomically renames it into place.
func DownloadDB(ctx context.Context, accountID, licenseKey, edition, destDir string) error {
	return downloadDBWithURL(ctx, accountID, licenseKey, edition, destDir, maxmindBaseURL)
}

// downloadDBWithURL is the testable implementation that accepts a custom base URL.
func downloadDBWithURL(ctx context.Context, accountID, licenseKey, edition, destDir, baseURL string) error {
	dlURL := baseURL + "/geoip/databases/" + edition + "/download?suffix=tar.gz"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dlURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(accountID, licenseKey)

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // URL built from trusted server config, not user input
	if err != nil {
		return fmt.Errorf("download %s: %w", edition, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: HTTP %d", edition, resp.StatusCode)
	}

	// Stream response to a temp file so we don't hold the whole archive in memory.
	tmpArchive, err := os.CreateTemp(destDir, edition+"-*.tar.gz")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpArchivePath := tmpArchive.Name()
	defer func() { _ = os.Remove(tmpArchivePath) }()

	if _, err := io.Copy(tmpArchive, resp.Body); err != nil {
		_ = tmpArchive.Close()
		return fmt.Errorf("write archive: %w", err)
	}

	// Seek back to beginning for extraction.
	if _, err := tmpArchive.Seek(0, io.SeekStart); err != nil {
		_ = tmpArchive.Close()
		return fmt.Errorf("seek archive: %w", err)
	}

	mmdbPath, err := extractMMDB(tmpArchive, destDir, edition)
	_ = tmpArchive.Close()
	if err != nil {
		return err
	}

	// Atomic rename to final location.
	finalPath := filepath.Join(destDir, edition+".mmdb")
	if err := os.Rename(mmdbPath, finalPath); err != nil { //nolint:gosec // paths from our own temp dir
		_ = os.Remove(mmdbPath) //nolint:gosec // path from our own temp file
		return fmt.Errorf("rename to final path: %w", err)
	}

	return nil
}

// extractMMDB extracts the first .mmdb file from a tar.gz archive into destDir
// as a temp file and returns its path.
func extractMMDB(r io.Reader, destDir, edition string) (string, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return "", fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("read tar: %w", err)
		}

		if !strings.HasSuffix(hdr.Name, ".mmdb") {
			continue
		}

		tmpFile, err := os.CreateTemp(destDir, edition+"-*.mmdb")
		if err != nil {
			return "", fmt.Errorf("create temp mmdb: %w", err)
		}

		if _, err := io.Copy(tmpFile, io.LimitReader(tr, 256<<20)); err != nil { // 256 MiB limit
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name()) //nolint:gosec // path from our own temp file
			return "", fmt.Errorf("extract mmdb: %w", err)
		}

		if err := tmpFile.Close(); err != nil {
			_ = os.Remove(tmpFile.Name()) //nolint:gosec // path from our own temp file
			return "", fmt.Errorf("close temp mmdb: %w", err)
		}

		return tmpFile.Name(), nil
	}

	return "", fmt.Errorf("no .mmdb file found in %s archive", edition)
}
