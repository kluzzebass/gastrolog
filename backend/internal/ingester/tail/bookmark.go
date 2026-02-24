package tail

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// bookmarks persists file positions across restarts.
type bookmarks struct {
	Files map[string]fileBookmark `json:"files"`
}

type fileBookmark struct {
	Inode  uint64 `json:"inode"`
	Offset int64  `json:"offset"`
}

// loadBookmarks reads bookmark state from the given path.
// Returns empty bookmarks if the file doesn't exist.
func loadBookmarks(path string) (bookmarks, error) {
	b := bookmarks{Files: make(map[string]fileBookmark)}
	if path == "" {
		return b, nil
	}

	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return b, nil
		}
		return b, err
	}

	if err := json.Unmarshal(data, &b); err != nil {
		// Corrupt bookmark file; start fresh rather than failing.
		return bookmarks{Files: make(map[string]fileBookmark)}, nil //nolint:nilerr // corrupt bookmark file is treated as empty state
	}
	if b.Files == nil {
		b.Files = make(map[string]fileBookmark)
	}
	return b, nil
}

// saveBookmarks atomically writes bookmark state to the given path.
func saveBookmarks(path string, b bookmarks) error {
	if path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}

	data, err := json.Marshal(b)
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
