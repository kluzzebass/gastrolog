package docker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// state persists the last-seen timestamp per container across restarts.
type state struct {
	Containers map[string]containerBookmark `json:"containers"`
}

type containerBookmark struct {
	LastTimestamp time.Time `json:"last_timestamp"`
}

// loadState reads bookmark state from the given path.
// Returns empty state if the file doesn't exist.
func loadState(path string) (state, error) {
	s := state{Containers: make(map[string]containerBookmark)}
	if path == "" {
		return s, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return s, err
	}

	if err := json.Unmarshal(data, &s); err != nil {
		return state{Containers: make(map[string]containerBookmark)}, nil
	}
	if s.Containers == nil {
		s.Containers = make(map[string]containerBookmark)
	}
	return s, nil
}

// saveState atomically writes bookmark state to the given path.
func saveState(path string, s state) error {
	if path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data, err := json.Marshal(s)
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
