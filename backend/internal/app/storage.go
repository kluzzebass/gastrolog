package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"gastrolog/internal/config"
)

// parseStorageFlags parses --storage flag values into StorageArea structs.
//
// Format: path:class=N[:label=Name]
//
// Windows paths (e.g. C:\data) are handled: the first segment is always the
// path (which may contain a drive letter colon), then key=value pairs follow.
func parseStorageFlags(flags []string) ([]config.StorageArea, error) {
	areas := make([]config.StorageArea, 0, len(flags))
	for _, raw := range flags {
		area, err := parseOneStorageFlag(raw)
		if err != nil {
			return nil, fmt.Errorf("flag %q: %w", raw, err)
		}
		areas = append(areas, area)
	}
	return areas, nil
}

func parseOneStorageFlag(raw string) (config.StorageArea, error) {
	// Split on ":" — but re-join the first segment if it looks like a
	// Windows drive letter (single ASCII letter followed by rest-of-path).
	parts := strings.Split(raw, ":")
	if len(parts) < 2 {
		return config.StorageArea{}, errors.New("need at least path:class=N")
	}

	// Detect Windows drive letter: first segment is a single letter,
	// second segment starts with `\` or `/`.
	path := parts[0]
	kvStart := 1
	if len(parts) >= 3 && len(parts[0]) == 1 && isASCIILetter(parts[0][0]) &&
		len(parts[1]) > 0 && (parts[1][0] == '\\' || parts[1][0] == '/') {
		path = parts[0] + ":" + parts[1]
		kvStart = 2
	}

	var (
		classSet bool
		class    uint32
		label    string
	)

	for _, kv := range parts[kvStart:] {
		key, val, ok := strings.Cut(kv, "=")
		if !ok {
			return config.StorageArea{}, fmt.Errorf("invalid key=value pair %q", kv)
		}
		switch strings.ToLower(key) {
		case "class":
			n, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return config.StorageArea{}, fmt.Errorf("invalid class %q: %w", val, err)
			}
			class = uint32(n)
			classSet = true
		case "label":
			label = val
		default:
			return config.StorageArea{}, fmt.Errorf("unknown key %q", key)
		}
	}

	if !classSet {
		return config.StorageArea{}, errors.New("class=N is required")
	}

	if label == "" {
		label = filepath.Base(path)
	}

	return config.StorageArea{
		ID:           uuid.Must(uuid.NewV7()),
		StorageClass: class,
		Label:        label,
		Path:         path,
	}, nil
}

func isASCIILetter(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

// registerNodeStorage overwrites the node's storage configuration with the
// given areas. This runs on every startup when --storage flags are present,
// so the config always reflects the current CLI invocation.
func registerNodeStorage(ctx context.Context, cfgStore config.Store, nodeID string, areas []config.StorageArea) error {
	return cfgStore.SetNodeStorageConfig(ctx, config.NodeStorageConfig{
		NodeID: nodeID,
		Areas:  areas,
	})
}
