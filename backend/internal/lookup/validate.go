package lookup

import (
	"time"

	"github.com/oschwald/maxminddb-golang"
)

// MmdbInfo holds metadata from a validated MMDB file.
type MmdbInfo struct {
	DatabaseType string
	BuildTime    time.Time
	NodeCount    uint
}

// ValidateMMDB opens an MMDB file, reads its metadata, and closes it.
// Used for user-facing validation without loading into the live lookup table.
func ValidateMMDB(path string) (MmdbInfo, error) {
	r, err := maxminddb.Open(path)
	if err != nil {
		return MmdbInfo{}, err
	}
	defer func() { _ = r.Close() }()

	return MmdbInfo{
		DatabaseType: r.Metadata.DatabaseType,
		BuildTime:    time.Unix(int64(r.Metadata.BuildEpoch), 0), //nolint:gosec // BuildEpoch is a uint, safe for unix timestamps
		NodeCount:    r.Metadata.NodeCount,
	}, nil
}
