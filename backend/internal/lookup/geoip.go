package lookup

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/oschwald/maxminddb-golang"
)

// GeoIPInfo describes a loaded MMDB database.
type GeoIPInfo struct {
	DatabaseType string
	BuildTime    time.Time
}

// mmdbRecord contains the fields we decode from a GeoLite2-City / GeoIP2-City MMDB file.
type mmdbRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
	Subdivisions []struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	Location struct {
		Latitude       float64 `maxminddb:"latitude"`
		Longitude      float64 `maxminddb:"longitude"`
		TimeZone       string  `maxminddb:"time_zone"`
		AccuracyRadius uint16  `maxminddb:"accuracy_radius"`
	} `maxminddb:"location"`
}

// GeoIP is a lookup table backed by a MaxMind MMDB file.
// It maps IP addresses to geographic metadata (country, city).
// Safe for concurrent use; the reader is swapped atomically.
type GeoIP struct {
	reader atomic.Pointer[maxminddb.Reader]

	mu        sync.Mutex
	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

// NewGeoIP creates a GeoIP lookup table. Starts empty (nil reader);
// Lookup returns nil until a database is loaded via Load.
func NewGeoIP() *GeoIP {
	return &GeoIP{}
}

// Suffixes returns the output suffixes this table produces.
func (g *GeoIP) Suffixes() []string {
	return []string{"country", "city", "subdivision", "latitude", "longitude", "timezone", "accuracy_radius"}
}

// Lookup resolves an IP address to geographic metadata.
// Returns nil on miss, parse error, or if no database is loaded.
func (g *GeoIP) Lookup(_ context.Context, value string) map[string]string {
	r := g.reader.Load()
	if r == nil {
		return nil
	}

	ip := net.ParseIP(value)
	if ip == nil {
		return nil
	}

	var rec mmdbRecord
	if err := r.Lookup(ip, &rec); err != nil {
		return nil
	}

	out := make(map[string]string, 7)
	if rec.Country.ISOCode != "" {
		out["country"] = rec.Country.ISOCode
	}
	if name := rec.City.Names["en"]; name != "" {
		out["city"] = name
	}
	if len(rec.Subdivisions) > 0 {
		if name := rec.Subdivisions[0].Names["en"]; name != "" {
			out["subdivision"] = name
		} else if rec.Subdivisions[0].ISOCode != "" {
			out["subdivision"] = rec.Subdivisions[0].ISOCode
		}
	}
	if rec.Location.Latitude != 0 || rec.Location.Longitude != 0 {
		out["latitude"] = strconv.FormatFloat(rec.Location.Latitude, 'f', 4, 64)
		out["longitude"] = strconv.FormatFloat(rec.Location.Longitude, 'f', 4, 64)
	}
	if rec.Location.TimeZone != "" {
		out["timezone"] = rec.Location.TimeZone
	}
	if rec.Location.AccuracyRadius > 0 {
		out["accuracy_radius"] = strconv.FormatUint(uint64(rec.Location.AccuracyRadius), 10)
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// Load opens an MMDB file and swaps the atomic reader pointer.
// The old reader is closed after the swap. Returns metadata about
// the loaded database on success.
func (g *GeoIP) Load(path string) (GeoIPInfo, error) {
	r, err := maxminddb.Open(path)
	if err != nil {
		return GeoIPInfo{}, fmt.Errorf("open mmdb %q: %w", path, err)
	}
	info := GeoIPInfo{
		DatabaseType: r.Metadata.DatabaseType,
		BuildTime:    time.Unix(int64(r.Metadata.BuildEpoch), 0), //nolint:gosec // BuildEpoch is a uint, safe for unix timestamps
	}
	old := g.reader.Swap(r)
	if old != nil {
		_ = old.Close()
	}
	return info, nil
}

// WatchFile watches an MMDB file for changes using fsnotify.
// On write/create events, it reloads the database via Load.
// Calling WatchFile again replaces the previous watch.
func (g *GeoIP) WatchFile(path string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Stop any existing watcher.
	g.stopWatchLocked()

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	if err := w.Add(path); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %q: %w", path, err)
	}

	g.watcher = w
	g.watchPath = path
	g.watchDone = make(chan struct{})

	go g.watchLoop(w, path, g.watchDone)
	return nil
}

func (g *GeoIP) watchLoop(w *fsnotify.Watcher, path string, done chan struct{}) {
	defer close(done)
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				_, _ = g.Load(path)
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
		}
	}
}

func (g *GeoIP) stopWatchLocked() {
	if g.watcher != nil {
		_ = g.watcher.Close()
		<-g.watchDone
		g.watcher = nil
		g.watchPath = ""
		g.watchDone = nil
	}
}

// Close stops the file watcher and closes the current MMDB reader.
func (g *GeoIP) Close() {
	g.mu.Lock()
	g.stopWatchLocked()
	g.mu.Unlock()

	if r := g.reader.Swap(nil); r != nil {
		_ = r.Close()
	}
}
