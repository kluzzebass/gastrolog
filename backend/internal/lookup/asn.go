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

// ASNInfo describes a loaded ASN MMDB database.
type ASNInfo struct {
	DatabaseType string
	BuildTime    time.Time
}

// asnRecord contains only the fields we decode from a GeoLite2-ASN / GeoIP2-ASN MMDB file.
type asnRecord struct {
	Number       uint   `maxminddb:"autonomous_system_number"`
	Organization string `maxminddb:"autonomous_system_organization"`
}

// ASN is a lookup table backed by a MaxMind ASN MMDB file.
// It maps IP addresses to autonomous system metadata (ASN, AS organization).
// Safe for concurrent use; the reader is swapped atomically.
type ASN struct {
	reader atomic.Pointer[maxminddb.Reader]

	mu        sync.Mutex
	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

// NewASN creates an ASN lookup table. Starts empty (nil reader);
// Lookup returns nil until a database is loaded via Load.
func NewASN() *ASN {
	return &ASN{}
}

// Suffixes returns the output suffixes this table produces.
func (a *ASN) Suffixes() []string {
	return []string{"asn", "as_org"}
}

// Lookup resolves an IP address to ASN metadata.
// Returns nil on miss, parse error, or if no database is loaded.
func (a *ASN) Lookup(_ context.Context, value string) map[string]string {
	r := a.reader.Load()
	if r == nil {
		return nil
	}

	ip := net.ParseIP(value)
	if ip == nil {
		return nil
	}

	var rec asnRecord
	if err := r.Lookup(ip, &rec); err != nil {
		return nil
	}

	out := make(map[string]string, 2)
	if rec.Number != 0 {
		out["asn"] = "AS" + strconv.FormatUint(uint64(rec.Number), 10)
	}
	if rec.Organization != "" {
		out["as_org"] = rec.Organization
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// Load opens an ASN MMDB file and swaps the atomic reader pointer.
// The old reader is closed after the swap. Returns metadata about
// the loaded database on success.
func (a *ASN) Load(path string) (ASNInfo, error) {
	r, err := maxminddb.Open(path)
	if err != nil {
		return ASNInfo{}, fmt.Errorf("open mmdb %q: %w", path, err)
	}
	info := ASNInfo{
		DatabaseType: r.Metadata.DatabaseType,
		BuildTime:    time.Unix(int64(r.Metadata.BuildEpoch), 0), //nolint:gosec // BuildEpoch is a uint, safe for unix timestamps
	}
	old := a.reader.Swap(r)
	if old != nil {
		_ = old.Close()
	}
	return info, nil
}

// WatchFile watches an MMDB file for changes using fsnotify.
// On write/create events, it reloads the database via Load.
// Calling WatchFile again replaces the previous watch.
func (a *ASN) WatchFile(path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Stop any existing watcher.
	a.stopWatchLocked()

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	if err := w.Add(path); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %q: %w", path, err)
	}

	a.watcher = w
	a.watchPath = path
	a.watchDone = make(chan struct{})

	go a.watchLoop(w, path, a.watchDone)
	return nil
}

func (a *ASN) watchLoop(w *fsnotify.Watcher, path string, done chan struct{}) {
	defer close(done)
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				_, _ = a.Load(path)
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
		}
	}
}

func (a *ASN) stopWatchLocked() {
	if a.watcher != nil {
		_ = a.watcher.Close()
		<-a.watchDone
		a.watcher = nil
		a.watchPath = ""
		a.watchDone = nil
	}
}

// Close stops the file watcher and closes the current MMDB reader.
func (a *ASN) Close() {
	a.mu.Lock()
	a.stopWatchLocked()
	a.mu.Unlock()

	if r := a.reader.Swap(nil); r != nil {
		_ = r.Close()
	}
}
