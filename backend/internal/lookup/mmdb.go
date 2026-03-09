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

// MMDBInfo describes a loaded MMDB database.
type MMDBInfo struct {
	DatabaseType string
	BuildTime    time.Time
}

// decodeFunc extracts key→value pairs from an MMDB reader for a given IP.
type decodeFunc func(r *maxminddb.Reader, ip net.IP) map[string]string

// MMDB is a lookup table backed by a MaxMind MMDB file.
// The dbType (passed at construction) selects which fields are decoded.
// Safe for concurrent use; the reader is swapped atomically.
type MMDB struct {
	dbType   string
	suffixes []string
	decode   decodeFunc

	reader atomic.Pointer[maxminddb.Reader]

	mu        sync.Mutex
	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

// NewMMDB creates an MMDB lookup table for the given database type.
// Supported types: "city", "asn". Starts empty; LookupValues returns nil
// until a database is loaded via Load.
func NewMMDB(dbType string) *MMDB {
	m := &MMDB{dbType: dbType}
	switch dbType {
	case "city":
		m.suffixes = []string{"country", "city", "subdivision", "latitude", "longitude", "timezone", "accuracy_radius"}
		m.decode = decodeCity
	case "asn":
		m.suffixes = []string{"asn", "as_org"}
		m.decode = decodeASN
	default:
		m.suffixes = nil
		m.decode = func(*maxminddb.Reader, net.IP) map[string]string { return nil }
	}
	return m
}

// DBType returns the database type this table was created for.
func (m *MMDB) DBType() string { return m.dbType }

// Parameters returns the single input parameter name.
func (m *MMDB) Parameters() []string { return []string{"value"} }

// Suffixes returns the output suffixes this table produces.
func (m *MMDB) Suffixes() []string { return m.suffixes }

// LookupValues resolves an IP address using the configured decode function.
func (m *MMDB) LookupValues(_ context.Context, values map[string]string) map[string]string {
	r := m.reader.Load()
	if r == nil {
		return nil
	}
	ip := net.ParseIP(values["value"])
	if ip == nil {
		return nil
	}
	return m.decode(r, ip)
}

// Load opens an MMDB file and swaps the atomic reader pointer.
func (m *MMDB) Load(path string) (MMDBInfo, error) {
	r, err := maxminddb.Open(path)
	if err != nil {
		return MMDBInfo{}, fmt.Errorf("open mmdb %q: %w", path, err)
	}
	info := MMDBInfo{
		DatabaseType: r.Metadata.DatabaseType,
		BuildTime:    time.Unix(int64(r.Metadata.BuildEpoch), 0), //nolint:gosec // BuildEpoch is a uint, safe for unix timestamps
	}
	old := m.reader.Swap(r)
	if old != nil {
		_ = old.Close()
	}
	return info, nil
}

// WatchFile watches an MMDB file for changes using fsnotify.
func (m *MMDB) WatchFile(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopWatchLocked()

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	if err := w.Add(path); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %q: %w", path, err)
	}

	m.watcher = w
	m.watchPath = path
	m.watchDone = make(chan struct{})

	go m.watchLoop(w, path, m.watchDone)
	return nil
}

func (m *MMDB) watchLoop(w *fsnotify.Watcher, path string, done chan struct{}) {
	defer close(done)
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				_, _ = m.Load(path)
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
		}
	}
}

func (m *MMDB) stopWatchLocked() {
	if m.watcher != nil {
		_ = m.watcher.Close()
		<-m.watchDone
		m.watcher = nil
		m.watchPath = ""
		m.watchDone = nil
	}
}

// Close stops the file watcher and closes the current MMDB reader.
func (m *MMDB) Close() {
	m.mu.Lock()
	m.stopWatchLocked()
	m.mu.Unlock()

	if r := m.reader.Swap(nil); r != nil {
		_ = r.Close()
	}
}

// --- decode functions ---

// mmdbCityRecord contains the fields decoded from a GeoLite2-City / GeoIP2-City MMDB file.
type mmdbCityRecord struct {
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

func decodeCity(r *maxminddb.Reader, ip net.IP) map[string]string {
	var rec mmdbCityRecord
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

// mmdbASNRecord contains the fields decoded from a GeoLite2-ASN / GeoIP2-ASN MMDB file.
type mmdbASNRecord struct {
	Number       uint   `maxminddb:"autonomous_system_number"`
	Organization string `maxminddb:"autonomous_system_organization"`
}

func decodeASN(r *maxminddb.Reader, ip net.IP) map[string]string {
	var rec mmdbASNRecord
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
