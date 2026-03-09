package lookup

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

// CSVConfig configures a CSV file-backed lookup table.
type CSVConfig struct {
	KeyColumn    string // column header to use as the lookup key; empty = first column
	ValueColumns []string // column headers to include as output; empty = all non-key columns
	Delimiter    rune     // field delimiter; zero = ','
}

// csvData holds the parsed CSV lookup data.
type csvData struct {
	rows     map[string]map[string]string // key → {col: value}
	suffixes []string                     // output column names in header order
}

// CSV is a lookup table backed by a CSV file.
// The first matching row for a given key is returned.
// Safe for concurrent use; data is swapped atomically on reload.
type CSV struct {
	keyColumn    string
	valueColumns map[string]struct{} // nil = all non-key columns
	delimiter    rune

	data atomic.Pointer[csvData]

	mu        sync.Mutex
	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

var _ LookupTable = (*CSV)(nil)

// NewCSV creates a CSV file lookup table.
func NewCSV(cfg CSVConfig) *CSV {
	delim := cfg.Delimiter
	if delim == 0 {
		delim = ','
	}
	var valCols map[string]struct{}
	if len(cfg.ValueColumns) > 0 {
		valCols = make(map[string]struct{}, len(cfg.ValueColumns))
		for _, c := range cfg.ValueColumns {
			valCols[c] = struct{}{}
		}
	}
	return &CSV{
		keyColumn:    cfg.KeyColumn,
		valueColumns: valCols,
		delimiter:    delim,
	}
}

// Parameters returns the single input parameter name.
func (c *CSV) Parameters() []string { return []string{"value"} }

// Suffixes returns the output column names discovered from the CSV header.
func (c *CSV) Suffixes() []string {
	d := c.data.Load()
	if d == nil {
		return nil
	}
	return d.suffixes
}

// LookupValues looks up a key in the CSV table.
func (c *CSV) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	d := c.data.Load()
	if d == nil {
		return nil
	}
	return d.rows[key]
}

type colMapping struct {
	idx  int
	name string
}

// resolveColumns determines the key column index and value column mappings from the header.
func (c *CSV) resolveColumns(header []string, path string) (keyIdx int, mappings []colMapping, suffixes []string, err error) {
	if c.keyColumn != "" {
		keyIdx = -1
		for i, h := range header {
			if h == c.keyColumn {
				keyIdx = i
				break
			}
		}
		if keyIdx < 0 {
			return 0, nil, nil, fmt.Errorf("csv file %q: key column %q not found in header %v", path, c.keyColumn, header)
		}
	}

	for i, h := range header {
		if i == keyIdx {
			continue
		}
		if c.valueColumns != nil {
			if _, ok := c.valueColumns[h]; !ok {
				continue
			}
		}
		mappings = append(mappings, colMapping{idx: i, name: h})
		suffixes = append(suffixes, h)
	}

	if len(mappings) == 0 {
		return 0, nil, nil, fmt.Errorf("csv file %q: no value columns after filtering", path)
	}
	return keyIdx, mappings, suffixes, nil
}

// Load reads and parses a CSV file into the lookup table.
// The previous data is replaced atomically.
func (c *CSV) Load(path string) error {
	f, err := os.Open(path) //nolint:gosec // path comes from validated config
	if err != nil {
		return fmt.Errorf("open csv file %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	reader := csv.NewReader(f)
	reader.Comma = c.delimiter
	reader.FieldsPerRecord = -1 // allow ragged rows
	reader.ReuseRecord = false

	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read csv header from %q: %w", path, err)
	}
	if len(header) < 2 {
		return fmt.Errorf("csv file %q needs at least 2 columns (key + value)", path)
	}

	keyIdx, mappings, suffixes, err := c.resolveColumns(header, path)
	if err != nil {
		return err
	}

	// Read all rows into the map. First occurrence of a key wins.
	rows := make(map[string]map[string]string)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read csv row from %q: %w", path, err)
		}
		if keyIdx >= len(record) || record[keyIdx] == "" {
			continue
		}
		key := record[keyIdx]
		if _, exists := rows[key]; exists {
			continue // first occurrence wins
		}
		row := make(map[string]string, len(mappings))
		for _, m := range mappings {
			if m.idx < len(record) {
				row[m.name] = record[m.idx]
			}
		}
		rows[key] = row
	}

	c.data.Swap(&csvData{rows: rows, suffixes: suffixes})
	return nil
}

// WatchFile watches the CSV file for changes and reloads on write/create.
func (c *CSV) WatchFile(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stopWatchLocked()

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	if err := w.Add(path); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %q: %w", path, err)
	}

	c.watcher = w
	c.watchPath = path
	c.watchDone = make(chan struct{})

	go c.watchLoop(w, path, c.watchDone)
	return nil
}

func (c *CSV) watchLoop(w *fsnotify.Watcher, path string, done chan struct{}) {
	defer close(done)
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				_ = c.Load(path)
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
		}
	}
}

func (c *CSV) stopWatchLocked() {
	if c.watcher != nil {
		_ = c.watcher.Close()
		<-c.watchDone
		c.watcher = nil
		c.watchPath = ""
		c.watchDone = nil
	}
}

// Close stops the file watcher and releases all resources.
func (c *CSV) Close() {
	c.mu.Lock()
	c.stopWatchLocked()
	c.mu.Unlock()
	c.data.Store(nil)
}
