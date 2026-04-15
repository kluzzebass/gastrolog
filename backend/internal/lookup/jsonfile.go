package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/itchyny/gojq"
)

// JSONFileConfig configures a JSON file-backed lookup table.
type JSONFileConfig struct {
	Name         string
	Query        string   // jq expression that produces an array of objects
	KeyColumn    string   // field used as the lookup key; empty = first column
	ValueColumns []string // columns to include in output; empty = all non-key
}

// jsonData holds the indexed table produced by running the jq expression
// against a memory-mapped JSON file.
type jsonData struct {
	index    map[string]map[string]string // key value -> row values
	suffixes []string                     // output column names (sorted)

	// mmap backing — kept alive so garbage collection doesn't reclaim it prematurely.
	mmapData []byte
	file     *os.File
}

func (d *jsonData) close() {
	if d.mmapData != nil {
		_ = syscall.Munmap(d.mmapData)
	}
	if d.file != nil {
		_ = d.file.Close()
	}
}

// JSONFile is a lookup table backed by a memory-mapped JSON file.
// The jq expression is compiled once at construction time. On each Load,
// it runs against the parsed JSON to produce an array of objects, which are
// indexed by key_column for O(1) lookups.
//
// Safe for concurrent use; the data is swapped atomically on reload.
type JSONFile struct {
	query        *gojq.Code
	keyColumn    string
	valueColumns map[string]struct{} // nil = all non-key columns

	data atomic.Pointer[jsonData]

	mu        sync.Mutex
	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

var _ LookupTable = (*JSONFile)(nil)

// NewJSONFile creates a JSON file lookup table.
// The jq expression is compiled once here and reused on every Load.
func NewJSONFile(cfg JSONFileConfig) (*JSONFile, error) {
	parsed, err := gojq.Parse(cfg.Query)
	if err != nil {
		return nil, fmt.Errorf("parse jq expression %q: %w", cfg.Query, err)
	}
	code, err := gojq.Compile(parsed)
	if err != nil {
		return nil, fmt.Errorf("compile jq expression %q: %w", cfg.Query, err)
	}

	var valCols map[string]struct{}
	if len(cfg.ValueColumns) > 0 {
		valCols = make(map[string]struct{}, len(cfg.ValueColumns))
		for _, c := range cfg.ValueColumns {
			valCols[c] = struct{}{}
		}
	}

	return &JSONFile{
		query:        code,
		keyColumn:    cfg.KeyColumn,
		valueColumns: valCols,
	}, nil
}

// Parameters returns the single input parameter name.
func (j *JSONFile) Parameters() []string { return []string{"value"} }

// Suffixes returns the output column names discovered from the loaded data.
// Returns nil before any successful load.
func (j *JSONFile) Suffixes() []string {
	d := j.data.Load()
	if d == nil {
		return nil
	}
	return d.suffixes
}

// LookupValues performs an O(1) key lookup in the indexed table.
func (j *JSONFile) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	d := j.data.Load()
	if d == nil {
		return nil
	}
	row, ok := d.index[key]
	if !ok {
		return nil
	}
	// Return a copy to prevent caller mutation.
	out := make(map[string]string, len(row))
	maps.Copy(out, row)
	return out
}

// Load memory-maps a JSON file, parses it, runs the compiled jq expression
// to produce an array of objects, and indexes them by key_column.
// The previous data is released after the atomic swap.
func (j *JSONFile) Load(path string) error {
	f, err := os.Open(path) //nolint:gosec // path comes from validated config, not user input
	if err != nil {
		return fmt.Errorf("open json file %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat json file %q: %w", path, err)
	}

	size := info.Size()
	if size == 0 {
		_ = f.Close()
		return fmt.Errorf("json file %q is empty", path)
	}

	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64->int safe on 64-bit
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("mmap json file %q: %w", path, err)
	}

	var root any
	if err := json.Unmarshal(mmapData, &root); err != nil {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		return fmt.Errorf("parse json file %q: %w", path, err)
	}

	// Run the jq expression and build the indexed table.
	index, suffixes, err := j.buildIndex(root, path)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		return err
	}

	newData := &jsonData{
		index:    index,
		suffixes: suffixes,
		mmapData: mmapData,
		file:     f,
	}

	old := j.data.Swap(newData)
	if old != nil {
		old.close()
	}

	return nil
}

// buildIndex runs the compiled jq expression against the parsed JSON,
// collects object results into rows, and indexes them by key column.
func (j *JSONFile) buildIndex(root any, path string) (map[string]map[string]string, []string, error) {
	results := jqSelect(j.query, root)
	if len(results) == 0 {
		return nil, nil, fmt.Errorf("jq expression produced no results for %q", path)
	}

	// Collect results into rows. Each jq result may be a single object or
	// an array of objects (e.g. `.hosts` yields the whole array as one result).
	// Unwrap arrays so individual objects become rows.
	var objects []map[string]any
	for _, r := range results {
		switch v := r.(type) {
		case map[string]any:
			objects = append(objects, v)
		case []any:
			for _, elem := range v {
				if obj, ok := elem.(map[string]any); ok {
					objects = append(objects, obj)
				}
			}
		}
	}

	var rows []map[string]string
	for _, obj := range objects {
		row := flattenScalars(obj)
		if row != nil {
			rows = append(rows, row)
		}
	}
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("jq expression produced no object results for %q", path)
	}

	keyCol := j.resolveKeyColumn(rows)
	suffixes := j.resolveSuffixes(rows, keyCol)

	index := indexRows(rows, keyCol, suffixes)
	return index, suffixes, nil
}

// indexRows builds a key -> value-columns map from flattened rows.
// First occurrence wins for duplicate keys; empty keys are skipped.
func indexRows(rows []map[string]string, keyCol string, suffixes []string) map[string]map[string]string {
	valSet := make(map[string]struct{}, len(suffixes))
	for _, c := range suffixes {
		valSet[c] = struct{}{}
	}

	index := make(map[string]map[string]string, len(rows))
	for _, row := range rows {
		key := row[keyCol]
		if key == "" {
			continue
		}
		if _, exists := index[key]; exists {
			continue // first occurrence wins
		}
		vals := make(map[string]string, len(suffixes))
		for k, v := range row {
			if k == keyCol {
				continue
			}
			if _, ok := valSet[k]; ok {
				vals[k] = v
			}
		}
		index[key] = vals
	}
	return index
}

// resolveKeyColumn returns the key column name: the configured one, or the
// lexicographically first column from the first row.
func (j *JSONFile) resolveKeyColumn(rows []map[string]string) string {
	if j.keyColumn != "" {
		return j.keyColumn
	}
	keys := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		return keys[0]
	}
	return ""
}

// resolveSuffixes returns the output column names: the configured value columns,
// or all non-key columns discovered from the rows (sorted for determinism).
func (j *JSONFile) resolveSuffixes(rows []map[string]string, keyCol string) []string {
	if j.valueColumns != nil {
		out := make([]string, 0, len(j.valueColumns))
		for c := range j.valueColumns {
			out = append(out, c)
		}
		sort.Strings(out)
		return out
	}

	seen := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			if k != keyCol {
				seen[k] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// WatchFile watches a JSON file for changes using fsnotify.
// On write/create events, it reloads the data via Load.
// Calling WatchFile again replaces the previous watch.
func (j *JSONFile) WatchFile(path string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.stopWatchLocked()

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	if err := w.Add(path); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %q: %w", path, err)
	}

	j.watcher = w
	j.watchPath = path
	j.watchDone = make(chan struct{})

	go j.watchLoop(w, path, j.watchDone)
	return nil
}

func (j *JSONFile) watchLoop(w *fsnotify.Watcher, path string, done chan struct{}) {
	defer close(done)
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				_ = j.Load(path)
			}
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
		}
	}
}

func (j *JSONFile) stopWatchLocked() {
	if j.watcher != nil {
		_ = j.watcher.Close()
		<-j.watchDone
		j.watcher = nil
		j.watchPath = ""
		j.watchDone = nil
	}
}

// Close stops the file watcher and releases all resources.
func (j *JSONFile) Close() {
	j.mu.Lock()
	j.stopWatchLocked()
	j.mu.Unlock()

	if d := j.data.Swap(nil); d != nil {
		d.close()
	}
}
