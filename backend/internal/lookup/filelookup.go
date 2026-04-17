package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/itchyny/gojq"
	"sigs.k8s.io/yaml"
)

const maxStructuredFileSize = 10 << 20 // 10 MB; larger files should use CSV format

// FileLookupConfig configures a structured-file-backed lookup table
// (JSON or YAML). Format differs only in how the raw bytes parse into the
// map[string]any / []any tree that jq operates on.
type FileLookupConfig struct {
	Name         string
	Query        string   // jq expression that produces an array of objects
	KeyColumn    string   // field used as the lookup key; empty = first column
	ValueColumns []string // columns to include in output; empty = all non-key
}

// JSONFileConfig is an alias for FileLookupConfig on the JSON path.
type JSONFileConfig = FileLookupConfig

// YAMLFileConfig is an alias for FileLookupConfig on the YAML path.
type YAMLFileConfig = FileLookupConfig

// unmarshalFn parses raw source bytes into the generic tree jq needs.
type unmarshalFn func([]byte, any) error

// FileLookup is a lookup table backed by a structured file (JSON or YAML)
// transformed via a jq expression.
//
// On Load the source file is parsed, transformed through the compiled jq
// expression, and the results are encoded as a sorted binary lookup file.
// That file is memory-mapped for O(log n) binary search lookups with zero
// heap-allocated index — only column name strings live on the heap.
//
// Source files are limited to 10 MB. For larger datasets, use CSV format
// which is memory-mapped directly without a transform step.
//
// Safe for concurrent use; the data is swapped atomically on reload.
type FileLookup struct {
	query        *gojq.Code
	keyColumn    string
	valueColumns map[string]struct{}
	unmarshal    unmarshalFn
	format       string // "json" / "yaml" — used in error messages

	// dataMu protects data and serializes mmap lifecycle. Lookups take a
	// read lock for the duration of the lookup so the mmap cannot be
	// unmapped underneath them. Load/Close take a write lock to swap and
	// reclaim the old mmap.
	dataMu  sync.RWMutex
	data    *binData
	tmpPath atomic.Value // string: current temp file path

	mu        sync.Mutex
	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

// JSONFile is retained as a type alias so existing call sites keep working.
type JSONFile = FileLookup

var _ LookupTable = (*FileLookup)(nil)

// NewJSONFile creates a JSON file lookup table.
func NewJSONFile(cfg JSONFileConfig) (*FileLookup, error) {
	return newFileLookup(cfg, "json", json.Unmarshal)
}

// NewYAMLFile creates a YAML file lookup table. YAML parses into the
// same map[string]any / []any tree as JSON (via sigs.k8s.io/yaml, which
// routes through a JSON round-trip so map keys end up as strings), so the
// jq expression, column resolution, and mmap storage are identical.
func NewYAMLFile(cfg YAMLFileConfig) (*FileLookup, error) {
	return newFileLookup(cfg, "yaml", func(b []byte, v any) error { return yaml.Unmarshal(b, v) })
}

// newFileLookup is the shared constructor. The jq expression is compiled
// once here and reused on every Load.
func newFileLookup(cfg FileLookupConfig, format string, unmarshal unmarshalFn) (*FileLookup, error) {
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

	return &FileLookup{
		query:        code,
		keyColumn:    cfg.KeyColumn,
		valueColumns: valCols,
		unmarshal:    unmarshal,
		format:       format,
	}, nil
}

// Parameters returns the single input parameter name.
func (j *FileLookup) Parameters() []string { return []string{"value"} }

// Format returns "json" or "yaml" — used by the server to distinguish
// formats when JSONFile and YAMLFile share the same underlying type.
func (j *FileLookup) Format() string { return j.format }

// DuplicateKeys returns the number of rows with duplicate key values
// that were skipped during the last Load (first occurrence wins).
func (j *FileLookup) DuplicateKeys() int {
	j.dataMu.RLock()
	defer j.dataMu.RUnlock()
	if j.data == nil {
		return 0
	}
	return j.data.duplicateKeys
}

// Suffixes returns the output column names discovered from the loaded data.
func (j *FileLookup) Suffixes() []string {
	j.dataMu.RLock()
	defer j.dataMu.RUnlock()
	if j.data == nil {
		return nil
	}
	return j.data.suffixes
}

// LookupValues performs an O(log n) binary search lookup in the mmap'd data.
func (j *FileLookup) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	j.dataMu.RLock()
	defer j.dataMu.RUnlock()
	if j.data == nil {
		return nil
	}
	return j.data.lookupKey(j.data.mmapData, key, j.data.suffixes)
}

// Load parses the source, transforms it through jq, encodes as a
// binary lookup file, and memory-maps it for lookups.
func (j *FileLookup) Load(path string) error {
	// Transform source → binary lookup file.
	tmpPath, dups, err := j.transformToBin(path)
	if err != nil {
		return err
	}

	// Mmap the binary file.
	f, err := os.Open(tmpPath) //nolint:gosec // path from CreateTemp
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("open bin lookup: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("mmap bin lookup: %w", err)
	}

	// Decode header and column names.
	newData, err := decodeBinHeader(mmapData)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	cols, err := decodeBinColumns(mmapData, newData.numCols)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	newData.mmapData = mmapData
	newData.file = f
	newData.suffixes = cols
	newData.duplicateKeys = dups

	// Hold the write lock through swap AND mmap reclamation so no
	// in-flight LookupValues call (holding the read lock) can be reading
	// the old mmap when we Munmap it. SIGBUS otherwise.
	j.dataMu.Lock()
	old := j.data
	j.data = newData
	if old != nil {
		oldMmap := old.mmapData
		old.close()
		if oldMmap != nil {
			_ = syscall.Munmap(oldMmap)
		}
	}
	j.dataMu.Unlock()

	if prev, _ := j.tmpPath.Load().(string); prev != "" && prev != tmpPath {
		_ = os.Remove(prev)
	}
	j.tmpPath.Store(tmpPath)
	return nil
}

// transformToBin parses source, runs jq, and encodes the results as a
// binary lookup file. Returns the temp file path.
func (j *FileLookup) transformToBin(path string) (tmpPath string, duplicates int, err error) {
	// Mmap source for reading.
	srcFile, err := os.Open(path) //nolint:gosec // validated config path
	if err != nil {
		return "", 0, fmt.Errorf("open %q: %w", path, err)
	}
	info, err := srcFile.Stat()
	if err != nil {
		_ = srcFile.Close()
		return "", 0, fmt.Errorf("stat %q: %w", path, err)
	}
	size := info.Size()
	if size == 0 {
		_ = srcFile.Close()
		return "", 0, fmt.Errorf("%s file %q is empty", j.format, path)
	}
	if size > maxStructuredFileSize {
		_ = srcFile.Close()
		return "", 0, fmt.Errorf("%s file %q is %d bytes (max %d); use CSV format for large files", j.format, path, size, maxStructuredFileSize)
	}
	mmapData, err := syscall.Mmap(int(srcFile.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115
	if err != nil {
		_ = srcFile.Close()
		return "", 0, fmt.Errorf("mmap %q: %w", path, err)
	}

	// Parse (transient — freed after this function returns).
	var root any
	if err := j.unmarshal(mmapData, &root); err != nil {
		_ = syscall.Munmap(mmapData)
		_ = srcFile.Close()
		return "", 0, fmt.Errorf("parse %q: %w", path, err)
	}

	// Release source mmap — we only need the parsed tree now.
	_ = syscall.Munmap(mmapData)
	_ = srcFile.Close()

	// Run jq and collect rows.
	columns, rows, err := j.collectRows(root, path)
	if err != nil {
		return "", 0, err
	}

	// Encode to binary.
	encoded, dups, err := encodeBinLookup(columns, rows)
	if err != nil {
		return "", 0, fmt.Errorf("encode bin lookup: %w", err)
	}

	// Write to temp file.
	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".filelookup-*.bin")
	if err != nil {
		return "", 0, fmt.Errorf("create temp bin: %w", err)
	}
	tmpPath = tmpFile.Name()
	if _, err := tmpFile.Write(encoded); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return "", 0, fmt.Errorf("write bin lookup: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return "", 0, err
	}

	return tmpPath, dups, nil
}

const maxTransformRows = 100_000

// resolveColumns determines the key column and value columns from the first
// flattened row. Returns the key column name and the sorted value column names.
func (j *FileLookup) resolveColumns(flat map[string]string) (string, []string) {
	allCols := sortedStringKeys(flat)

	// Resolve key column.
	var keyCol string
	if j.keyColumn != "" {
		keyCol = j.keyColumn
	} else if len(allCols) > 0 {
		keyCol = allCols[0]
	}

	// Value columns: configured or all non-key.
	var valueCols []string
	for _, c := range allCols {
		if c == keyCol {
			continue
		}
		if j.valueColumns != nil {
			if _, ok := j.valueColumns[c]; !ok {
				continue
			}
		}
		valueCols = append(valueCols, c)
	}
	return keyCol, valueCols
}

// toObjects coerces a jq output value into a slice of objects.
// Non-object values are silently skipped.
func toObjects(v any) []map[string]any {
	switch tv := v.(type) {
	case map[string]any:
		return []map[string]any{tv}
	case []any:
		var out []map[string]any
		for _, elem := range tv {
			if obj, ok := elem.(map[string]any); ok {
				out = append(out, obj)
			}
		}
		return out
	default:
		return nil
	}
}

// rowCollector accumulates rows during jq iteration, resolving columns
// from the first flattened object.
type rowCollector struct {
	j          *FileLookup
	keyCol     string
	allColumns []string
	rows       []binRow
}

// addObject flattens an object and appends a row. Returns false if the
// object was nil (skipped).
func (rc *rowCollector) addObject(obj map[string]any) bool {
	flat := flattenScalars(obj)
	if flat == nil {
		return false
	}
	if rc.allColumns == nil {
		rc.keyCol, rc.allColumns = rc.j.resolveColumns(flat)
	}
	vals := make([]string, len(rc.allColumns))
	for i, c := range rc.allColumns {
		vals[i] = flat[c]
	}
	rc.rows = append(rc.rows, binRow{key: flat[rc.keyCol], values: vals})
	return true
}

// collectRows runs the jq expression and collects flattened rows.
// Returns the value column names (sorted, excluding key) and the rows.
func (j *FileLookup) collectRows(root any, path string) ([]string, []binRow, error) {
	rc := rowCollector{j: j}

	iter := j.query.Run(root)
	for len(rc.rows) < maxTransformRows {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if _, isErr := v.(error); isErr {
			break
		}
		for _, obj := range toObjects(v) {
			rc.addObject(obj)
			if len(rc.rows) >= maxTransformRows {
				break
			}
		}
	}

	if len(rc.rows) == 0 {
		return nil, nil, fmt.Errorf("jq expression produced no results for %q", path)
	}
	return rc.allColumns, rc.rows, nil
}

// sortedStringKeys returns the sorted keys of a map.
func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// WatchFile watches a file for changes using fsnotify.
func (j *FileLookup) WatchFile(path string) error {
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

func (j *FileLookup) watchLoop(w *fsnotify.Watcher, path string, done chan struct{}) {
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

func (j *FileLookup) stopWatchLocked() {
	if j.watcher != nil {
		_ = j.watcher.Close()
		<-j.watchDone
		j.watcher = nil
		j.watchPath = ""
		j.watchDone = nil
	}
}

// Close stops the file watcher and releases all resources including temp files.
func (j *FileLookup) Close() {
	j.mu.Lock()
	j.stopWatchLocked()
	j.mu.Unlock()

	j.dataMu.Lock()
	d := j.data
	j.data = nil
	if d != nil {
		mmap := d.mmapData
		d.close()
		if mmap != nil {
			_ = syscall.Munmap(mmap)
		}
	}
	j.dataMu.Unlock()
	if p, _ := j.tmpPath.Load().(string); p != "" {
		_ = os.Remove(p)
	}
}
