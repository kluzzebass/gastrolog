package lookup

import (
	"context"
	"encoding/csv"
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
)

const maxJSONFileSize = 10 << 20 // 10 MB; larger files should use CSV format

// JSONFileConfig configures a JSON file-backed lookup table.
type JSONFileConfig struct {
	Name         string
	Query        string   // jq expression that produces an array of objects
	KeyColumn    string   // field used as the lookup key; empty = first column
	ValueColumns []string // columns to include in output; empty = all non-key
}

// JSONFile is a lookup table backed by a JSON file transformed via a jq expression.
//
// On Load, the source JSON is parsed, transformed through the compiled jq
// expression, and the results are written as CSV to a temp file. That CSV is
// then memory-mapped using the same csvData structure as the CSV lookup — row
// data lives in the mmap region, only key strings and byte offsets are on the
// heap. The parsed JSON tree is transient and freed after the transform.
//
// JSON source files are limited to 10 MB. For larger datasets, use CSV format
// which is memory-mapped directly without a transform step.
//
// Safe for concurrent use; the data is swapped atomically on reload.
type JSONFile struct {
	query        *gojq.Code
	keyColumn    string
	valueColumns map[string]struct{}

	data    atomic.Pointer[csvData]
	tmpPath atomic.Value // string: current temp CSV file path

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

// DuplicateKeys returns the number of rows with duplicate key values
// that were skipped during the last Load (first occurrence wins).
func (j *JSONFile) DuplicateKeys() int {
	d := j.data.Load()
	if d == nil {
		return 0
	}
	return d.duplicateKeys
}

// Suffixes returns the output column names discovered from the loaded data.
func (j *JSONFile) Suffixes() []string {
	d := j.data.Load()
	if d == nil {
		return nil
	}
	return d.suffixes
}

// LookupValues performs an O(1) key lookup in the mmap'd transformed data.
func (j *JSONFile) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	d := j.data.Load()
	if d == nil {
		return nil
	}
	rowIdx, ok := d.keyIndex[key]
	if !ok {
		return nil
	}
	return d.parseRow(rowIdx)
}

// Load parses the JSON source, transforms it through jq, writes the results
// as CSV, and memory-maps the CSV for lookups.
func (j *JSONFile) Load(path string) error {
	// Transform JSON → temp CSV.
	tmpPath, err := j.transformToCSV(path)
	if err != nil {
		return err
	}

	// Mmap the CSV and build key index.
	loader := &CSV{
		keyColumn:    j.keyColumn,
		valueColumns: j.valueColumns,
		delimiter:    ',',
	}
	if err := loader.Load(tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("index transformed csv: %w", err)
	}
	newData := loader.data.Swap(nil)

	old := j.data.Swap(newData)
	if old != nil {
		old.close()
	}

	if prev, _ := j.tmpPath.Load().(string); prev != "" && prev != tmpPath {
		_ = os.Remove(prev)
	}
	j.tmpPath.Store(tmpPath)
	return nil
}

// transformToCSV parses the JSON, runs jq, and writes results as CSV.
func (j *JSONFile) transformToCSV(path string) (string, error) {
	// Mmap source for reading.
	srcFile, err := os.Open(path) //nolint:gosec // validated config path
	if err != nil {
		return "", fmt.Errorf("open %q: %w", path, err)
	}
	info, err := srcFile.Stat()
	if err != nil {
		_ = srcFile.Close()
		return "", fmt.Errorf("stat %q: %w", path, err)
	}
	size := info.Size()
	if size == 0 {
		_ = srcFile.Close()
		return "", fmt.Errorf("json file %q is empty", path)
	}
	if size > maxJSONFileSize {
		_ = srcFile.Close()
		return "", fmt.Errorf("json file %q is %d bytes (max %d); use CSV format for large files", path, size, maxJSONFileSize)
	}
	mmapData, err := syscall.Mmap(int(srcFile.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115
	if err != nil {
		_ = srcFile.Close()
		return "", fmt.Errorf("mmap %q: %w", path, err)
	}

	// Parse (transient — freed after this function returns).
	var root any
	if err := json.Unmarshal(mmapData, &root); err != nil {
		_ = syscall.Munmap(mmapData)
		_ = srcFile.Close()
		return "", fmt.Errorf("parse %q: %w", path, err)
	}

	// Release source mmap — we only need the parsed tree now.
	_ = syscall.Munmap(mmapData)
	_ = srcFile.Close()

	// Create temp file for CSV output.
	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".jsonlookup-*.csv")
	if err != nil {
		return "", fmt.Errorf("create temp csv: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Run jq and stream results to CSV.
	if err := j.writeCSV(root, tmpFile, path); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703: path from os.CreateTemp, not user input
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703: path from os.CreateTemp, not user input
		return "", fmt.Errorf("close temp csv: %w", err)
	}

	return tmpPath, nil
}

const maxTransformRows = 100_000

// writeCSV runs the compiled jq expression and writes results as CSV rows.
func (j *JSONFile) writeCSV(root any, out *os.File, path string) error {
	w := csv.NewWriter(out)
	var header []string
	rowCount := 0

	iter := j.query.Run(root)
	for rowCount < maxTransformRows {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if _, isErr := v.(error); isErr {
			break
		}

		// Each result may be a single object or an array of objects.
		var objects []map[string]any
		switch tv := v.(type) {
		case map[string]any:
			objects = []map[string]any{tv}
		case []any:
			for _, elem := range tv {
				if obj, ok := elem.(map[string]any); ok {
					objects = append(objects, obj)
				}
			}
		}

		for _, obj := range objects {
			flat := flattenScalars(obj)
			if flat == nil {
				continue
			}
			if header == nil {
				header = sortedStringKeys(flat)
				_ = w.Write(header)
			}
			record := make([]string, len(header))
			for i, h := range header {
				record[i] = flat[h]
			}
			_ = w.Write(record)
			rowCount++
			if rowCount >= maxTransformRows {
				break
			}
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}
	if rowCount == 0 {
		return fmt.Errorf("jq expression produced no results for %q", path)
	}
	return nil
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

// WatchFile watches a JSON file for changes using fsnotify.
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

// Close stops the file watcher and releases all resources including temp files.
func (j *JSONFile) Close() {
	j.mu.Lock()
	j.stopWatchLocked()
	j.mu.Unlock()

	if d := j.data.Swap(nil); d != nil {
		d.close()
	}
	if p, _ := j.tmpPath.Load().(string); p != "" {
		_ = os.Remove(p)
	}
}
