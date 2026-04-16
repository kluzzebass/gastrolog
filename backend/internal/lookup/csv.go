package lookup

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/fsnotify/fsnotify"
)

// CSVConfig configures a CSV file-backed lookup table.
type CSVConfig struct {
	KeyColumn    string   // column header to use as the lookup key; empty = first column
	ValueColumns []string // column headers to include as output; empty = all non-key columns
	Delimiter    rune     // field delimiter; zero = ','
}

// CSV is a lookup table backed by a CSV file.
//
// On Load, the source CSV is parsed and encoded as a sorted binary lookup
// file. That file is memory-mapped for O(log n) binary search lookups with
// zero heap-allocated index. The source CSV is only read during Load — the
// mmap'd binary file serves all lookups.
//
// Safe for concurrent use; data is swapped atomically on reload.
type CSV struct {
	keyColumn    string
	valueColumns map[string]struct{} // nil = all non-key columns
	delimiter    rune

	data    atomic.Pointer[binData]
	tmpPath atomic.Value // string: current temp binary file path

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

// DuplicateKeys returns the number of rows with duplicate key values
// that were skipped during the last Load (first occurrence wins).
func (c *CSV) DuplicateKeys() int {
	d := c.data.Load()
	if d == nil {
		return 0
	}
	return d.duplicateKeys
}

// LookupValues performs an O(log n) binary search lookup.
func (c *CSV) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	d := c.data.Load()
	if d == nil {
		return nil
	}
	return d.lookupKey(d.mmapData, key, d.suffixes)
}

// Load parses the source CSV, encodes it as a binary lookup file, and
// memory-maps the binary file for lookups.
func (c *CSV) Load(path string) error {
	// Mmap source CSV for reading.
	srcFile, err := os.Open(path) //nolint:gosec // path from validated config
	if err != nil {
		return fmt.Errorf("open csv file %q: %w", path, err)
	}
	info, err := srcFile.Stat()
	if err != nil {
		_ = srcFile.Close()
		return fmt.Errorf("stat csv file %q: %w", path, err)
	}
	if info.Size() == 0 {
		_ = srcFile.Close()
		return fmt.Errorf("csv file %q is empty", path)
	}
	mmapData, err := syscall.Mmap(int(srcFile.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115
	if err != nil {
		_ = srcFile.Close()
		return fmt.Errorf("mmap csv file %q: %w", path, err)
	}

	// Parse CSV and collect rows.
	columns, rows, err := c.parseCSVRows(mmapData, path)

	// Release source mmap — we only need the collected rows now.
	_ = syscall.Munmap(mmapData)
	_ = srcFile.Close()

	if err != nil {
		return err
	}

	// Encode to binary.
	encoded, dups, err := encodeBinLookup(columns, rows)
	if err != nil {
		return fmt.Errorf("encode bin lookup for %q: %w", path, err)
	}

	// Write to temp file.
	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".csvlookup-*.bin")
	if err != nil {
		return fmt.Errorf("create temp bin: %w", err)
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(encoded); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return fmt.Errorf("write bin lookup: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return err
	}

	// Mmap the binary file.
	binFile, err := os.Open(tmpPath) //nolint:gosec // path from CreateTemp
	if err != nil {
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return err
	}
	binInfo, err := binFile.Stat()
	if err != nil {
		_ = binFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return err
	}
	binMmap, err := syscall.Mmap(int(binFile.Fd()), 0, int(binInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115
	if err != nil {
		_ = binFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return fmt.Errorf("mmap bin lookup: %w", err)
	}

	// Decode header.
	newData, err := decodeBinHeader(binMmap)
	if err != nil {
		_ = syscall.Munmap(binMmap)
		_ = binFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return err
	}
	cols, err := decodeBinColumns(binMmap, newData.numCols)
	if err != nil {
		_ = syscall.Munmap(binMmap)
		_ = binFile.Close()
		_ = os.Remove(tmpPath) //nolint:gosec // G703
		return err
	}
	newData.mmapData = binMmap
	newData.file = binFile
	newData.suffixes = cols
	newData.duplicateKeys = dups

	old := c.data.Swap(newData)
	if old != nil {
		oldMmap := old.mmapData
		old.mmapData = nil
		old.close()
		if oldMmap != nil {
			_ = syscall.Munmap(oldMmap)
		}
	}

	if prev, _ := c.tmpPath.Load().(string); prev != "" && prev != tmpPath {
		_ = os.Remove(prev)
	}
	c.tmpPath.Store(tmpPath)
	return nil
}

// csvColMap maps a header column to its index.
type csvColMap struct {
	idx  int
	name string
}

// findKeyColumnIndex returns the index of the named column in header, or -1.
func findKeyColumnIndex(header []string, name string) int {
	for i, h := range header {
		if h == name {
			return i
		}
	}
	return -1
}

// resolveCSVColumns determines the key column index and value column mappings
// from the CSV header. Returns the key index, the value column mappings, and
// the value column names.
func (c *CSV) resolveCSVColumns(header []string, path string) (int, []csvColMap, []string, error) {
	keyIdx := 0
	if c.keyColumn != "" {
		keyIdx = findKeyColumnIndex(header, c.keyColumn)
		if keyIdx < 0 {
			return 0, nil, nil, fmt.Errorf("csv file %q: key column %q not found in header %v", path, c.keyColumn, header)
		}
	}

	valCols := c.buildValueColumns(header, keyIdx)
	if len(valCols) == 0 {
		return 0, nil, nil, fmt.Errorf("csv file %q: no value columns after filtering", path)
	}

	columns := make([]string, len(valCols))
	for i, vc := range valCols {
		columns[i] = vc.name
	}
	return keyIdx, valCols, columns, nil
}

// buildValueColumns returns the value column mappings from the header,
// excluding the key column and filtering by configured value columns.
func (c *CSV) buildValueColumns(header []string, keyIdx int) []csvColMap {
	var valCols []csvColMap
	for i, h := range header {
		if i == keyIdx {
			continue
		}
		if c.valueColumns != nil {
			if _, ok := c.valueColumns[h]; !ok {
				continue
			}
		}
		valCols = append(valCols, csvColMap{idx: i, name: h})
	}
	return valCols
}

// readCSVDataRows reads all data records from the CSV reader and converts them to binRows.
func readCSVDataRows(reader *csv.Reader, keyIdx int, valCols []csvColMap) []binRow {
	var rows []binRow
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		if keyIdx >= len(record) {
			continue
		}
		key := record[keyIdx]
		if key == "" {
			continue
		}
		vals := make([]string, len(valCols))
		for i, vc := range valCols {
			if vc.idx < len(record) {
				vals[i] = record[vc.idx]
			}
		}
		rows = append(rows, binRow{key: key, values: vals})
	}
	return rows
}

// parseCSVRows reads the mmap'd CSV data and returns value column names and rows.
func (c *CSV) parseCSVRows(data []byte, path string) ([]string, []binRow, error) {
	// Strip UTF-8 BOM.
	src := data
	if len(src) >= 3 && src[0] == 0xEF && src[1] == 0xBB && src[2] == 0xBF {
		src = src[3:]
	}

	reader := csv.NewReader(bytes.NewReader(src))
	reader.Comma = c.delimiter
	reader.FieldsPerRecord = -1

	// Read header.
	header, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("read csv header from %q: %w", path, err)
	}
	if len(header) < 2 {
		return nil, nil, fmt.Errorf("csv file %q needs at least 2 columns (key + value)", path)
	}

	keyIdx, valCols, columns, err := c.resolveCSVColumns(header, path)
	if err != nil {
		return nil, nil, err
	}

	rows := readCSVDataRows(reader, keyIdx, valCols)
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("csv file %q: no data rows", path)
	}
	return columns, rows, nil
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

	if d := c.data.Swap(nil); d != nil {
		mmap := d.mmapData
		d.mmapData = nil
		d.close()
		if mmap != nil {
			_ = syscall.Munmap(mmap)
		}
	}
	if p, _ := c.tmpPath.Load().(string); p != "" {
		_ = os.Remove(p)
	}
}
