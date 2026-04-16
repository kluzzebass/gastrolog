package lookup

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
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

// csvData holds the mmap'd CSV file and its key index.
// Row data lives in the mmap region, not on the heap.
// Only the key strings and their offsets are heap-allocated.
type csvData struct {
	mmapData      []byte   // memory-mapped file contents
	file          *os.File // kept open while mmap is active
	rowOffsets    []int    // byte offset of each data row (excludes header)
	keyIndex      map[string]int // key value → index into rowOffsets
	header        []string
	keyIdx        int          // column index of the key
	mappings      []colMapping // value column indices and names
	suffixes      []string     // output column names
	delimiter     rune
	duplicateKeys int // number of rows skipped due to duplicate keys
}

func (d *csvData) close() {
	if d.mmapData != nil {
		_ = syscall.Munmap(d.mmapData)
	}
	if d.file != nil {
		_ = d.file.Close()
	}
}

// CSV is a lookup table backed by a memory-mapped CSV file.
// The file is mmap'd read-only; only the key index lives on the heap.
// Row data is parsed on demand from the mmap region during lookups.
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
// Parses the matching row on demand from the mmap'd file data.
func (c *CSV) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	d := c.data.Load()
	if d == nil {
		return nil
	}
	rowIdx, ok := d.keyIndex[key]
	if !ok {
		return nil
	}
	return d.parseRow(rowIdx)
}

// parseRow parses a single data row from the mmap'd bytes and returns the value columns.
func (d *csvData) parseRow(rowIdx int) map[string]string {
	start := d.rowOffsets[rowIdx]
	end := len(d.mmapData)
	if rowIdx+1 < len(d.rowOffsets) {
		end = d.rowOffsets[rowIdx+1]
	}

	reader := csv.NewReader(bytes.NewReader(d.mmapData[start:end]))
	reader.Comma = d.delimiter
	reader.FieldsPerRecord = -1
	record, err := reader.Read()
	if err != nil {
		return nil
	}

	result := make(map[string]string, len(d.mappings))
	for _, m := range d.mappings {
		if m.idx < len(record) {
			result[m.name] = record[m.idx]
		}
	}
	return result
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

// Load memory-maps a CSV file and builds a key index.
// Row data stays in the mmap region; only key strings are heap-allocated.
func (c *CSV) Load(path string) error {
	f, err := os.Open(path) //nolint:gosec // path comes from validated config
	if err != nil {
		return fmt.Errorf("open csv file %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat csv file %q: %w", path, err)
	}
	size := info.Size()
	if size == 0 {
		_ = f.Close()
		return fmt.Errorf("csv file %q is empty", path)
	}

	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("mmap csv file %q: %w", path, err)
	}

	// Scan row byte offsets with quote-aware newline detection.
	rowOffsets := scanCSVRowOffsets(mmapData)
	if len(rowOffsets) < 1 {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		return fmt.Errorf("csv file %q: no rows found", path)
	}

	// Parse header from the first row, skipping UTF-8 BOM if present.
	headerStart := 0
	if len(mmapData) >= 3 && mmapData[0] == 0xEF && mmapData[1] == 0xBB && mmapData[2] == 0xBF {
		headerStart = 3
	}
	headerEnd := len(mmapData)
	if len(rowOffsets) > 1 {
		headerEnd = rowOffsets[1]
	}
	headerReader := csv.NewReader(bytes.NewReader(mmapData[headerStart:headerEnd]))
	headerReader.Comma = c.delimiter
	headerReader.FieldsPerRecord = -1
	header, err := headerReader.Read()
	if err != nil {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		return fmt.Errorf("read csv header from %q: %w", path, err)
	}
	if len(header) < 2 {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		return fmt.Errorf("csv file %q needs at least 2 columns (key + value)", path)
	}

	keyIdx, mappings, suffixes, err := c.resolveColumns(header, path)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		_ = f.Close()
		return err
	}

	// Data rows start after the header (index 1 onward).
	dataOffsets := rowOffsets[1:]

	// Build key index by parsing only the key column from each row.
	keyIndex := make(map[string]int, len(dataOffsets))
	var duplicateKeys int
	for i, off := range dataOffsets {
		end := len(mmapData)
		if i+1 < len(dataOffsets) {
			end = dataOffsets[i+1]
		}
		rowReader := csv.NewReader(bytes.NewReader(mmapData[off:end]))
		rowReader.Comma = c.delimiter
		rowReader.FieldsPerRecord = -1
		record, err := rowReader.Read()
		if err != nil || keyIdx >= len(record) {
			continue
		}
		key := record[keyIdx]
		if key == "" {
			continue
		}
		if _, exists := keyIndex[key]; exists {
			duplicateKeys++
			continue // first occurrence wins
		}
		keyIndex[key] = i
	}

	newData := &csvData{
		mmapData:      mmapData,
		file:          f,
		rowOffsets:    dataOffsets,
		keyIndex:      keyIndex,
		header:        header,
		keyIdx:        keyIdx,
		mappings:      mappings,
		suffixes:      suffixes,
		delimiter:     c.delimiter,
		duplicateKeys: duplicateKeys,
	}

	old := c.data.Swap(newData)
	if old != nil {
		old.close()
	}
	return nil
}

// scanCSVRowOffsets returns byte offsets of each row start (including the header at index 0).
// Handles quoted fields that contain embedded newlines.
func scanCSVRowOffsets(data []byte) []int {
	offsets := []int{0}
	inQuote := false
	for i := 0; i < len(data); i++ {
		b := data[i]
		switch {
		case inQuote && b == '"':
			if i+1 < len(data) && data[i+1] == '"' {
				i++ // skip escaped quote
			} else {
				inQuote = false
			}
		case !inQuote && b == '"':
			inQuote = true
		case !inQuote && b == '\n':
			if i+1 < len(data) {
				offsets = append(offsets, i+1)
			}
		}
	}
	return offsets
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
	old := c.data.Swap(nil)
	if old != nil {
		old.close()
	}
}
