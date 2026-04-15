package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// JSONFileConfig configures a JSON file-backed lookup table.
type JSONFileConfig struct {
	Query         string   // JSONPath query template with {value} placeholder
	ResponsePaths []string // optional: JSONPath expressions to extract from query results
	Parameters    []string // ordered parameter names for {name} placeholders; empty = legacy {value} mode
}

// jsonData holds a memory-mapped JSON file and its parsed representation.
type jsonData struct {
	root     any      // parsed JSON tree (backed by mmapped bytes)
	suffixes []string // discovered output keys

	// mmap backing — kept alive so the parsed strings can reference it.
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
// At lookup time, the query template is instantiated with the lookup value
// and executed as a JSONPath expression against the parsed JSON tree.
// Optional response paths further extract fields from query results.
//
// Safe for concurrent use; the data is swapped atomically on reload.
type JSONFile struct {
	queryTemplate string     // JSONPath with {value}/{name} placeholders
	responsePaths []httpPath // parsed JSONPath for post-extraction; nil = flatten results directly
	parameters    []string   // ordered parameter names; empty = legacy {value} mode

	data atomic.Pointer[jsonData]

	mu        sync.Mutex
	cache     map[string]cacheEntry
	cacheTTL  time.Duration
	cacheSize int

	watcher   *fsnotify.Watcher
	watchPath string
	watchDone chan struct{}
}

type cacheEntry struct {
	result  map[string]string
	expires time.Time
}

const (
	defaultJSONFileCacheTTL = 1 * time.Minute
	defaultJSONFileCacheMax = 10_000
)

// NewJSONFile creates a JSON file lookup table.
func NewJSONFile(cfg JSONFileConfig) *JSONFile {
	var paths []httpPath
	for _, p := range cfg.ResponsePaths {
		code, err := CompileJQ(p)
		if err != nil {
			continue
		}
		paths = append(paths, httpPath{raw: p, parsed: code})
	}

	params := cfg.Parameters
	if len(params) == 0 {
		params = []string{"value"}
	}

	return &JSONFile{
		queryTemplate: cfg.Query,
		responsePaths: paths,
		parameters:    params,
		cache:         make(map[string]cacheEntry),
		cacheTTL:      defaultJSONFileCacheTTL,
		cacheSize:     defaultJSONFileCacheMax,
	}
}

// Suffixes returns the output keys discovered from the loaded data.
// Returns nil before any successful load or if no suffixes were discovered.
func (j *JSONFile) Suffixes() []string {
	d := j.data.Load()
	if d == nil {
		return nil
	}
	return d.suffixes
}

// Parameters returns the ordered parameter names (at least ["value"]).
func (j *JSONFile) Parameters() []string {
	return j.parameters
}

// LookupValues performs a single lookup with multiple named input values.
// Values are substituted as {key} placeholders in the query template.
func (j *JSONFile) LookupValues(_ context.Context, values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	d := j.data.Load()
	if d == nil {
		return nil
	}

	// Build cache key from parameter values in order.
	var b strings.Builder
	for _, p := range j.parameters {
		if b.Len() > 0 {
			b.WriteByte(0)
		}
		b.WriteString(values[p])
	}
	cacheKey := b.String()

	return j.cachedExecute(d, cacheKey, func() string {
		query := j.queryTemplate
		for k, v := range values {
			query = strings.ReplaceAll(query, "{"+k+"}", v)
		}
		return query
	})
}

// cachedExecute checks the cache, executes the query if needed, and caches the result.
func (j *JSONFile) cachedExecute(d *jsonData, cacheKey string, buildQuery func() string) map[string]string {
	// Check cache.
	j.mu.Lock()
	if entry, ok := j.cache[cacheKey]; ok {
		if time.Now().Before(entry.expires) {
			j.mu.Unlock()
			return entry.result
		}
	}
	j.mu.Unlock()

	result := j.execute(d.root, buildQuery())

	// Cache the result (including nil for negative caching).
	j.mu.Lock()
	if len(j.cache) >= j.cacheSize {
		clear(j.cache)
	}
	j.cache[cacheKey] = cacheEntry{result: result, expires: time.Now().Add(j.cacheTTL)}

	// Discover suffixes from first successful result.
	if result != nil {
		dd := j.data.Load()
		if dd != nil && dd.suffixes == nil {
			keys := make([]string, 0, len(result))
			for k := range result {
				keys = append(keys, k)
			}
			dd.suffixes = keys
		}
	}
	j.mu.Unlock()

	return result
}

// execute runs the JSONPath query against the root data.
func (j *JSONFile) execute(root any, query string) map[string]string {

	code, err := CompileJQ(query)
	if err != nil {
		return nil
	}

	nodes := jqSelect(code, root)
	if len(nodes) == 0 {
		return nil
	}

	// If no response paths, flatten query results directly.
	if len(j.responsePaths) == 0 {
		merged := make(map[string]string)
		for _, node := range nodes {
			mergeNode(merged, query, node)
		}
		if len(merged) == 0 {
			return nil
		}
		return merged
	}

	// Apply response paths to each query result and merge.
	merged := make(map[string]string)
	for _, node := range nodes {
		for _, hp := range j.responsePaths {
			subNodes := jqSelect(hp.parsed, node)
			for _, sub := range subNodes {
				mergeNode(merged, hp.raw, sub)
			}
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

// Load memory-maps a JSON file and parses it into the lookup tree.
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

	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: int64→int safe on 64-bit
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

	newData := &jsonData{
		root:     root,
		mmapData: mmapData,
		file:     f,
	}

	old := j.data.Swap(newData)

	// Flush cache on reload.
	j.mu.Lock()
	clear(j.cache)
	j.mu.Unlock()

	if old != nil {
		old.close()
	}

	return nil
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
