package tail

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"gastrolog/internal/orchestrator"
)

// tailedFile tracks the state of a single file being tailed.
type tailedFile struct {
	path    string
	inode   uint64
	offset  int64
	lineBuf []byte // partial line from last read
	file    *os.File
}

// newIngester creates a tail ingester from parsed config.
func newIngester(cfg config) *ingester {
	return &ingester{
		id:           cfg.ID,
		patterns:     cfg.Patterns,
		pollInterval: cfg.PollInterval,
		stateFile:    cfg.StateFile,
		logger:       cfg.Logger,
		files:        make(map[string]*tailedFile),
	}
}

type ingester struct {
	id           string
	patterns     []string
	pollInterval time.Duration
	stateFile    string
	logger       *slog.Logger

	mu    sync.Mutex
	files map[string]*tailedFile
}

// Run implements orchestrator.Ingester.
func (ing *ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	// Load bookmarks.
	bm, err := loadBookmarks(ing.stateFile)
	if err != nil {
		ing.logger.Warn("failed to load bookmarks, starting fresh", "error", err)
		bm = bookmarks{Files: make(map[string]fileBookmark)}
	}

	// Discover initial files.
	paths, err := discoverFiles(ing.patterns)
	if err != nil {
		return err
	}

	// Open and seek to bookmarked offsets (or EOF if no bookmark).
	for _, path := range paths {
		ing.openFile(path, bm)
	}

	// Set up fsnotify watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()

	// Watch parent directories for new file creation.
	for _, dir := range watchDirsForPatterns(ing.patterns) {
		if err := watcher.Add(dir); err != nil {
			ing.logger.Warn("failed to watch directory", "dir", dir, "error", err)
		}
	}

	// Initial read of all files.
	ing.mu.Lock()
	for _, tf := range ing.files {
		ing.readNewLines(tf, out)
	}
	ing.mu.Unlock()

	// Set up poll ticker.
	var ticker *time.Ticker
	var tickCh <-chan time.Time
	if ing.pollInterval > 0 {
		ticker = time.NewTicker(ing.pollInterval)
		tickCh = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			ing.saveAndCleanup(bm)
			return nil

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			ing.handleFSEvent(event, bm, out, watcher)

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			ing.logger.Warn("fsnotify error", "error", err)

		case <-tickCh:
			ing.poll(bm, out, watcher)
		}
	}
}

// openFile opens a file and seeks to the bookmarked offset, or EOF if no bookmark.
func (ing *ingester) openFile(path string, bm bookmarks) {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	if _, exists := ing.files[path]; exists {
		return
	}

	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		ing.logger.Warn("failed to open file", "path", path, "error", err)
		return
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		ing.logger.Warn("failed to stat file", "path", path, "error", err)
		return
	}

	inode, _ := getInode(info)

	tf := &tailedFile{
		path:  path,
		inode: inode,
		file:  f,
	}

	// Check for bookmark.
	if fb, ok := bm.Files[path]; ok && fb.Inode == inode && fb.Offset <= info.Size() {
		tf.offset = fb.Offset
	} else {
		// No valid bookmark — seek to end to avoid flooding.
		tf.offset = info.Size()
	}

	if _, err := f.Seek(tf.offset, io.SeekStart); err != nil {
		_ = f.Close()
		ing.logger.Warn("failed to seek", "path", path, "error", err)
		return
	}

	ing.files[path] = tf
	ing.logger.Debug("tailing file", "path", path, "offset", tf.offset)
}

// readNewLines reads complete lines from a tailed file and emits them.
// Caller must hold ing.mu.
func (ing *ingester) readNewLines(tf *tailedFile, out chan<- orchestrator.IngestMessage) {
	info, err := os.Stat(tf.path)
	if err != nil {
		ing.logger.Warn("failed to stat file during read", "path", tf.path, "error", err)
		return
	}

	// Check for inode change (file was rotated/replaced).
	if newInode, ok := getInode(info); ok && tf.inode != 0 && newInode != tf.inode {
		ing.logger.Info("inode change detected, reopening", "path", tf.path)
		_ = tf.file.Close()
		f, err := os.Open(tf.path)
		if err != nil {
			ing.logger.Warn("failed to reopen after rotation", "path", tf.path, "error", err)
			return
		}
		newInfo, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return
		}
		tf.file = f
		tf.inode, _ = getInode(newInfo)
		tf.offset = 0
		tf.lineBuf = nil
	}

	// Check for truncation (file size < our offset).
	if info.Size() < tf.offset {
		ing.logger.Info("truncation detected, resetting", "path", tf.path)
		tf.offset = 0
		tf.lineBuf = nil
		if _, err := tf.file.Seek(0, io.SeekStart); err != nil {
			return
		}
	}

	// Nothing new to read.
	if info.Size() == tf.offset {
		return
	}

	// Seek to our current offset.
	if _, err := tf.file.Seek(tf.offset, io.SeekStart); err != nil {
		return
	}

	now := time.Now()
	scanner := bufio.NewScanner(tf.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB max line

	for scanner.Scan() {
		line := scanner.Bytes()

		// If we had a partial line buffered, prepend it.
		if len(tf.lineBuf) > 0 {
			line = append(tf.lineBuf, line...)
			tf.lineBuf = nil
		}

		// Strip trailing \r if present (handles \r\n).
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}

		// Skip empty lines.
		if len(line) == 0 {
			continue
		}

		// Make a copy since scanner reuses the buffer.
		raw := make([]byte, len(line))
		copy(raw, line)

		out <- orchestrator.IngestMessage{
			Attrs: map[string]string{
				"ingester_type": "tail",
				"file":          tf.path,
			},
			Raw:        raw,
			IngestTS:   now,
			IngesterID: ing.id,
		}
	}

	ing.updateOffset(tf, info, scanner.Err())
}

func (ing *ingester) updateOffset(tf *tailedFile, info os.FileInfo, scanErr error) {
	newOffset, err := tf.file.Seek(0, io.SeekCurrent)
	if err != nil || scanErr != nil {
		return
	}
	ing.bufferPartialLine(tf, info, newOffset)
	tf.offset = newOffset
}

func (ing *ingester) bufferPartialLine(tf *tailedFile, info os.FileInfo, newOffset int64) {
	if newOffset >= info.Size() {
		return
	}
	remaining := make([]byte, info.Size()-newOffset)
	n, _ := tf.file.ReadAt(remaining, newOffset)
	if n > 0 {
		tf.lineBuf = append(tf.lineBuf, remaining[:n]...)
	}
}

// handleFSEvent processes a filesystem notification event.
func (ing *ingester) handleFSEvent(event fsnotify.Event, bm bookmarks, out chan<- orchestrator.IngestMessage, _ *fsnotify.Watcher) {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	switch {
	case event.Has(fsnotify.Write):
		if tf, ok := ing.files[event.Name]; ok {
			ing.readNewLines(tf, out)
		}

	case event.Has(fsnotify.Create):
		// Check if new file matches our patterns.
		if matchesAnyPattern(event.Name, ing.patterns) {
			// Unlock to call openFile (which takes the lock).
			ing.mu.Unlock()
			ing.openFile(event.Name, bm)
			ing.mu.Lock()
			if tf, ok := ing.files[event.Name]; ok {
				// For newly created files, start from offset 0.
				tf.offset = 0
				if _, err := tf.file.Seek(0, io.SeekStart); err == nil {
					ing.readNewLines(tf, out)
				}
			}
		}

	case event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename):
		if tf, ok := ing.files[event.Name]; ok {
			_ = tf.file.Close()
			delete(ing.files, event.Name)
			ing.logger.Debug("file removed/renamed", "path", event.Name)
		}
	}
}

// poll re-evaluates globs, reads all files, and saves bookmarks.
func (ing *ingester) poll(bm bookmarks, out chan<- orchestrator.IngestMessage, _ *fsnotify.Watcher) {
	// Discover files and open any new ones.
	paths, err := discoverFiles(ing.patterns)
	if err != nil {
		ing.logger.Warn("poll discovery failed", "error", err)
	} else {
		for _, path := range paths {
			ing.openFile(path, bm)
		}
	}

	// Read all files.
	ing.mu.Lock()
	for _, tf := range ing.files {
		ing.readNewLines(tf, out)
	}

	// Update bookmarks.
	for path, tf := range ing.files {
		bm.Files[path] = fileBookmark{
			Inode:  tf.inode,
			Offset: tf.offset,
		}
	}
	ing.mu.Unlock()

	if err := saveBookmarks(ing.stateFile, bm); err != nil {
		ing.logger.Warn("failed to save bookmarks", "error", err)
	}
}

// saveAndCleanup saves bookmarks and closes all files.
func (ing *ingester) saveAndCleanup(bm bookmarks) {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	for path, tf := range ing.files {
		bm.Files[path] = fileBookmark{
			Inode:  tf.inode,
			Offset: tf.offset,
		}
		_ = tf.file.Close()
	}

	if err := saveBookmarks(ing.stateFile, bm); err != nil {
		ing.logger.Warn("failed to save bookmarks on shutdown", "error", err)
	}
}

// getInode extracts the inode number from file info.
func getInode(info os.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Ino, true
}
