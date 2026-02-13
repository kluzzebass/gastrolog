package tail

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

// collectMessages reads messages from out until the channel is drained or timeout.
func collectMessages(t *testing.T, out chan orchestrator.IngestMessage, timeout time.Duration) []orchestrator.IngestMessage {
	t.Helper()
	var msgs []orchestrator.IngestMessage
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-out:
			msgs = append(msgs, msg)
		case <-deadline:
			return msgs
		}
	}
}

func TestFactoryMissingPaths(t *testing.T) {
	factory := NewFactory()
	_, err := factory("test", map[string]string{}, nil)
	if err == nil {
		t.Fatal("expected error for missing paths")
	}
}

func TestFactoryInvalidPathsJSON(t *testing.T) {
	factory := NewFactory()
	_, err := factory("test", map[string]string{"paths": "not-json"}, nil)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestFactoryEmptyPaths(t *testing.T) {
	factory := NewFactory()
	_, err := factory("test", map[string]string{"paths": "[]"}, nil)
	if err == nil {
		t.Fatal("expected error for empty paths array")
	}
}

func TestFactoryInvalidPollInterval(t *testing.T) {
	factory := NewFactory()
	_, err := factory("test", map[string]string{
		"paths":         `["/tmp/*.log"]`,
		"poll_interval": "not-a-duration",
	}, nil)
	if err == nil {
		t.Fatal("expected error for invalid poll_interval")
	}
}

func TestFactoryNegativePollInterval(t *testing.T) {
	factory := NewFactory()
	_, err := factory("test", map[string]string{
		"paths":         `["/tmp/*.log"]`,
		"poll_interval": "-1s",
	}, nil)
	if err == nil {
		t.Fatal("expected error for negative poll_interval")
	}
}

func TestFactoryStateDir(t *testing.T) {
	factory := NewFactory()
	ing, err := factory("myid", map[string]string{
		"paths":      `["/tmp/*.log"]`,
		"_state_dir": "/data",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ti := ing.(*ingester)
	want := filepath.Join("/data", "state", "tail", "myid.json")
	if ti.stateFile != want {
		t.Errorf("stateFile = %q, want %q", ti.stateFile, want)
	}
}

func TestSingleFileTailing(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logFile, []byte("existing line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	factory := NewFactory()
	ing, err := factory("test", map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "0s", // disable polling
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()

	// Wait a moment for initial setup.
	time.Sleep(100 * time.Millisecond)

	// The existing line should NOT appear (seek to EOF on first start).
	select {
	case msg := <-out:
		t.Fatalf("unexpected message from existing content: %q", msg.Raw)
	case <-time.After(200 * time.Millisecond):
		// Good — no messages.
	}

	// Write new lines.
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("hello world\n")
	f.WriteString("second line\n")
	f.Close()

	msgs := collectMessages(t, out, 2*time.Second)
	if len(msgs) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(msgs))
	}
	if string(msgs[0].Raw) != "hello world" {
		t.Errorf("msg[0] = %q, want %q", msgs[0].Raw, "hello world")
	}
	if string(msgs[1].Raw) != "second line" {
		t.Errorf("msg[1] = %q, want %q", msgs[1].Raw, "second line")
	}

	// Check attributes.
	if msgs[0].Attrs["ingester_type"] != "tail" {
		t.Errorf("ingester_type = %q, want %q", msgs[0].Attrs["ingester_type"], "tail")
	}
	if msgs[0].Attrs["file"] != logFile {
		t.Errorf("file = %q, want %q", msgs[0].Attrs["file"], logFile)
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestCRLFLineEndings(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")

	factory := NewFactory()
	ing, err := factory("test", map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "0s",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create file after factory (will be discovered on start).
	if err := os.WriteFile(logFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()
	time.Sleep(100 * time.Millisecond)

	// Write CRLF lines.
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("line one\r\n")
	f.WriteString("line two\r\n")
	f.Close()

	msgs := collectMessages(t, out, 2*time.Second)
	if len(msgs) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(msgs))
	}
	if string(msgs[0].Raw) != "line one" {
		t.Errorf("msg[0] = %q, want %q", msgs[0].Raw, "line one")
	}
	if string(msgs[1].Raw) != "line two" {
		t.Errorf("msg[1] = %q, want %q", msgs[1].Raw, "line two")
	}

	cancel()
	<-errCh
}

func TestMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	log1 := filepath.Join(dir, "a.log")
	log2 := filepath.Join(dir, "b.log")
	os.WriteFile(log1, nil, 0o644)
	os.WriteFile(log2, nil, 0o644)

	factory := NewFactory()
	ing, err := factory("test", map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "0s",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()
	time.Sleep(100 * time.Millisecond)

	// Write to both files.
	f1, _ := os.OpenFile(log1, os.O_APPEND|os.O_WRONLY, 0o644)
	f1.WriteString("from file a\n")
	f1.Close()

	f2, _ := os.OpenFile(log2, os.O_APPEND|os.O_WRONLY, 0o644)
	f2.WriteString("from file b\n")
	f2.Close()

	msgs := collectMessages(t, out, 2*time.Second)
	if len(msgs) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(msgs))
	}

	// Verify both files represented.
	files := make(map[string]bool)
	for _, msg := range msgs {
		files[msg.Attrs["file"]] = true
	}
	if !files[log1] || !files[log2] {
		t.Errorf("expected messages from both files, got: %v", files)
	}

	cancel()
	<-errCh
}

func TestTruncationDetection(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	os.WriteFile(logFile, nil, 0o644)

	factory := NewFactory()
	ing, err := factory("test", map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "100ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()
	time.Sleep(100 * time.Millisecond)

	// Write some lines.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("line before truncate\n")
	f.Close()

	msgs := collectMessages(t, out, time.Second)
	if len(msgs) < 1 {
		t.Fatal("expected at least 1 message before truncation")
	}

	// Truncate the file and write new content.
	os.WriteFile(logFile, []byte("after truncate\n"), 0o644)

	msgs = collectMessages(t, out, 2*time.Second)
	if len(msgs) < 1 {
		t.Fatal("expected at least 1 message after truncation")
	}
	if string(msgs[0].Raw) != "after truncate" {
		t.Errorf("msg = %q, want %q", msgs[0].Raw, "after truncate")
	}

	cancel()
	<-errCh
}

func TestBookmarkRoundTrip(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")

	bm := bookmarks{
		Files: map[string]fileBookmark{
			"/var/log/app.log": {Inode: 12345, Offset: 98765},
			"/var/log/sys.log": {Inode: 67890, Offset: 54321},
		},
	}

	if err := saveBookmarks(stateFile, bm); err != nil {
		t.Fatal(err)
	}

	loaded, err := loadBookmarks(stateFile)
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(loaded.Files))
	}

	fb := loaded.Files["/var/log/app.log"]
	if fb.Inode != 12345 || fb.Offset != 98765 {
		t.Errorf("app.log bookmark = %+v, want inode=12345 offset=98765", fb)
	}
}

func TestBookmarkLoadMissing(t *testing.T) {
	bm, err := loadBookmarks("/nonexistent/path.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(bm.Files) != 0 {
		t.Errorf("expected empty bookmarks, got %d files", len(bm.Files))
	}
}

func TestBookmarkResumeFromOffset(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	stateFile := filepath.Join(dir, "state.json")

	// Write initial content.
	if err := os.WriteFile(logFile, []byte("line one\nline two\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Get inode for bookmark.
	info, err := os.Stat(logFile)
	if err != nil {
		t.Fatal(err)
	}
	inode, _ := getInode(info)

	// Create bookmark at the end of existing content.
	bm := bookmarks{
		Files: map[string]fileBookmark{
			logFile: {Inode: inode, Offset: info.Size()},
		},
	}
	if err := saveBookmarks(stateFile, bm); err != nil {
		t.Fatal(err)
	}

	factory := NewFactory()
	ing, err := factory("test", map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "0s",
		"_state_dir":    dir,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Override state file to use our custom one.
	ing.(*ingester).stateFile = stateFile

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()
	time.Sleep(100 * time.Millisecond)

	// Should not see existing lines since bookmarked past them.
	select {
	case msg := <-out:
		t.Fatalf("unexpected message from bookmarked content: %q", msg.Raw)
	case <-time.After(200 * time.Millisecond):
	}

	// Write new lines.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("line three\n")
	f.Close()

	msgs := collectMessages(t, out, 2*time.Second)
	if len(msgs) < 1 {
		t.Fatal("expected at least 1 message")
	}
	if string(msgs[0].Raw) != "line three" {
		t.Errorf("msg = %q, want %q", msgs[0].Raw, "line three")
	}

	cancel()
	<-errCh
}

func TestGlobDiscovery(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	os.MkdirAll(sub, 0o755)

	os.WriteFile(filepath.Join(dir, "a.log"), []byte("a"), 0o644)
	os.WriteFile(filepath.Join(sub, "b.log"), []byte("b"), 0o644)
	os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("x"), 0o644)

	// Test simple glob.
	files, err := discoverFiles([]string{filepath.Join(dir, "*.log")})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file from *.log, got %d: %v", len(files), files)
	}

	// Test ** recursive glob.
	files, err = discoverFiles([]string{filepath.Join(dir, "**", "*.log")})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files from **/*.log, got %d: %v", len(files), files)
	}
}

func TestWatchDirsForPatterns(t *testing.T) {
	dirs := watchDirsForPatterns([]string{
		"/var/log/*.log",
		"/var/log/app/**/*.log",
		"/tmp/test.log",
	})

	expected := map[string]bool{
		"/var/log":     true,
		"/var/log/app": true,
		"/tmp":         true,
	}

	if len(dirs) != len(expected) {
		t.Errorf("expected %d dirs, got %d: %v", len(expected), len(dirs), dirs)
	}
	for _, d := range dirs {
		if !expected[d] {
			t.Errorf("unexpected dir %q", d)
		}
	}
}

func TestPollDetectsNewFile(t *testing.T) {
	dir := t.TempDir()

	factory := NewFactory()
	ing, err := factory("test", map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "200ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 100)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ing.Run(ctx, out)
	}()
	time.Sleep(100 * time.Millisecond)

	// Create a new log file after the ingester started.
	logFile := filepath.Join(dir, "new.log")
	os.WriteFile(logFile, []byte("new file line\n"), 0o644)

	// Wait for poll to discover.
	msgs := collectMessages(t, out, 2*time.Second)
	if len(msgs) < 1 {
		t.Fatal("expected at least 1 message from newly created file")
	}

	cancel()
	<-errCh
}
