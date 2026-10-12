package jsonl

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// chunkRecords is an iterator over n records whose payloads name the chunk.
func chunkRecords(tag string, n int) chunk.RecordIterator {
	i := 0
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return func() (chunk.Record, error) {
		if i == n {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		i++
		ts := base.Add(time.Duration(i) * time.Second)
		return chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte(tag)}, nil
	}
}

// sinkLines returns each line's raw payload and write time.
func sinkLines(t *testing.T, path string) (raws []string, writeTS []time.Time) {
	t.Helper()
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("sink line %q is not a record: %v", sc.Text(), err)
		}
		raws = append(raws, rec.Raw)
		writeTS = append(writeTS, rec.WriteTS)
	}
	return raws, writeTS
}

func count(raws []string, tag string) int {
	n := 0
	for _, r := range raws {
		if r == tag {
			n++
		}
	}
	return n
}

// Each sealed chunk reaches the sink once, with its records' own write times,
// across reopens of the sink.
func TestWriteChunkWritesEachChunkOnce(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "sink.jsonl")
	m, err := NewManager(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	a, b := chunk.NewChunkID(), chunk.NewChunkID()
	if wrote, err := m.WriteChunk(a, chunkRecords("a", 3)); err != nil || !wrote {
		t.Fatalf("WriteChunk a = (%v, %v), want written", wrote, err)
	}
	if wrote, err := m.WriteChunk(a, chunkRecords("a", 3)); err != nil || wrote {
		t.Fatalf("WriteChunk a again = (%v, %v), want skipped", wrote, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewManager(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if !reopened.Written(a) {
		t.Fatal("a reopened sink does not know chunk a was written")
	}
	if wrote, err := reopened.WriteChunk(a, chunkRecords("a", 3)); err != nil || wrote {
		t.Fatalf("WriteChunk a after reopen = (%v, %v), want skipped", wrote, err)
	}
	if wrote, err := reopened.WriteChunk(b, chunkRecords("b", 2)); err != nil || !wrote {
		t.Fatalf("WriteChunk b = (%v, %v), want written", wrote, err)
	}

	raws, writeTS := sinkLines(t, path)
	if count(raws, "a") != 3 || count(raws, "b") != 2 || len(raws) != 5 {
		t.Fatalf("sink lines %v, want 3 of a and 2 of b", raws)
	}
	want := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)
	if !writeTS[0].Equal(want) {
		t.Fatalf("first line's write time %s, want the record's own %s", writeTS[0], want)
	}
}

// A crash part-way through a chunk leaves its partial lines, one of them
// torn, and no ledger entry: reopening cuts them and the chunk is written
// again whole, once.
func TestWriteChunkRewritesAChunkACrashInterrupted(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "sink.jsonl")
	m, err := NewManager(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	done := chunk.NewChunkID()
	if _, err := m.WriteChunk(done, chunkRecords("done", 2)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, path, `{"raw":"torn"}`+"\n"+`{"raw":"to`)

	reopened, err := NewManager(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	torn := chunk.NewChunkID()
	if reopened.Written(torn) {
		t.Fatal("the interrupted chunk is listed as written")
	}
	if wrote, err := reopened.WriteChunk(torn, chunkRecords("torn", 2)); err != nil || !wrote {
		t.Fatalf("WriteChunk torn = (%v, %v), want written", wrote, err)
	}
	raws, _ := sinkLines(t, path)
	if count(raws, "done") != 2 || count(raws, "torn") != 2 || len(raws) != 4 {
		t.Fatalf("sink lines %v, want 2 of done and the interrupted chunk's 2 once", raws)
	}
}

// The sink never changes bytes it did not write: a file that was already at
// the path, ending mid-line or binary, keeps its content however the sink
// later recovers.
func TestSinkNeverCutsBytesItDidNotWrite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "existing")
	existing := "operator data, no newline \x00\x01"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.WriteChunk(chunk.NewChunkID(), chunkRecords("ours", 1)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	appendRaw(t, path, `{"raw":"interr`)

	reopened, err := NewManager(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	_ = reopened.Close()
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < len(existing) || string(data[:len(existing)]) != existing {
		t.Fatalf("the file's own content changed: %q", data)
	}
	if got := string(data[len(existing):]); got == "" || got[len(got)-1] != '\n' {
		t.Fatalf("the sink's interrupted bytes were not cut back to its last chunk: %q", got)
	}
}

func appendRaw(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(text); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}

// A chunk whose records cannot be read is not listed as written, so it is
// written again on the next pass.
func TestWriteChunkReadErrorLeavesTheChunkUnwritten(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{Path: filepath.Join(t.TempDir(), "sink.jsonl")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	id := chunk.NewChunkID()
	broken := errors.New("disk read failed")
	calls := 0
	_, err = m.WriteChunk(id, func() (chunk.Record, error) {
		calls++
		if calls == 2 {
			return chunk.Record{}, broken
		}
		return chunk.Record{Raw: []byte("x")}, nil
	})
	if !errors.Is(err, broken) {
		t.Fatalf("WriteChunk error = %v, want the read error", err)
	}
	if m.Written(id) {
		t.Fatal("a chunk whose read failed is listed as written")
	}
	if wrote, err := m.WriteChunk(id, chunkRecords("x", 1)); err != nil || !wrote {
		t.Fatalf("retry WriteChunk = (%v, %v), want written", wrote, err)
	}
	// The failed attempt's line was cut: the chunk is in the sink once.
	raws, _ := sinkLines(t, m.file.Name())
	if len(raws) != 1 {
		t.Fatalf("sink lines %v, want the retried chunk's one line only", raws)
	}
}

// The sink reports no chunk of its own: a JSONL vault's chunks are its
// pipeline chunks, and a count of the lines it has written would be shown as
// one more chunk beside them.
func TestSinkReportsNoChunkOfItsOwn(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{Path: filepath.Join(t.TempDir(), "sink.jsonl")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	if _, err := m.WriteChunk(chunk.NewChunkID(), chunkRecords("a", 3)); err != nil {
		t.Fatal(err)
	}
	if active := m.Active(); active != nil {
		t.Fatalf("Active = %+v, want none", *active)
	}
	if metas, err := m.List(); err != nil || len(metas) != 0 {
		t.Fatalf("List = (%+v, %v), want none", metas, err)
	}
}
