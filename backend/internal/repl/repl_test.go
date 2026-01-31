package repl

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memsource "gastrolog/internal/index/memory/source"
	memtime "gastrolog/internal/index/memory/time"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/source"
)

func setupTestSystem(t *testing.T) (*orchestrator.Orchestrator, *source.Registry, chunk.ChunkManager) {
	t.Helper()

	// Create memory-based chunk manager.
	cm, err := chunkmem.NewManager(chunkmem.Config{MaxRecords: 10000})
	if err != nil {
		t.Fatalf("create chunk manager: %v", err)
	}

	// Create memory-based index manager.
	timeIdx := memtime.NewIndexer(cm, 1)
	srcIdx := memsource.NewIndexer(cm)
	tokIdx := memtoken.NewIndexer(cm)
	im := indexmem.NewManager([]index.Indexer{timeIdx, srcIdx, tokIdx}, timeIdx, srcIdx, tokIdx, nil)

	// Create source registry.
	sources, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("create source registry: %v", err)
	}

	// Create query engine.
	qe := query.New(cm, im, nil)

	// Create orchestrator.
	orch := orchestrator.New(orchestrator.Config{
		Sources: sources,
	})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", qe)

	return orch, sources, cm
}

func TestREPL_Help(t *testing.T) {
	orch, sources, _ := setupTestSystem(t)
	defer sources.Close()

	input := "help\nexit\n"
	output := &bytes.Buffer{}

	r := NewSimple(orch, sources, strings.NewReader(input), output)
	if err := r.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}

	out := output.String()
	if !strings.Contains(out, "Commands:") {
		t.Errorf("help output missing 'Commands:': %s", out)
	}
	if !strings.Contains(out, "sources") {
		t.Errorf("help output missing 'sources': %s", out)
	}
	if !strings.Contains(out, "query") {
		t.Errorf("help output missing 'query': %s", out)
	}
}

func TestREPL_Sources(t *testing.T) {
	orch, sources, _ := setupTestSystem(t)
	defer sources.Close()

	// Create some sources.
	sources.Resolve(map[string]string{"env": "prod", "service": "api"})
	sources.Resolve(map[string]string{"env": "prod", "service": "web"})
	sources.Resolve(map[string]string{"env": "dev", "service": "api"})

	t.Run("list all", func(t *testing.T) {
		input := "sources\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		// Should show all 3 sources.
		lines := strings.Split(out, "\n")
		sourceLines := 0
		for _, line := range lines {
			if strings.Contains(line, "env=") {
				sourceLines++
			}
		}
		if sourceLines != 3 {
			t.Errorf("expected 3 source lines, got %d: %s", sourceLines, out)
		}
	})

	t.Run("filter by attribute", func(t *testing.T) {
		input := "sources env=prod\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		lines := strings.Split(out, "\n")
		sourceLines := 0
		for _, line := range lines {
			if strings.Contains(line, "env=") {
				sourceLines++
			}
		}
		if sourceLines != 2 {
			t.Errorf("expected 2 source lines for env=prod, got %d: %s", sourceLines, out)
		}
	})

	t.Run("no matches", func(t *testing.T) {
		input := "sources env=staging\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		if !strings.Contains(out, "No sources found") {
			t.Errorf("expected 'No sources found', got: %s", out)
		}
	})
}

func TestREPL_Query(t *testing.T) {
	orch, sources, cm := setupTestSystem(t)
	defer sources.Close()

	// Start orchestrator for ingestion.
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("start orchestrator: %v", err)
	}
	defer orch.Stop()

	// Ingest some records.
	src1 := sources.Resolve(map[string]string{"service": "api"})
	src2 := sources.Resolve(map[string]string{"service": "web"})

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		rec := chunk.Record{
			IngestTS: baseTime.Add(time.Duration(i) * time.Second),
			SourceID: src1,
			Raw:      []byte("error from api"),
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		rec := chunk.Record{
			IngestTS: baseTime.Add(time.Duration(i) * time.Second),
			SourceID: src2,
			Raw:      []byte("info from web"),
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	t.Run("query all", func(t *testing.T) {
		input := "query\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		// When all records are returned, it shows "(end of results)" instead of "records shown"
		if !strings.Contains(out, "records") {
			t.Errorf("expected records message: %s", out)
		}
	})

	t.Run("query with token filter", func(t *testing.T) {
		input := "query token=error\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		// Should only show api records with "error".
		if !strings.Contains(out, "error from api") {
			t.Errorf("expected 'error from api' in output: %s", out)
		}
	})

	t.Run("query with limit", func(t *testing.T) {
		input := "query limit=3\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		// Count actual record lines (lines containing the source ID pattern).
		lines := strings.Split(out, "\n")
		recordLines := 0
		for _, line := range lines {
			// Record lines have a UUID pattern and a timestamp.
			if strings.Contains(line, "error from api") || strings.Contains(line, "info from web") {
				recordLines++
			}
		}
		if recordLines != 3 {
			t.Errorf("expected 3 record lines with limit=3, got %d: %s", recordLines, out)
		}
	})
}

func TestREPL_NextAndReset(t *testing.T) {
	orch, sources, cm := setupTestSystem(t)
	defer sources.Close()

	// Start orchestrator for ingestion.
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("start orchestrator: %v", err)
	}
	defer orch.Stop()

	// Ingest 25 records.
	src := sources.Resolve(map[string]string{"service": "api"})
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 25; i++ {
		rec := chunk.Record{
			IngestTS: baseTime.Add(time.Duration(i) * time.Second),
			SourceID: src,
			Raw:      []byte("log message"),
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	t.Run("pagination with next", func(t *testing.T) {
		input := "query\nnext\nnext\nnext\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		// After exhausting 25 records (10 + 10 + 5), the 4th next shows "No active query"
		// because resultIter is set to nil after the iterator ends.
		if !strings.Contains(out, "No active query") {
			t.Errorf("expected 'No active query' after exhausting iterator: %s", out)
		}
		// Count record lines to verify all 25 were shown.
		lines := strings.Split(out, "\n")
		recordLines := 0
		for _, line := range lines {
			if strings.Contains(line, "log message") {
				recordLines++
			}
		}
		if recordLines != 25 {
			t.Errorf("expected 25 record lines, got %d: %s", recordLines, out)
		}
	})

	t.Run("next without query", func(t *testing.T) {
		input := "next\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		if !strings.Contains(out, "No active query") {
			t.Errorf("expected 'No active query': %s", out)
		}
	})

	t.Run("reset clears state", func(t *testing.T) {
		input := "query\nreset\nnext\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		if !strings.Contains(out, "Query state cleared") {
			t.Errorf("expected 'Query state cleared': %s", out)
		}
		if !strings.Contains(out, "No active query") {
			t.Errorf("expected 'No active query' after reset: %s", out)
		}
	})
}

func TestREPL_Store(t *testing.T) {
	orch, sources, _ := setupTestSystem(t)
	defer sources.Close()

	t.Run("get default store", func(t *testing.T) {
		input := "store\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		if !strings.Contains(out, "Current store: default") {
			t.Errorf("expected 'Current store: default': %s", out)
		}
	})

	t.Run("set store", func(t *testing.T) {
		input := "store archive\nstore\nexit\n"
		output := &bytes.Buffer{}

		r := NewSimple(orch, sources, strings.NewReader(input), output)
		if err := r.Run(); err != nil {
			t.Fatalf("run: %v", err)
		}

		out := output.String()
		if !strings.Contains(out, "Store set to: archive") {
			t.Errorf("expected 'Store set to: archive': %s", out)
		}
		if !strings.Contains(out, "Current store: archive") {
			t.Errorf("expected 'Current store: archive': %s", out)
		}
	})
}

func TestREPL_UnknownCommand(t *testing.T) {
	orch, sources, _ := setupTestSystem(t)
	defer sources.Close()

	input := "foobar\nexit\n"
	output := &bytes.Buffer{}

	r := NewSimple(orch, sources, strings.NewReader(input), output)
	if err := r.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}

	out := output.String()
	if !strings.Contains(out, "Unknown command: foobar") {
		t.Errorf("expected 'Unknown command': %s", out)
	}
}

func TestREPL_Exit(t *testing.T) {
	orch, sources, _ := setupTestSystem(t)
	defer sources.Close()

	for _, cmd := range []string{"exit", "quit"} {
		t.Run(cmd, func(t *testing.T) {
			input := cmd + "\n"
			output := &bytes.Buffer{}

			r := NewSimple(orch, sources, strings.NewReader(input), output)
			if err := r.Run(); err != nil {
				t.Fatalf("run: %v", err)
			}
			// Should exit cleanly without error.
		})
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		input string
		want  time.Time
		err   bool
	}{
		{"2024-01-01T00:00:00Z", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"2024-01-01T12:30:45.123456789Z", time.Date(2024, 1, 1, 12, 30, 45, 123456789, time.UTC), false},
		{"1704067200", time.Unix(1704067200, 0), false},
		{"invalid", time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseTime(tt.input)
			if tt.err {
				if err == nil {
					t.Errorf("expected error for %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for %q: %v", tt.input, err)
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("parseTime(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
