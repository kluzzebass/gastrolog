package routing_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/record"
)

func catchAllTable(vaultIDs ...glid.GLID) *routing.Table {
	r, err := routing.CompileRoute(glid.New(), "all", 0, "*", vaultIDs)
	if err != nil {
		panic(err)
	}
	return routing.NewTable([]*routing.Route{r})
}

func prodOnlyTable(vaultID glid.GLID) *routing.Table {
	r, err := routing.CompileRoute(glid.New(), "prod", 0, "env=prod", []glid.GLID{vaultID})
	if err != nil {
		panic(err)
	}
	return routing.NewTable([]*routing.Route{r})
}

func TestManagerFansOutSamePointer(t *testing.T) {
	t.Parallel()

	vaultA := glid.New()
	vaultB := glid.New()
	chA := make(chan *record.Record, 1)
	chB := make(chan *record.Record, 1)

	mgr := routing.New(routing.Config{
		Workers: 2,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults: map[glid.GLID]chan<- *record.Record{
			vaultA: chA,
			vaultB: chB,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &record.Record{Attrs: record.Attributes{"env": "prod"}}
	in := make(chan routing.Input, 1)
	in <- routing.IngestInput(rec)
	close(in)

	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, in)
		close(done)
	}()

	gotA := <-chA
	gotB := <-chB
	if gotA != rec || gotB != rec {
		t.Errorf("expected same pointer %p on both vaults, got %p and %p", rec, gotA, gotB)
	}

	<-done
	stats := mgr.Stats()
	if stats.Matched != 1 || stats.Unmatched != 0 {
		t.Errorf("stats = %+v, want matched=1 unmatched=0", stats)
	}
}

func TestManagerCountsUnmatched(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan *record.Record, 4)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   prodOnlyTable(vaultID),
		Vaults:  map[glid.GLID]chan<- *record.Record{vaultID: out},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input, 2)
	in <- routing.IngestInput(&record.Record{Attrs: record.Attributes{"env": "staging"}})
	in <- routing.IngestInput(&record.Record{Attrs: record.Attributes{"env": "prod"}})
	close(in)

	_ = mgr.Run(ctx, in)

	stats := mgr.Stats()
	if stats.Unmatched != 1 || stats.Matched != 1 {
		t.Errorf("stats = %+v, want matched=1 unmatched=1", stats)
	}
	if len(out) != 1 {
		t.Errorf("expected 1 routed record, got %d", len(out))
	}
}

func TestManagerErrNotRunning(t *testing.T) {
	t.Parallel()

	mgr := routing.New(routing.Config{
		Table: catchAllTable(glid.New()),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input)
	close(in)
	if err := mgr.Run(ctx, in); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := mgr.Run(ctx, in); err != routing.ErrNotRunning {
		t.Errorf("second Run err = %v, want ErrNotRunning", err)
	}
}

func TestManagerWorkersProcessConcurrently(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan *record.Record, 8)

	mgr := routing.New(routing.Config{
		Workers: 4,
		Table:   catchAllTable(vaultID),
		Vaults:  map[glid.GLID]chan<- *record.Record{vaultID: out},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input, 8)
	for range 8 {
		in <- routing.IngestInput(&record.Record{Attrs: record.Attributes{"n": "x"}})
	}
	close(in)

	start := time.Now()
	_ = mgr.Run(ctx, in)
	elapsed := time.Since(start)

	for range 8 {
		<-out
	}
	if stats := mgr.Stats(); stats.Matched != 8 {
		t.Errorf("matched = %d, want 8", stats.Matched)
	}
	_ = elapsed
}

func TestManagerBackpressureOnVaultChannel(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan *record.Record) // unbuffered — blocks until read

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   catchAllTable(vaultID),
		Vaults:  map[glid.GLID]chan<- *record.Record{vaultID: out},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec1 := &record.Record{Attrs: record.Attributes{"seq": "1"}}
	rec2 := &record.Record{Attrs: record.Attributes{"seq": "2"}}
	in := make(chan routing.Input, 2)
	in <- routing.IngestInput(rec1)
	in <- routing.IngestInput(rec2)

	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, in)
		close(done)
	}()

	got1 := <-out
	if got1 != rec1 {
		t.Fatalf("first record = %p, want %p", got1, rec1)
	}

	inDone := make(chan struct{})
	go func() {
		close(in)
		close(inDone)
	}()

	select {
	case got2 := <-out:
		if got2 != rec2 {
			t.Errorf("second record = %p, want %p", got2, rec2)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("routing blocked on full vault channel — expected delivery after read")
	}

	<-inDone
	close(out)
	<-done
}

func TestManagerSkipsUnwiredVault(t *testing.T) {
	t.Parallel()

	vaultA := glid.New()
	vaultB := glid.New()
	chA := make(chan *record.Record, 1)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults:  map[glid.GLID]chan<- *record.Record{vaultA: chA},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input, 1)
	in <- routing.IngestInput(&record.Record{})
	close(in)

	_ = mgr.Run(ctx, in)

	if len(chA) != 1 {
		t.Errorf("expected record on wired vault A, got %d", len(chA))
	}
	if stats := mgr.Stats(); stats.Matched != 1 {
		t.Errorf("matched = %d, want 1 (unwired vault skipped silently)", stats.Matched)
	}
}

func TestManagerUsesIngesterFromRecord(t *testing.T) {
	t.Parallel()

	targetIngester := glid.New()
	vaultID := glid.New()

	r, err := routing.CompileRoute(glid.New(), "ing", 0,
		`_ingester="`+targetIngester.String()+`"`, []glid.GLID{vaultID})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	table := routing.NewTable([]*routing.Route{r})
	out := make(chan *record.Record, 1)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   table,
		Vaults:  map[glid.GLID]chan<- *record.Record{vaultID: out},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input, 1)
	in <- routing.IngestInput(&record.Record{
		EventID: record.EventID{IngesterID: targetIngester},
	})
	close(in)

	_ = mgr.Run(ctx, in)

	if len(out) != 1 {
		t.Fatalf("expected routed record, stats=%+v", mgr.Stats())
	}
}

func TestManagerRoutesRetentionEject(t *testing.T) {
	t.Parallel()

	sourceVault := glid.New()
	archiveVault := glid.New()

	r, err := routing.CompileRoute(glid.New(), "eject-archive", 0,
		`_source="retention" AND _vault="`+sourceVault.String()+`"`,
		[]glid.GLID{archiveVault})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	table := routing.NewTable([]*routing.Route{r})
	out := make(chan *record.Record, 2)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   table,
		Vaults:  map[glid.GLID]chan<- *record.Record{archiveVault: out},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &record.Record{Attrs: record.Attributes{"env": "prod"}}
	in := make(chan routing.Input, 2)
	in <- routing.Input{
		Record: rec,
		Source: routing.RetentionSource(sourceVault, ""),
	}
	in <- routing.IngestInput(rec)
	close(in)

	_ = mgr.Run(ctx, in)

	if len(out) != 1 {
		t.Fatalf("expected 1 ejected record on archive vault, got %d; stats=%+v", len(out), mgr.Stats())
	}
	if got := <-out; got != rec {
		t.Errorf("expected same pointer %p, got %p", rec, got)
	}
	stats := mgr.Stats()
	if stats.Matched != 1 || stats.Unmatched != 1 {
		t.Errorf("stats = %+v, want matched=1 unmatched=1 (ingest path misses retention route)", stats)
	}
}

func TestManagerRetentionEjectSamePointerFanOut(t *testing.T) {
	t.Parallel()

	sourceVault := glid.New()
	destA := glid.New()
	destB := glid.New()

	r, err := routing.CompileRoute(glid.New(), "eject", 0, `_source="retention"`,
		[]glid.GLID{destA, destB})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	chA := make(chan *record.Record, 1)
	chB := make(chan *record.Record, 1)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   routing.NewTable([]*routing.Route{r}),
		Vaults: map[glid.GLID]chan<- *record.Record{
			destA: chA,
			destB: chB,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &record.Record{Raw: []byte("line")}
	in := make(chan routing.Input, 1)
	in <- routing.Input{
		Record: rec,
		Source: routing.RetentionSource(sourceVault, "age"),
	}
	close(in)

	_ = mgr.Run(ctx, in)

	gotA := <-chA
	gotB := <-chB
	if gotA != rec || gotB != rec {
		t.Errorf("expected same pointer %p on both vaults, got %p and %p", rec, gotA, gotB)
	}
}
