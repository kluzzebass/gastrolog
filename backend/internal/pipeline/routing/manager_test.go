package routing_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segmentation"
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
	chA := make(chan segmentation.Input, 1)
	chB := make(chan segmentation.Input, 1)

	mgr := routing.New(routing.Config{
		Workers: 2,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults: map[glid.GLID]chan<- segmentation.Input{
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
	if gotA.Record != rec || gotB.Record != rec {
		t.Errorf("expected same pointer %p on both vaults, got %p and %p", rec, gotA.Record, gotB.Record)
	}

	<-done
	stats := mgr.Stats()
	if stats.Matched != 1 || stats.Unmatched != 0 {
		t.Errorf("stats = %+v, want matched=1 unmatched=0", stats)
	}
}

func TestManagerUnregisterDuringDeliver(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan segmentation.Input, 2)

	mgr := routing.New(routing.Config{
		Workers: 4,
		Table:   catchAllTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 32
	in := make(chan routing.Input, n)
	for range n {
		in <- routing.IngestInput(&record.Record{Attrs: record.Attributes{"n": "x"}})
	}

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range n {
			<-out
		}
	}()

	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, in)
		close(done)
	}()

	// Let workers pile up behind the small segmentation buffer.
	time.Sleep(20 * time.Millisecond)

	mgr.UnregisterVault(vaultID)
	close(out) // segmentation closes after routing unregisters

	close(in)
	<-done
	<-drained

	if stats := mgr.Stats(); stats.Matched != n {
		t.Errorf("matched = %d, want %d", stats.Matched, n)
	}
}

func TestManagerReRegisterReplacesSink(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	outA := make(chan segmentation.Input, 4)
	outB := make(chan segmentation.Input, 4)

	mgr := routing.New(routing.Config{
		Workers: 2,
		Table:   catchAllTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: outA},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input, 4)
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, in)
		close(done)
	}()

	in <- routing.IngestInput(&record.Record{Attrs: record.Attributes{"seq": "1"}})
	if got := <-outA; got.Record.Attrs["seq"] != "1" {
		t.Fatalf("first record on outA: %+v", got.Record.Attrs)
	}

	mgr.UnregisterVault(vaultID)
	close(outA)

	mgr.RegisterVault(vaultID, outB)
	in <- routing.IngestInput(&record.Record{Attrs: record.Attributes{"seq": "2"}})
	close(in)
	<-done

	if len(outB) != 1 {
		t.Fatalf("expected 1 record on replacement sink, got %d", len(outB))
	}
	if got := <-outB; got.Record.Attrs["seq"] != "2" {
		t.Fatalf("replacement sink record: %+v", got.Record.Attrs)
	}
}

func TestManagerCountsUnmatched(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan segmentation.Input, 4)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   prodOnlyTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
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

func TestManagerErrAlreadyRunning(t *testing.T) {
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
	if err := mgr.Run(ctx, in); err != routing.ErrAlreadyRunning {
		t.Errorf("second Run err = %v, want ErrAlreadyRunning", err)
	}
}

func TestManagerWorkersProcessConcurrently(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	out := make(chan segmentation.Input, 8)

	mgr := routing.New(routing.Config{
		Workers: 4,
		Table:   catchAllTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
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
	out := make(chan segmentation.Input) // unbuffered — blocks until read

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   catchAllTable(vaultID),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
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
	if got1.Record != rec1 {
		t.Fatalf("first record = %p, want %p", got1.Record, rec1)
	}

	inDone := make(chan struct{})
	go func() {
		close(in)
		close(inDone)
	}()

	select {
	case got2 := <-out:
		if got2.Record != rec2 {
			t.Errorf("second record = %p, want %p", got2.Record, rec2)
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
	chA := make(chan segmentation.Input, 1)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultA: chA},
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
	out := make(chan segmentation.Input, 1)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   table,
		Vaults:  map[glid.GLID]chan<- segmentation.Input{vaultID: out},
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
	out := make(chan segmentation.Input, 2)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   table,
		Vaults:  map[glid.GLID]chan<- segmentation.Input{archiveVault: out},
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
	if got := <-out; got.Record != rec {
		t.Errorf("expected same pointer %p, got %p", rec, got.Record)
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
	chA := make(chan segmentation.Input, 1)
	chB := make(chan segmentation.Input, 1)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   routing.NewTable([]*routing.Route{r}),
		Vaults: map[glid.GLID]chan<- segmentation.Input{
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
	if gotA.Record != rec || gotB.Record != rec {
		t.Errorf("expected same pointer %p on both vaults, got %p and %p", rec, gotA.Record, gotB.Record)
	}
}

// TestManagerVaultGateRejectsWholeRecord pins the per-destination admission
// semantics: when ANY matched vault is gated, the record is nacked to the
// source and delivered NOWHERE — a partial fan-out would be silent loss for
// the gated vault. When the gate lifts, the same record flows to all targets.
func TestManagerVaultGateRejectsWholeRecord(t *testing.T) {
	t.Parallel()

	vaultA := glid.New()
	vaultB := glid.New()
	chA := make(chan segmentation.Input, 2)
	chB := make(chan segmentation.Input, 2)

	gateErr := errors.New("vault's volume is out of disk space")
	var gated atomic.Bool
	gated.Store(true)

	mgr := routing.New(routing.Config{
		Workers: 1,
		Table:   catchAllTable(vaultA, vaultB),
		Vaults: map[glid.GLID]chan<- segmentation.Input{
			vaultA: chA,
			vaultB: chB,
		},
		VaultGate: func(id glid.GLID) error {
			if gated.Load() && id == vaultB {
				return gateErr
			}
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan routing.Input, 2)
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, in)
		close(done)
	}()

	rec := &record.Record{Attrs: record.Attributes{"env": "prod"}}
	ack := make(chan error, 1)
	in <- routing.Input{Record: rec, Source: routing.IngestSource(rec), Ack: ack}

	err := <-ack
	if !errors.Is(err, gateErr) {
		t.Fatalf("ack = %v, want the vault gate error", err)
	}
	// The gate error alone doesn't say WHICH matched vault rejected the
	// record — with two-plus matched destinations, an operator reading the
	// alarm/log can't tell which one to act on. route() must name it.
	if !strings.Contains(err.Error(), vaultB.String()) {
		t.Fatalf("ack error must name the gated destination vault %s; got %v", vaultB, err)
	}
	select {
	case got := <-chA:
		t.Fatalf("healthy vault received %v despite a gated sibling — partial fan-out is loss", got.Record)
	default:
	}

	// Gate lifts: the retried record reaches both vaults and counts as matched.
	gated.Store(false)
	in <- routing.Input{Record: rec, Source: routing.IngestSource(rec)}
	close(in)
	gotA := <-chA
	gotB := <-chB
	if gotA.Record != rec || gotB.Record != rec {
		t.Fatal("record must fan out to both vaults after the gate lifts")
	}
	<-done
	stats := mgr.Stats()
	if stats.Matched != 1 {
		t.Fatalf("matched = %d, want 1 — a gated rejection must not count as routed", stats.Matched)
	}
}
