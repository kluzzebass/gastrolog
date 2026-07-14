package digestion_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/record"
)

type enrichDigester struct {
	key   string
	value string
}

func (d *enrichDigester) Digest(msg *ingestion.Message) error {
	if msg.Attrs == nil {
		msg.Attrs = map[string]string{}
	}
	msg.Attrs[d.key] = d.value
	return nil
}

type failDigester struct {
	failRaw []byte
}

func (d *failDigester) Digest(msg *ingestion.Message) error {
	if string(msg.Raw) == string(d.failRaw) {
		return errors.New("parse failed")
	}
	return nil
}

type slowDigester struct {
	delay   time.Duration
	slowRaw []byte
}

func (d *slowDigester) Digest(msg *ingestion.Message) error {
	if len(d.slowRaw) > 0 && string(msg.Raw) != string(d.slowRaw) {
		return nil
	}
	time.Sleep(d.delay)
	return nil
}

func TestManagerBuildsRecord(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{Workers: 2, OutCapacity: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 1)
	eventID := record.EventID{
		IngesterID: glid.New(),
		NodeID:     glid.New(),
		IngestTS:   time.Now().UTC(),
		IngestSeq:  0,
	}
	in <- ingestion.Message{
		EventID:  eventID,
		Attrs:    map[string]string{"k": "v"},
		Raw:      []byte("line"),
		SourceTS: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	close(in)

	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx, in)
		close(done)
	}()

	o := <-out
	if o.Err != nil {
		t.Fatalf("unexpected error: %v", o.Err)
	}
	if o.Record.EventID != eventID {
		t.Fatalf("EventID = %+v, want %+v", o.Record.EventID, eventID)
	}
	if string(o.Record.Raw) != "line" {
		t.Fatalf("raw = %q", o.Record.Raw)
	}
	if o.Record.Attrs["k"] != "v" {
		t.Fatalf("attrs = %v", o.Record.Attrs)
	}
	if !o.Record.SourceTS.Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("SourceTS = %v", o.Record.SourceTS)
	}
	if o.Record.IngestTS != eventID.IngestTS {
		t.Fatalf("IngestTS = %v, want %v", o.Record.IngestTS, eventID.IngestTS)
	}

	<-done
	_, open := <-out
	if open {
		t.Fatal("expected output channel closed")
	}
}

func TestManagerDigesterChain(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{
		Workers: 1,
		Digesters: []digestion.Digester{
			&enrichDigester{key: "a", value: "1"},
			&enrichDigester{key: "b", value: "2"},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 1)
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 0}, Raw: []byte("x")}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	o := <-out
	if o.Record.Attrs["a"] != "1" || o.Record.Attrs["b"] != "2" {
		t.Fatalf("attrs = %v", o.Record.Attrs)
	}
	for range out {
	}
}

func TestManagerParseErrorDoesNotBlockPeers(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{
		Workers:     4,
		OutCapacity: 4,
		Digesters:   []digestion.Digester{&failDigester{failRaw: []byte("bad")}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 3)
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 0}, Raw: []byte("good")}
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 1}, Raw: []byte("bad")}
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 2}, Raw: []byte("good2")}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	var ok, failed int
	for range 3 {
		o := <-out
		if o.Err != nil {
			failed++
		} else if string(o.Record.Raw) == "good" || string(o.Record.Raw) == "good2" {
			ok++
		}
	}
	if ok != 2 || failed != 1 {
		t.Fatalf("ok=%d failed=%d", ok, failed)
	}
}

func TestManagerOutOfOrderCompletion(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{
		Workers:   2,
		Digesters: []digestion.Digester{&slowDigester{delay: 80 * time.Millisecond, slowRaw: []byte("slow")}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 2)
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 0}, Raw: []byte("slow")}
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 1}, Raw: []byte("fast")}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	first := <-out
	if first.Record.EventID.IngestSeq != 1 {
		t.Fatalf("fast message should complete first, got IngestSeq=%d", first.Record.EventID.IngestSeq)
	}
}

func TestManagerRecordImmutableAfterDigest(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{Workers: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	raw := []byte("mutable")
	attrs := map[string]string{"k": "v"}
	in := make(chan ingestion.Message, 1)
	in <- ingestion.Message{
		EventID: record.EventID{IngestSeq: 0},
		Attrs:   attrs,
		Raw:     raw,
	}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	o := <-out
	raw[0] = 'X'
	attrs["k"] = "changed"

	if o.Record.Raw[0] == 'X' {
		t.Fatal("record raw mutated when source slice changed")
	}
	if o.Record.Attrs["k"] != "v" {
		t.Fatal("record attrs mutated when source map changed")
	}
}

func TestManagerPreservesAck(t *testing.T) {
	t.Parallel()

	ack := make(chan error, 1)
	mgr, out := digestion.New(digestion.Config{Workers: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 1)
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 0}, Raw: []byte("x"), Ack: ack}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	o := <-out
	if o.Ack == nil {
		t.Fatal("Ack not preserved")
	}
}

func TestManagerConcurrentWorkers(t *testing.T) {
	t.Parallel()

	const n = 32
	mgr, out := digestion.New(digestion.Config{Workers: 8, OutCapacity: n})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, n)
	for i := range n {
		in <- ingestion.Message{
			EventID: record.EventID{IngestSeq: uint32(i)},
			Raw:     []byte("x"),
		}
	}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	seen := make(map[uint32]bool)
	for range n {
		o := <-out
		if o.Err != nil {
			t.Fatalf("unexpected err: %v", o.Err)
		}
		seen[o.Record.EventID.IngestSeq] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d records, want %d", len(seen), n)
	}
}

func TestManagerBackpressure(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{Workers: 1, OutCapacity: 2})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 3)
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 0}, Raw: []byte("1")}
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 1}, Raw: []byte("2")}
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 2}, Raw: []byte("3")}

	go func() { _ = mgr.Run(ctx, in) }()

	<-out
	<-out

	done := make(chan struct{})
	go func() {
		<-out
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("third output arrived before queue had capacity")
	}

	close(in)
	for range out {
	}
}

func TestManagerRunTwice(t *testing.T) {
	t.Parallel()

	mgr, _ := digestion.New(digestion.Config{Workers: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message)
	close(in)
	if err := mgr.Run(ctx, in); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := mgr.Run(ctx, in); !errors.Is(err, digestion.ErrAlreadyRunning) {
		t.Fatalf("second Run = %v, want ErrAlreadyRunning", err)
	}
}

func TestManagerWaitForReplica(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{Workers: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ingestion.Message, 2)
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 0}, Raw: []byte("plain")}
	in <- ingestion.Message{EventID: record.EventID{IngestSeq: 1}, Raw: []byte("ack"), Ack: make(chan error, 1)}
	close(in)

	go func() { _ = mgr.Run(ctx, in) }()

	o0 := <-out
	o1 := <-out
	if o0.Record.WaitForReplica {
		t.Fatal("plain message should not wait for replica")
	}
	if !o1.Record.WaitForReplica {
		t.Fatal("ack message should wait for replica")
	}
}

func TestManagerParallelLoad(t *testing.T) {
	t.Parallel()

	mgr, out := digestion.New(digestion.Config{
		Workers:   4,
		Digesters: []digestion.Digester{&slowDigester{delay: 5 * time.Millisecond}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 40
	in := make(chan ingestion.Message, n)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = mgr.Run(ctx, in)
	}()

	for i := range n {
		in <- ingestion.Message{EventID: record.EventID{IngestSeq: uint32(i)}, Raw: []byte("x")}
	}
	close(in)
	wg.Wait()

	count := 0
	for range out {
		count++
	}
	if count != n {
		t.Fatalf("got %d outputs, want %d", count, n)
	}
}
