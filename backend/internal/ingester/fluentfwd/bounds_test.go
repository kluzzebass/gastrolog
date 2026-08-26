package fluentfwd

import (
	"bytes"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"gastrolog/internal/ingester/limits"
	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

func startBoundedFluent(t *testing.T) (string, chan ingestion.IngesterMessage, *logtest.Recorder) {
	t.Helper()
	logger, logs := logtest.New()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	out := make(chan ingestion.IngesterMessage, 4)
	ing := New(Config{ID: "test-fwd", Addr: addr, Logger: logger})
	go func() { _ = ing.Run(t.Context(), out) }()
	waitDialable(t, addr)
	return addr, out, logs
}

// TestRecordFieldFloodIsBounded proves a record carrying tens of thousands
// of fields cannot explode the attribute map — or, downstream, index
// cardinality. The log line itself still lands: fields are dropped, the
// record is not.
func TestRecordFieldFloodIsBounded(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedFluent(t)

	record := map[string]any{"message": "the log line"}
	for i := range 50_000 {
		record["k"+strconv.Itoa(i)] = "v"
	}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(3)
	_ = enc.EncodeString("app.log")
	_ = enc.EncodeInt(time.Now().Unix())
	_ = enc.EncodeMap(record)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case msg := <-out:
		fromProducer := 0
		for k := range msg.Attrs {
			if strings.HasPrefix(k, "k") {
				fromProducer++
			}
		}
		if fromProducer > limits.Records.Count {
			t.Errorf("stored %d producer attributes, past the %d ceiling", fromProducer, limits.Records.Count)
		}
		if string(msg.Raw) != "the log line" {
			t.Errorf("the record's payload was lost: %q", msg.Raw)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the record was dropped entirely; only its excess fields should have been")
	}

	if !logs.Wait(5*time.Second, "fluent record attributes dropped", "max_attrs") {
		t.Errorf("attributes were dropped silently: %s", logs)
	}
}

// TestPackedBatchLengthClaimIsRejected proves a packed-forward batch that
// declares more than the batch ceiling is refused on the claim, before the
// sender has to transmit any of it.
func TestPackedBatchLengthClaimIsRejected(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedFluent(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(2)
	_ = enc.EncodeString("app.log")
	buf.Write(bin32Header(maxDecompressedFluentBytes + 1))
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection stayed usable after an oversize batch claim")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after an oversize batch claim: %v", err)
	}
	select {
	case msg := <-out:
		t.Fatalf("an oversize batch reached the pipeline: %q", msg.Raw)
	default:
	}

	if !logs.Wait(5*time.Second, "decode packed entries") {
		t.Errorf("the rejection was silent: %s", logs)
	}
}

// bin32Header is a msgpack bin32 type byte followed by a big-endian length.
func bin32Header(n uint32) []byte {
	return []byte{0xc6, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

// TestConformingRecordsAreUntouched proves the ceiling did not change what a
// normal forwarder's records look like, including a multi-kilobyte field.
func TestConformingRecordsAreUntouched(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedFluent(t)

	stack := strings.Repeat("frame\n", 500)
	record := map[string]any{"message": "line", "level": "error", "stack": stack}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(3)
	_ = enc.EncodeString("app.log")
	_ = enc.EncodeInt(time.Now().Unix())
	_ = enc.EncodeMap(record)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case msg := <-out:
		if msg.Attrs["level"] != "error" {
			t.Errorf("field lost: level=%q", msg.Attrs["level"])
		}
		if msg.Attrs["stack"] != stack {
			t.Errorf("a %d-byte field was truncated or dropped", len(stack))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a conforming record was not ingested")
	}

	if logs.Contains("attributes dropped") {
		t.Errorf("conforming fields were reported as dropped: %s", logs)
	}
}
