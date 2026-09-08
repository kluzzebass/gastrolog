package fluentfwd

import (
	"bytes"
	"fmt"
	"net"
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

// floodKey names a field in the flood. The keys are zero-padded so that
// sorted order — the order the ceiling keeps — is also numeric order, and
// the test can name the survivors.
func floodKey(i int) string { return "k" + fmt.Sprintf("%05d", i) }

// messageModeRecord encodes a message-mode frame carrying record.
func messageModeRecord(tag string, record map[string]any) []byte {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(3)
	_ = enc.EncodeString(tag)
	_ = enc.EncodeInt(time.Now().Unix())
	_ = enc.EncodeMap(record)
	return buf.Bytes()
}

// TestRecordFieldFloodIsBounded proves a record carrying tens of thousands
// of fields cannot explode the attribute map — or, downstream, index
// cardinality. The log line itself still lands: fields are dropped, the
// record is not.
//
// It also pins WHICH fields survive. msgpack hands the record over as a Go
// map, whose iteration order is randomized, so dropping whatever the range
// happened to reach last would ingest two identical records differently and
// the same record differently on the next process. The survivors are the
// first Count in sorted order, every time.
func TestRecordFieldFloodIsBounded(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedFluent(t)

	record := map[string]any{"message": "the log line"}
	for i := range 50_000 {
		record[floodKey(i)] = "v"
	}
	frame := messageModeRecord("app.log", record)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// The same record twice: the two passes must agree, or the vault sees
	// two different records for one input.
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	first := recvBounded(t, out)
	second := recvBounded(t, out)

	fromProducer := 0
	for k := range first.Attrs {
		if strings.HasPrefix(k, "k") {
			fromProducer++
		}
	}
	if fromProducer != limits.Records.Count {
		t.Errorf("stored %d producer fields, want exactly the %d the ceiling allows", fromProducer, limits.Records.Count)
	}
	for i := range limits.Records.Count {
		if _, ok := first.Attrs[floodKey(i)]; !ok {
			t.Fatalf("%s was dropped; the survivors are not the first %d in sorted order",
				floodKey(i), limits.Records.Count)
		}
	}
	if _, ok := first.Attrs[floodKey(limits.Records.Count)]; ok {
		t.Errorf("%s survived past the ceiling", floodKey(limits.Records.Count))
	}
	if string(first.Raw) != "the log line" {
		t.Errorf("the record's payload was lost: %q", first.Raw)
	}

	if len(second.Attrs) != len(first.Attrs) {
		t.Fatalf("field count differed between passes: %d then %d", len(first.Attrs), len(second.Attrs))
	}
	for k, v := range first.Attrs {
		if second.Attrs[k] != v {
			t.Fatalf("field %q survived one pass and not the other", k)
		}
	}

	if !logs.Wait(5*time.Second, "fluent record attributes dropped", "max_attrs") {
		t.Errorf("attributes were dropped silently: %s", logs)
	}
}

// TestRecordFieldsCannotForgeTheIngestersOwnAttributes proves a producer
// cannot overwrite the attributes that say where a record came from — and
// that a record field displaced this way is reported rather than vanishing.
// "tag" is a plausible field name, so this is a real record losing a real
// field, not only an attack.
func TestRecordFieldsCannotForgeTheIngestersOwnAttributes(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedFluent(t)

	frame := messageModeRecord("app.log", map[string]any{
		"message":       "line",
		"tag":           "somewhere-else",
		"ingester_type": "syslog",
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	msg := recvBounded(t, out)
	if msg.Attrs["tag"] != "app.log" {
		t.Errorf("a record field forged the tag: %q", msg.Attrs["tag"])
	}
	if msg.Attrs["ingester_type"] != "fluentfwd" {
		t.Errorf("a record field forged ingester_type: %q", msg.Attrs["ingester_type"])
	}

	if !logs.Wait(5*time.Second, "fluent record attributes dropped", "displaced=2") {
		t.Errorf("displaced fields vanished without a word: %s", logs)
	}
}

// TestUncontestedFieldsAreNotReportedAsDisplaced proves the accounting does
// not cry wolf: a record whose fields do not collide reports nothing.
func TestUncontestedFieldsAreNotReportedAsDisplaced(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedFluent(t)

	frame := messageModeRecord("app.log", map[string]any{"message": "line", "level": "info"})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	if msg := recvBounded(t, out); msg.Attrs["level"] != "info" {
		t.Errorf("field lost: %v", msg.Attrs)
	}
	if logs.Contains("fluent record attributes dropped") {
		t.Errorf("a conforming record was reported as lossy: %s", logs)
	}
}

func recvBounded(t *testing.T, out chan ingestion.IngesterMessage) ingestion.IngesterMessage {
	t.Helper()
	select {
	case msg := <-out:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("the record was dropped entirely; only its excess fields should have been")
		return ingestion.IngesterMessage{}
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
	buf.Write(bin32Header(limits.MaxDecompressedBytes + 1))
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
	frame := messageModeRecord("app.log", map[string]any{"message": "line", "level": "error", "stack": stack})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write(frame); err != nil {
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
