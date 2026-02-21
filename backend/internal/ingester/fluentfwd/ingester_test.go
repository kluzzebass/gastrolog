package fluentfwd

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"

	"gastrolog/internal/orchestrator"
)

// dialIngester starts a Fluent Forward ingester and returns the TCP address and output channel.
func dialIngester(t *testing.T, chanSize int) (string, chan orchestrator.IngestMessage) {
	t.Helper()

	// Find a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	out := make(chan orchestrator.IngestMessage, chanSize)
	ing := New(Config{
		ID:   "test-fwd",
		Addr: addr,
	})

	ctx := t.Context()
	go ing.Run(ctx, out)
	time.Sleep(100 * time.Millisecond)

	return addr, out
}

func recv(t *testing.T, out chan orchestrator.IngestMessage) orchestrator.IngestMessage {
	t.Helper()
	select {
	case msg := <-out:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return orchestrator.IngestMessage{}
	}
}

// sendMsgpack connects to the given address, writes the msgpack-encoded data, and returns the connection.
func sendMsgpack(t *testing.T, addr string, data []byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_, err = conn.Write(data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	return conn
}

// --- Message Mode Tests ---

func TestFluentFwdMessageMode(t *testing.T) {
	addr, out := dialIngester(t, 10)

	ts := int64(1700000000)
	// Message mode: [tag, time, record]
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("app.log")
	enc.EncodeInt(ts)
	enc.EncodeMap(map[string]any{"message": "hello fluent", "level": "info"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != "hello fluent" {
		t.Errorf("raw: expected %q, got %q", "hello fluent", msg.Raw)
	}
	if msg.Attrs["tag"] != "app.log" {
		t.Errorf("tag: expected app.log, got %q", msg.Attrs["tag"])
	}
	if msg.Attrs["level"] != "info" {
		t.Errorf("level: expected info, got %q", msg.Attrs["level"])
	}
	if msg.Attrs["ingester_type"] != "fluentfwd" {
		t.Errorf("ingester_type: expected fluentfwd, got %q", msg.Attrs["ingester_type"])
	}
	if msg.Attrs["ingester_id"] != "test-fwd" {
		t.Errorf("ingester_id: expected test-fwd, got %q", msg.Attrs["ingester_id"])
	}
	if msg.SourceTS.Unix() != ts {
		t.Errorf("SourceTS: expected %d, got %d", ts, msg.SourceTS.Unix())
	}
	if time.Since(msg.IngestTS) > 2*time.Second {
		t.Errorf("IngestTS should be recent, got %v", msg.IngestTS)
	}
}

func TestFluentFwdMessageModeLogKey(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Use "log" key instead of "message".
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("docker")
	enc.EncodeInt(int64(1700000000))
	enc.EncodeMap(map[string]any{"log": "container output"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != "container output" {
		t.Errorf("raw: expected %q, got %q", "container output", msg.Raw)
	}
}

func TestFluentFwdMessageModeMsgKey(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Use "msg" key.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("test")
	enc.EncodeInt(int64(1700000000))
	enc.EncodeMap(map[string]any{"msg": "short form"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != "short form" {
		t.Errorf("raw: expected %q, got %q", "short form", msg.Raw)
	}
}

func TestFluentFwdMessageModeNoKnownKey(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// No "message", "log", or "msg" key — should JSON-encode the whole record.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("test")
	enc.EncodeInt(int64(1700000000))
	enc.EncodeMap(map[string]any{"custom_field": "value123"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != `{"custom_field":"value123"}` {
		t.Errorf("raw: expected JSON-encoded record, got %q", msg.Raw)
	}
}

// --- Forward Mode Tests ---

func TestFluentFwdForwardMode(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Forward mode: [tag, [[time1, record1], [time2, record2]]]
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(2)
	enc.EncodeString("app.log")
	// Array of entries.
	enc.EncodeArrayLen(2)
	// Entry 1.
	enc.EncodeArrayLen(2)
	enc.EncodeInt(int64(1700000001))
	enc.EncodeMap(map[string]any{"message": "entry one"})
	// Entry 2.
	enc.EncodeArrayLen(2)
	enc.EncodeInt(int64(1700000002))
	enc.EncodeMap(map[string]any{"message": "entry two"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg1 := recv(t, out)
	msg2 := recv(t, out)

	if string(msg1.Raw) != "entry one" {
		t.Errorf("msg1 raw: expected %q, got %q", "entry one", msg1.Raw)
	}
	if msg1.SourceTS.Unix() != 1700000001 {
		t.Errorf("msg1 SourceTS: expected 1700000001, got %d", msg1.SourceTS.Unix())
	}
	if msg1.Attrs["tag"] != "app.log" {
		t.Errorf("msg1 tag: expected app.log, got %q", msg1.Attrs["tag"])
	}

	if string(msg2.Raw) != "entry two" {
		t.Errorf("msg2 raw: expected %q, got %q", "entry two", msg2.Raw)
	}
	if msg2.SourceTS.Unix() != 1700000002 {
		t.Errorf("msg2 SourceTS: expected 1700000002, got %d", msg2.SourceTS.Unix())
	}
}

// --- PackedForward Mode Tests ---

func TestFluentFwdPackedForwardMode(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Build packed entries (concatenated msgpack arrays).
	var packed bytes.Buffer
	penc := msgpack.NewEncoder(&packed)
	// Entry 1.
	penc.EncodeArrayLen(2)
	penc.EncodeInt(int64(1700000010))
	penc.EncodeMap(map[string]any{"message": "packed one"})
	// Entry 2.
	penc.EncodeArrayLen(2)
	penc.EncodeInt(int64(1700000011))
	penc.EncodeMap(map[string]any{"message": "packed two"})

	// PackedForward: [tag, bin_entries]
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(2)
	enc.EncodeString("packed.tag")
	enc.EncodeBytes(packed.Bytes())

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg1 := recv(t, out)
	msg2 := recv(t, out)

	if string(msg1.Raw) != "packed one" {
		t.Errorf("msg1 raw: expected %q, got %q", "packed one", msg1.Raw)
	}
	if msg1.Attrs["tag"] != "packed.tag" {
		t.Errorf("msg1 tag: expected packed.tag, got %q", msg1.Attrs["tag"])
	}
	if string(msg2.Raw) != "packed two" {
		t.Errorf("msg2 raw: expected %q, got %q", "packed two", msg2.Raw)
	}
}

// --- CompressedPackedForward Mode Tests ---

func TestFluentFwdCompressedPackedForwardMode(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Build packed entries.
	var packed bytes.Buffer
	penc := msgpack.NewEncoder(&packed)
	penc.EncodeArrayLen(2)
	penc.EncodeInt(int64(1700000020))
	penc.EncodeMap(map[string]any{"message": "compressed entry"})

	// Gzip compress.
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	gz.Write(packed.Bytes())
	gz.Close()

	// CompressedPackedForward: [tag, gzip_bin, option{compressed: gzip}]
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("compressed.tag")
	enc.EncodeBytes(compressed.Bytes())
	enc.EncodeMap(map[string]any{"compressed": "gzip"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != "compressed entry" {
		t.Errorf("raw: expected %q, got %q", "compressed entry", msg.Raw)
	}
	if msg.Attrs["tag"] != "compressed.tag" {
		t.Errorf("tag: expected compressed.tag, got %q", msg.Attrs["tag"])
	}
}

// --- EventTime Extension Tests ---

func TestFluentFwdEventTime(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Build a message mode with EventTime extension type 0.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("precise.tag")

	// Manually encode EventTime extension: type 0, 8 bytes.
	et := &eventTime{Time: time.Unix(1700000050, 123456789)}
	enc.Encode(et)

	enc.EncodeMap(map[string]any{"message": "nanosecond precision"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != "nanosecond precision" {
		t.Errorf("raw: expected %q, got %q", "nanosecond precision", msg.Raw)
	}
	if msg.SourceTS.Unix() != 1700000050 {
		t.Errorf("SourceTS seconds: expected 1700000050, got %d", msg.SourceTS.Unix())
	}
	if msg.SourceTS.Nanosecond() != 123456789 {
		t.Errorf("SourceTS nanos: expected 123456789, got %d", msg.SourceTS.Nanosecond())
	}
}

// --- Ack Tests ---

func TestFluentFwdAck(t *testing.T) {
	addr, out := dialIngester(t, 10)

	chunkID := "test-chunk-abc"

	// Message mode with option containing chunk key: [tag, time, record, option]
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(4)
	enc.EncodeString("ack.tag")
	enc.EncodeInt(int64(1700000000))
	enc.EncodeMap(map[string]any{"message": "ack test"})
	enc.EncodeMap(map[string]any{"chunk": chunkID})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	// Receive the message.
	msg := recv(t, out)
	if string(msg.Raw) != "ack test" {
		t.Errorf("raw: expected %q, got %q", "ack test", msg.Raw)
	}

	// Read the ack response from the connection.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	dec := msgpack.NewDecoder(conn)
	var ackResp map[string]string
	if err := dec.Decode(&ackResp); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ackResp["ack"] != chunkID {
		t.Errorf("ack: expected %q, got %q", chunkID, ackResp["ack"])
	}
}

func TestFluentFwdNoAckWithoutChunk(t *testing.T) {
	addr, out := dialIngester(t, 10)

	// Message mode without option — no ack expected.
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.EncodeArrayLen(3)
	enc.EncodeString("noack.tag")
	enc.EncodeInt(int64(1700000000))
	enc.EncodeMap(map[string]any{"message": "no ack"})

	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	msg := recv(t, out)
	if string(msg.Raw) != "no ack" {
		t.Errorf("raw: expected %q, got %q", "no ack", msg.Raw)
	}

	// Verify no ack was sent (try reading with a short timeout).
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	oneByte := make([]byte, 1)
	_, err := conn.Read(oneByte)
	if err == nil {
		t.Error("expected no data on connection (no ack), but read succeeded")
	}
}

// --- Multiple Messages on Same Connection ---

func TestFluentFwdMultipleMessagesOneConnection(t *testing.T) {
	addr, out := dialIngester(t, 10)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	enc := msgpack.NewEncoder(conn)

	for i := range 5 {
		enc.EncodeArrayLen(3)
		enc.EncodeString("multi.tag")
		enc.EncodeInt(int64(1700000000 + i))
		enc.EncodeMap(map[string]any{"message": "msg" + string(rune('0'+i))})
	}

	for i := range 5 {
		msg := recv(t, out)
		if msg.Attrs["tag"] != "multi.tag" {
			t.Errorf("msg %d: expected tag multi.tag, got %q", i, msg.Attrs["tag"])
		}
	}
}

// --- Connection Close Resilience ---

func TestFluentFwdConnectionCloseResilience(t *testing.T) {
	addr, _ := dialIngester(t, 10)

	// Connect and immediately close — should not crash the ingester.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	// The ingester should still accept new connections.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("second dial failed (ingester may have crashed): %v", err)
	}
	conn2.Close()
}

// --- EventTime Extension Encoding/Decoding ---

func TestEventTimeMarshalUnmarshal(t *testing.T) {
	original := time.Unix(1700000099, 500000000)
	et := &eventTime{Time: original}

	data, err := et.MarshalMsgpack()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(data))
	}

	sec := binary.BigEndian.Uint32(data[0:4])
	nsec := binary.BigEndian.Uint32(data[4:8])
	if sec != 1700000099 {
		t.Errorf("seconds: expected 1700000099, got %d", sec)
	}
	if nsec != 500000000 {
		t.Errorf("nanoseconds: expected 500000000, got %d", nsec)
	}

	et2 := &eventTime{}
	if err := et2.UnmarshalMsgpack(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if et2.Unix() != original.Unix() || et2.Nanosecond() != original.Nanosecond() {
		t.Errorf("round-trip failed: expected %v, got %v", original, et2.Time)
	}
}

func TestEventTimeInvalidLength(t *testing.T) {
	et := &eventTime{}
	err := et.UnmarshalMsgpack([]byte{1, 2, 3}) // too short
	if err == nil {
		t.Error("expected error for invalid length")
	}
}

// --- Factory Tests ---

func TestFluentFwdFactory(t *testing.T) {
	factory := NewFactory()

	// Default addr.
	ing, err := factory(uuid.New(), nil, nil)
	if err != nil {
		t.Fatalf("factory with nil params: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	// Custom addr.
	ing, err = factory(uuid.New(), map[string]string{"addr": ":9224"}, nil)
	if err != nil {
		t.Fatalf("factory with custom addr: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	// Invalid addr.
	_, err = factory(uuid.New(), map[string]string{"addr": "noport"}, nil)
	if err == nil {
		t.Error("expected error for invalid addr")
	}
}

// --- Helper Tests ---

func TestIsArrayCode(t *testing.T) {
	// fixarray: 0x90-0x9f
	for i := byte(0x90); i <= 0x9f; i++ {
		if !isArrayCode(i) {
			t.Errorf("expected 0x%02x to be array code", i)
		}
	}
	// array16, array32
	if !isArrayCode(0xdc) {
		t.Error("expected 0xdc (array16) to be array code")
	}
	if !isArrayCode(0xdd) {
		t.Error("expected 0xdd (array32) to be array code")
	}
	// Non-array codes.
	if isArrayCode(0x00) {
		t.Error("expected 0x00 to not be array code")
	}
	if isArrayCode(0xc4) {
		t.Error("expected 0xc4 (bin8) to not be array code")
	}
}

func TestIsCompressed(t *testing.T) {
	if isCompressed(nil) {
		t.Error("nil should not be compressed")
	}
	if isCompressed(map[string]any{}) {
		t.Error("empty should not be compressed")
	}
	if isCompressed(map[string]any{"compressed": "lz4"}) {
		t.Error("lz4 should not match (only gzip)")
	}
	if !isCompressed(map[string]any{"compressed": "gzip"}) {
		t.Error("gzip should be compressed")
	}
}
