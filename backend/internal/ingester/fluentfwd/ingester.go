// Package fluentfwd provides a Fluent Forward protocol ingester.
// It accepts messages from Fluentd and Fluent Bit over TCP using msgpack encoding.
package fluentfwd

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/ingester/limits"
	"gastrolog/internal/logging"
	"gastrolog/internal/logging/comp"
	"gastrolog/internal/panicguard"
	"gastrolog/internal/pipeline/ingestion"
)

// refusedLogInterval spaces the "listener at capacity" and "attributes
// dropped" warnings. A peer that hammers a full listener, or a producer
// emitting the same oversized record continuously, would otherwise write one
// line per event and bury the condition it reports.
const refusedLogInterval = 10 * time.Second

// Ingester accepts messages via the Fluent Forward protocol over TCP.
type Ingester struct {
	id     string
	addr   string
	out    chan<- ingestion.IngesterMessage
	logger *slog.Logger

	// conns caps concurrent connections; refusedLog throttles both the
	// refusal warning and the report of attributes the record ceiling
	// dropped, neither of which may be silent.
	conns      *limits.ConnLimiter
	refusedLog logging.Throttle

	// pressureGate throttles msgpack reads when the ingest pipeline is backed
	// up. Pausing before DecodeArrayLen stops reads from the TCP socket, so
	// the window closes and Fluent senders back off. Injected by the
	// orchestrator; nil means no throttling.
	pressureGate *chanwatch.PressureGate
}

// SetPressureGate wires the orchestrator's pressure gate into the ingester.
// Implements ingestion.PressureAware.
func (ing *Ingester) SetPressureGate(gate *chanwatch.PressureGate) {
	ing.pressureGate = gate
}

// Config holds Fluent Forward ingester configuration.
type Config struct {
	ID     string
	Addr   string // e.g. ":24224"
	Logger *slog.Logger
}

// New creates a new Fluent Forward ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		id:         cfg.ID,
		addr:       cfg.Addr,
		conns:      limits.NewConnLimiter(limits.MaxConnections),
		refusedLog: logging.Throttle{Interval: refusedLogInterval},
		logger:     comp.Ingester.Sub("fluentfwd").Desc("Fluent Forward ingester — accepts records from fluentd/fluent-bit using the Forward protocol.").Apply(logging.Default(cfg.Logger)),
	}
}

func init() {
	// Register EventTime extension (type 0) for msgpack.
	// Fluent Forward protocol encodes timestamps as extension type 0:
	// 8 bytes = 4-byte big-endian seconds + 4-byte big-endian nanoseconds.
	msgpack.RegisterExt(0, (*eventTime)(nil))
}

// eventTime implements msgpack extension type 0 for Fluent Forward EventTime.
type eventTime struct {
	time.Time
}

func (et *eventTime) MarshalMsgpack() ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], uint32(et.Unix()))       //nolint:gosec // G115: Unix timestamp fits in uint32 until 2106
	binary.BigEndian.PutUint32(b[4:8], uint32(et.Nanosecond())) //nolint:gosec // G115: nanosecond component is always 0..999999999
	return b, nil
}

func (et *eventTime) UnmarshalMsgpack(b []byte) error {
	if len(b) != 8 {
		return fmt.Errorf("eventtime: expected 8 bytes, got %d", len(b))
	}
	sec := binary.BigEndian.Uint32(b[0:4])
	nsec := binary.BigEndian.Uint32(b[4:8])
	et.Time = time.Unix(int64(sec), int64(nsec))
	return nil
}

// Run starts the TCP listener and blocks until ctx is cancelled.
func (ing *Ingester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	ing.out = out

	ln, err := net.Listen("tcp", ing.addr)
	if err != nil {
		return fmt.Errorf("fluentfwd listen: %w", err)
	}

	ing.logger.Info("fluent forward listening", "addr", ln.Addr().String())

	var wg sync.WaitGroup
	defer func() {
		_ = ln.Close()
		wg.Wait()
	}()

	// Accept loop.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Transient accept error, log and continue.
			ing.logger.Warn("accept error", "error", err)
			continue
		}

		remote := conn.RemoteAddr().String()
		// Past the cap, refuse immediately: a peer opening connections
		// faster than they close must not exhaust the node's file
		// descriptors or goroutines.
		if !ing.conns.Acquire() {
			_ = conn.Close()
			if n, ok := ing.refusedLog.Allow("conn-limit"); ok {
				ing.logger.Warn("fluent forward connection refused: listener at capacity",
					"remote", remote, "max_connections", limits.MaxConnections, "suppressed", n)
			}
			continue
		}

		wg.Go(func() {
			defer ing.conns.Release()
			// A panic on a hostile record costs this connection, not the
			// node and every other vault and ingester running on it.
			defer panicguard.Recover(ing.logger, "fluent forward connection", "remote", remote)
			ing.handleConn(ctx, conn)
		})
	}
}

// handleConn processes a single TCP connection.
func (ing *Ingester) handleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	remote := conn.RemoteAddr().String()
	ing.logger.Debug("connection accepted", "remote", remote)

	// A sender that starts a message must finish it. Idle time between
	// messages stays unbounded: a forwarder on a quiet host legitimately
	// holds an open connection, and is not the thing being defended
	// against.
	framed := limits.NewFrameConn(conn, limits.FrameTimeout)
	dec := msgpack.NewDecoder(framed)

	for {
		if ctx.Err() != nil {
			return
		}

		// Backpressure: pause msgpack decoding while the pipeline is backed
		// up. This stops the underlying TCP reads, closing the window so
		// Fluent senders see transport-level backpressure.
		if ing.pressureGate != nil {
			if err := ing.pressureGate.Wait(ctx); err != nil {
				return
			}
		}

		option, ok := ing.handleOneMessage(ctx, dec, remote)
		// The message is off the wire; clear its deadline before acking,
		// or the next read inherits a deadline that already expired.
		framed.Done()
		if !ok {
			return
		}

		sendAck(conn, option)
	}
}

func (ing *Ingester) handleOneMessage(ctx context.Context, dec *msgpack.Decoder, remote string) (map[string]any, bool) {
	arrLen, err := dec.DecodeArrayLen()
	if err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && ctx.Err() == nil {
			ing.logger.Warn("decode error", "remote", remote, "error", err)
		}
		return nil, false
	}

	if arrLen < 2 || arrLen > 4 {
		ing.logger.Warn("unexpected array length", "remote", remote, "len", arrLen)
		return nil, false
	}

	tag, err := dec.DecodeString()
	if err != nil {
		ing.logger.Warn("decode tag error", "remote", remote, "error", err)
		return nil, false
	}

	code, err := dec.PeekCode()
	if err != nil {
		ing.logger.Warn("peek error", "remote", remote, "error", err)
		return nil, false
	}

	switch {
	case code == msgpcode.Bin8 || code == msgpcode.Bin16 || code == msgpcode.Bin32:
		return ing.handlePackedForward(ctx, dec, tag, arrLen, remote)
	case isArrayCode(code):
		return ing.handleForward(ctx, dec, tag, arrLen, remote)
	default:
		return ing.handleMessage(ctx, dec, tag, arrLen, remote)
	}
}

func (ing *Ingester) handlePackedForward(ctx context.Context, dec *msgpack.Decoder, tag string, arrLen int, remote string) (map[string]any, bool) {
	binData, err := readPackedEntries(dec)
	if err != nil {
		ing.logger.Warn("decode packed entries", "remote", remote, "error", err)
		return nil, false
	}

	var option map[string]any
	if arrLen >= 3 {
		option, _ = decodeOption(dec)
	}

	if isCompressed(option) {
		binData, err = gunzip(binData)
		if err != nil {
			ing.logger.Warn("decompress error", "remote", remote, "error", err)
			return nil, false
		}
	}

	if err := ing.processPackedEntries(ctx, tag, binData); err != nil {
		if ctx.Err() == nil {
			ing.logger.Warn("process packed entries", "remote", remote, "error", err)
		}
		return nil, false
	}
	return option, true
}

func (ing *Ingester) handleForward(ctx context.Context, dec *msgpack.Decoder, tag string, arrLen int, remote string) (map[string]any, bool) {
	entries, err := ing.decodeEntries(dec)
	if err != nil {
		ing.logger.Warn("decode entries", "remote", remote, "error", err)
		return nil, false
	}

	var option map[string]any
	if arrLen >= 3 {
		option, _ = decodeOption(dec)
	}

	if err := ing.processEntries(ctx, tag, entries); err != nil {
		if ctx.Err() == nil {
			ing.logger.Warn("process entries", "remote", remote, "error", err)
		}
		return nil, false
	}
	return option, true
}

func (ing *Ingester) handleMessage(ctx context.Context, dec *msgpack.Decoder, tag string, arrLen int, remote string) (map[string]any, bool) {
	ts, err := decodeTime(dec)
	if err != nil {
		ing.logger.Warn("decode time", "remote", remote, "error", err)
		return nil, false
	}

	record, err := decodeRecord(dec)
	if err != nil {
		ing.logger.Warn("decode record", "remote", remote, "error", err)
		return nil, false
	}

	var option map[string]any
	if arrLen >= 4 {
		option, _ = decodeOption(dec)
	}

	if err := ing.processRecord(ctx, tag, ts, record); err != nil {
		if ctx.Err() == nil {
			ing.logger.Warn("process record", "remote", remote, "error", err)
		}
		return nil, false
	}
	return option, true
}

func sendAck(conn net.Conn, option map[string]any) {
	chunk, ok := option["chunk"]
	if !ok {
		return
	}
	chunkStr, ok := chunk.(string)
	if !ok {
		return
	}
	ack := map[string]string{"ack": chunkStr}
	data, _ := msgpack.Marshal(ack)
	_, _ = conn.Write(data)
}

// readPackedEntries reads a packed-forward batch, refusing a length the
// sender declares beyond the batch ceiling. The payload is copied in as it
// arrives rather than allocated from the declared length, so a claim alone
// costs nothing.
func readPackedEntries(dec *msgpack.Decoder) ([]byte, error) {
	n, err := dec.DecodeBytesLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if int64(n) > limits.MaxDecompressedBytes {
		return nil, fmt.Errorf("fluentfwd: packed batch declares %d bytes, over %d", n, limits.MaxDecompressedBytes)
	}
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, dec.Buffered(), int64(n)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// entry is a [time, record] pair.
type entry struct {
	ts     time.Time
	record map[string]any
}

// decodeEntries decodes a Forward-mode array of [time, record] pairs.
func (ing *Ingester) decodeEntries(dec *msgpack.Decoder) ([]entry, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}

	entries := make([]entry, 0, n)
	for range n {
		innerLen, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		if innerLen < 2 {
			return nil, fmt.Errorf("entry array too short: %d", innerLen)
		}

		ts, err := decodeTime(dec)
		if err != nil {
			return nil, err
		}

		record, err := decodeRecord(dec)
		if err != nil {
			return nil, err
		}

		// Skip extra elements.
		for range innerLen - 2 {
			if err := dec.Skip(); err != nil {
				return nil, err
			}
		}

		entries = append(entries, entry{ts: ts, record: record})
	}
	return entries, nil
}

// processPackedEntries decodes concatenated msgpack [time, record] entries.
func (ing *Ingester) processPackedEntries(ctx context.Context, tag string, data []byte) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	for {
		arrLen, err := dec.DecodeArrayLen()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if arrLen < 2 {
			return fmt.Errorf("packed entry too short: %d", arrLen)
		}

		ts, err := decodeTime(dec)
		if err != nil {
			return err
		}
		record, err := decodeRecord(dec)
		if err != nil {
			return err
		}
		for range arrLen - 2 {
			if err := dec.Skip(); err != nil {
				return err
			}
		}

		if err := ing.processRecord(ctx, tag, ts, record); err != nil {
			return err
		}
	}
}

// processEntries sends a batch of entries.
func (ing *Ingester) processEntries(ctx context.Context, tag string, entries []entry) error {
	for _, e := range entries {
		if err := ing.processRecord(ctx, tag, e.ts, e.record); err != nil {
			return err
		}
	}
	return nil
}

// processRecord converts a single record to an IngestMessage and sends it.
func (ing *Ingester) processRecord(ctx context.Context, tag string, ts time.Time, record map[string]any) error {
	attrs := make(map[string]string, min(len(record), limits.Records.Count)+2)

	// Extract raw log line from well-known keys.
	var raw string
	for _, key := range []string{"message", "log", "msg"} {
		if v, ok := record[key]; ok {
			raw = fmt.Sprint(v)
			break
		}
	}
	if raw == "" {
		// Fallback: JSON-encode the whole record.
		data, _ := json.Marshal(record)
		raw = string(data)
	}

	// Record fields are attacker-chosen in both count and length, and each
	// one becomes an index term. Bound them, and drop the excess rather
	// than the record: the log line itself is the payload that must not be
	// lost.
	//
	// Sorted, because msgpack hands the fields over as a Go map and its
	// iteration order is randomized: dropping whatever the range happened
	// to reach last would ingest two identical records differently, and
	// the same record differently on the next process.
	keys := make([]string, 0, len(record))
	for k := range record {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	dropped := 0
	for _, k := range keys {
		if err := limits.Records.Add(attrs, k, fmt.Sprint(record[k])); err != nil {
			dropped++
		}
	}
	// The ingester's own attributes are set last and outside the budget:
	// they identify where the record came from, so a field flood must
	// neither crowd them out nor forge them. "tag" is a plausible field
	// name, so a record can lose one this way — which is counted, because
	// a field that vanishes without a word is indistinguishable to the
	// sender from one that never arrived.
	displaced := setOwnAttr(attrs, "tag", tag)
	displaced += setOwnAttr(attrs, "ingester_type", "fluentfwd")

	if dropped > 0 || displaced > 0 {
		if n, ok := ing.refusedLog.Allow("record-attrs"); ok {
			ing.logger.Warn("fluent record attributes dropped",
				"tag", tag, "dropped", dropped, "displaced", displaced,
				"max_attrs", limits.Records.Count, "suppressed", n)
		}
	}

	msg := ingestion.IngesterMessage{
		Attrs:      attrs,
		Raw:        []byte(raw),
		RawOwned:   true,
		SourceTS:   ts,
		IngestTS:   time.Now(),
		IngesterID: ing.id,
	}

	select {
	case ing.out <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// setOwnAttr writes one of the ingester's own attributes over whatever the
// record had under that name, reporting whether it displaced a different
// value.
func setOwnAttr(attrs map[string]string, key, value string) int {
	displaced := 0
	if existing, ok := attrs[key]; ok && existing != value {
		displaced = 1
	}
	attrs[key] = value
	return displaced
}

// decodeTime decodes a msgpack value as a timestamp.
// Handles integer (Unix seconds), float, and EventTime extension.
func decodeTime(dec *msgpack.Decoder) (time.Time, error) {
	iface, err := dec.DecodeInterface()
	if err != nil {
		return time.Time{}, err
	}

	switch v := iface.(type) {
	case int64:
		return time.Unix(v, 0), nil
	case uint64:
		return time.Unix(int64(v), 0), nil //nolint:gosec // G115: Unix timestamps fit in int64 until year 292277026596
	case int8:
		return time.Unix(int64(v), 0), nil
	case int16:
		return time.Unix(int64(v), 0), nil
	case int32:
		return time.Unix(int64(v), 0), nil
	case uint8:
		return time.Unix(int64(v), 0), nil
	case uint16:
		return time.Unix(int64(v), 0), nil
	case uint32:
		return time.Unix(int64(v), 0), nil
	case float64:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		return time.Unix(sec, nsec), nil
	case *eventTime:
		return v.Time, nil
	default:
		return time.Time{}, fmt.Errorf("unexpected time type: %T", iface)
	}
}

// decodeRecord decodes a msgpack map as a record.
func decodeRecord(dec *msgpack.Decoder) (map[string]any, error) {
	var record map[string]any
	if err := dec.Decode(&record); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	return record, nil
}

// decodeOption decodes the optional option map.
func decodeOption(dec *msgpack.Decoder) (map[string]any, error) {
	var opt map[string]any
	if err := dec.Decode(&opt); err != nil {
		return nil, err
	}
	return opt, nil
}

// isCompressed checks if the option map indicates gzip compression.
func isCompressed(opt map[string]any) bool {
	if opt == nil {
		return false
	}
	v, ok := opt["compressed"]
	if !ok {
		return false
	}
	s, ok := v.(string)
	return ok && s == "gzip"
}

// gunzip decompresses gzip data, capping the output at the shared
// decompression ceiling. Returns an error if the decompressed payload would
// exceed it, so callers can drop the offending batch and log rather than
// OOMing. We stream into a bytes.Buffer via io.Copy with an io.LimitReader
// rather than reading the whole thing unbounded.
func gunzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	// Limit to cap+1 so we can distinguish "exactly at limit" from "exceeded".
	limited := io.LimitReader(r, limits.MaxDecompressedBytes+1)
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, limited); err != nil {
		return nil, err
	}
	if int64(buf.Len()) > limits.MaxDecompressedBytes {
		return nil, fmt.Errorf("fluentfwd: decompressed payload exceeds %d bytes (gzip bomb?)", limits.MaxDecompressedBytes)
	}
	return buf.Bytes(), nil
}

// isArrayCode returns true if the msgpack format code represents an array.
func isArrayCode(c byte) bool {
	return (c >= 0x90 && c <= 0x9f) || c == 0xdc || c == 0xdd
}
