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
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Ingester accepts messages via the Fluent Forward protocol over TCP.
type Ingester struct {
	id     string
	addr   string
	out    chan<- orchestrator.IngestMessage
	logger *slog.Logger
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
		id:     cfg.ID,
		addr:   cfg.Addr,
		logger: logging.Default(cfg.Logger).With("component", "ingester", "type", "fluentfwd"),
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
	binary.BigEndian.PutUint32(b[0:4], uint32(et.Unix()))
	binary.BigEndian.PutUint32(b[4:8], uint32(et.Nanosecond()))
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
func (ing *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	ing.out = out

	ln, err := net.Listen("tcp", ing.addr)
	if err != nil {
		return fmt.Errorf("fluentfwd listen: %w", err)
	}

	ing.logger.Info("fluent forward listening", "addr", ln.Addr().String())

	var wg sync.WaitGroup
	defer func() {
		ln.Close()
		wg.Wait()
	}()

	// Accept loop.
	go func() {
		<-ctx.Done()
		ln.Close()
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

		wg.Add(1)
		go func() {
			defer wg.Done()
			ing.handleConn(ctx, conn)
		}()
	}
}

// handleConn processes a single TCP connection.
func (ing *Ingester) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	ing.logger.Debug("connection accepted", "remote", remote)

	dec := msgpack.NewDecoder(conn)

	for {
		if ctx.Err() != nil {
			return
		}

		// Each message is a msgpack array.
		arrLen, err := dec.DecodeArrayLen()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || ctx.Err() != nil {
				return
			}
			ing.logger.Warn("decode error", "remote", remote, "error", err)
			return
		}

		if arrLen < 2 || arrLen > 4 {
			ing.logger.Warn("unexpected array length", "remote", remote, "len", arrLen)
			return
		}

		// First element: tag (string).
		tag, err := dec.DecodeString()
		if err != nil {
			ing.logger.Warn("decode tag error", "remote", remote, "error", err)
			return
		}

		// Peek at the second element to determine mode.
		code, err := dec.PeekCode()
		if err != nil {
			ing.logger.Warn("peek error", "remote", remote, "error", err)
			return
		}

		var option map[string]any

		switch {
		case code == msgpcode.Bin8 || code == msgpcode.Bin16 || code == msgpcode.Bin32:
			// PackedForward or CompressedPackedForward mode: [tag, bin, option?]
			binData, err := dec.DecodeBytes()
			if err != nil {
				ing.logger.Warn("decode packed entries", "remote", remote, "error", err)
				return
			}

			if arrLen >= 3 {
				option, _ = decodeOption(dec)
			}

			// Check for compression.
			if isCompressed(option) {
				binData, err = gunzip(binData)
				if err != nil {
					ing.logger.Warn("decompress error", "remote", remote, "error", err)
					return
				}
			}

			if err := ing.processPackedEntries(ctx, tag, binData); err != nil {
				if ctx.Err() != nil {
					return
				}
				ing.logger.Warn("process packed entries", "remote", remote, "error", err)
				return
			}

		case isArrayCode(code):
			// Could be Forward mode [tag, [[time,record],...], option?]
			// or Message mode where second element happened to look like an array:
			// [tag, time, record, option?]
			// Forward mode has an array of entries as second element.
			// Peek deeper: if it's an array of arrays, it's Forward mode.

			entries, err := ing.decodeEntries(dec)
			if err != nil {
				ing.logger.Warn("decode entries", "remote", remote, "error", err)
				return
			}

			if arrLen >= 3 {
				option, _ = decodeOption(dec)
			}

			if err := ing.processEntries(ctx, tag, entries); err != nil {
				if ctx.Err() != nil {
					return
				}
				ing.logger.Warn("process entries", "remote", remote, "error", err)
				return
			}

		default:
			// Message mode: [tag, time, record, option?]
			ts, err := decodeTime(dec)
			if err != nil {
				ing.logger.Warn("decode time", "remote", remote, "error", err)
				return
			}

			record, err := decodeRecord(dec)
			if err != nil {
				ing.logger.Warn("decode record", "remote", remote, "error", err)
				return
			}

			if arrLen >= 4 {
				option, _ = decodeOption(dec)
			}

			if err := ing.processRecord(ctx, tag, ts, record); err != nil {
				if ctx.Err() != nil {
					return
				}
				ing.logger.Warn("process record", "remote", remote, "error", err)
				return
			}
		}

		// Send ack if requested.
		if chunk, ok := option["chunk"]; ok {
			if chunkStr, ok := chunk.(string); ok {
				ack := map[string]string{"ack": chunkStr}
				data, _ := msgpack.Marshal(ack)
				conn.Write(data)
			}
		}
	}
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
	attrs := make(map[string]string, len(record)+4)
	attrs["tag"] = tag
	attrs["ingester_type"] = "fluentfwd"
	attrs["ingester_id"] = ing.id

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

	// Stringify all record keys as attributes.
	for k, v := range record {
		attrs[k] = fmt.Sprint(v)
	}

	msg := orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      []byte(raw),
		SourceTS: ts,
		IngestTS: time.Now(),
	}

	select {
	case ing.out <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
		return time.Unix(int64(v), 0), nil
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

// gunzip decompresses gzip data.
func gunzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// isArrayCode returns true if the msgpack format code represents an array.
func isArrayCode(c byte) bool {
	return (c >= 0x90 && c <= 0x9f) || c == 0xdc || c == 0xdd
}
