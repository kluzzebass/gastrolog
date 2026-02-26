// Package http provides an HTTP ingester that accepts log messages via the Loki Push API.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"gastrolog/internal/ingester/bodyutil"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Attribute limits to prevent abuse.
const (
	maxAttrs        = 32  // maximum number of attributes per message
	maxAttrKeyLen   = 64  // maximum length of attribute key
	maxAttrValueLen = 256 // maximum length of attribute value
)

// Ingester accepts log messages via the Loki Push API (POST /loki/api/v1/push).
// It implements orchestrator.Ingester.
//
// This is compatible with Promtail, Grafana Alloy, Fluent Bit, and other tools
// that support the Loki push protocol.
//
// Request modes:
//   - Fire-and-forget (default): Returns 204 No Content immediately after queuing
//   - Acknowledged: Returns 204 after message is persisted (X-Wait-Ack: true header)
//
// Note: X-Wait-Ack is a GastroLog extension not part of the Loki API.
type Ingester struct {
	id       string
	addr     string
	listener net.Listener
	server   *http.Server
	out      chan<- orchestrator.IngestMessage
	logger   *slog.Logger
}

// Config holds HTTP ingester configuration.
type Config struct {
	// ID is the ingester's config identifier.
	ID string

	// Addr is the address to listen on (e.g., ":3100", "127.0.0.1:3100").
	Addr string

	// Logger for structured logging.
	Logger *slog.Logger
}

// New creates a new HTTP ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		id:     cfg.ID,
		addr:   cfg.Addr,
		logger: logging.Default(cfg.Logger).With("component", "ingester", "type", "http"),
	}
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (r *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	r.out = out

	mux := http.NewServeMux()
	mux.HandleFunc("POST /loki/api/v1/push", r.handlePush)
	// Also support the legacy endpoint.
	mux.HandleFunc("POST /api/prom/push", r.handlePush)
	// Health check for load balancers.
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Create listener.
	var err error
	r.listener, err = net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}

	r.logger.Info("http ingester starting", "addr", r.listener.Addr().String())

	// Run server in background.
	errCh := make(chan error, 1)
	go func() {
		if err := r.server.Serve(r.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for context cancellation or server error.
	select {
	case <-ctx.Done():
		r.logger.Info("http ingester stopping")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

// Addr returns the listener address. Only valid after Run() has started.
func (r *Ingester) Addr() net.Addr {
	if r.listener == nil {
		return nil
	}
	return r.listener.Addr()
}

// PushRequest is the Loki push API request format.
type PushRequest struct {
	Streams []Stream `json:"streams"`
}

// Stream is a stream of log entries with shared labels.
type Stream struct {
	Stream map[string]string `json:"stream"`
	Values []Value           `json:"values"`
}

// Value is a log entry: [timestamp, line] or [timestamp, line, metadata].
// Timestamp is nanoseconds since epoch as a string.
type Value []json.RawMessage

// handlePush handles POST /loki/api/v1/push requests.
func (r *Ingester) handlePush(w http.ResponseWriter, req *http.Request) {
	if c := cap(r.out); c > 0 && len(r.out) >= c*9/10 {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "queue full, retry later", http.StatusTooManyRequests)
		return
	}

	messages, ok := r.decodePushBody(w, req)
	if !ok {
		return
	}

	if len(messages) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if req.Header.Get("X-Wait-Ack") == "true" {
		r.sendAcked(w, req, messages)
	} else {
		r.sendFireAndForget(w, req, messages)
	}
}

func (r *Ingester) decodePushBody(w http.ResponseWriter, req *http.Request) ([]orchestrator.IngestMessage, bool) {
	data, err := bodyutil.ReadBody(req.Body, req.Header.Get("Content-Encoding"), 10<<20)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return nil, false
	}

	var pushReq PushRequest
	if err := json.Unmarshal(data, &pushReq); err != nil {
		r.logger.Warn("failed to parse push request", "error", err)
		http.Error(w, "invalid JSON in request body", http.StatusBadRequest)
		return nil, false
	}

	var messages []orchestrator.IngestMessage
	for _, stream := range pushReq.Streams {
		for _, val := range stream.Values {
			msg, err := r.parseValue(val, stream.Stream)
			if err != nil {
				r.logger.Warn("failed to parse stream value", "error", err)
				http.Error(w, "invalid stream entry", http.StatusBadRequest)
				return nil, false
			}
			messages = append(messages, msg)
		}
	}
	return messages, true
}

func (r *Ingester) sendAcked(w http.ResponseWriter, req *http.Request, messages []orchestrator.IngestMessage) {
	ackCh := make(chan error, len(messages))
	for i := range messages {
		messages[i].Ack = ackCh
	}

	if !r.sendAll(w, req, messages) {
		return
	}

	var ackErr error
	for range messages {
		select {
		case err := <-ackCh:
			if err != nil && ackErr == nil {
				ackErr = err
			}
		case <-req.Context().Done():
			http.Error(w, "request cancelled", http.StatusServiceUnavailable)
			return
		}
	}

	if ackErr != nil {
		http.Error(w, ackErr.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Ingester) sendFireAndForget(w http.ResponseWriter, req *http.Request, messages []orchestrator.IngestMessage) {
	if !r.sendAll(w, req, messages) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Ingester) sendAll(w http.ResponseWriter, req *http.Request, messages []orchestrator.IngestMessage) bool {
	for _, msg := range messages {
		select {
		case r.out <- msg:
		case <-req.Context().Done():
			http.Error(w, "request cancelled", http.StatusServiceUnavailable)
			return false
		}
	}
	return true
}

// parseValue converts a Loki value to an IngestMessage.
// Value format: ["timestamp_ns", "line"] or ["timestamp_ns", "line", {metadata}]
func (r *Ingester) parseValue(val Value, streamLabels map[string]string) (orchestrator.IngestMessage, error) {
	if len(val) < 2 {
		return orchestrator.IngestMessage{}, errors.New("value must have at least 2 elements [timestamp, line]")
	}

	// Parse timestamp (nanoseconds since epoch as string).
	// This is the source timestamp from the log shipper.
	var tsStr string
	if err := json.Unmarshal(val[0], &tsStr); err != nil {
		return orchestrator.IngestMessage{}, fmt.Errorf("timestamp must be a string: %w", err)
	}

	tsNanos, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return orchestrator.IngestMessage{}, fmt.Errorf("invalid timestamp %q: %w", tsStr, err)
	}
	sourceTS := time.Unix(0, tsNanos)

	// Parse log line.
	var line string
	if err := json.Unmarshal(val[1], &line); err != nil {
		return orchestrator.IngestMessage{}, fmt.Errorf("log line must be a string: %w", err)
	}

	// Build attrs from stream labels (with validation).
	attrs := make(map[string]string, min(len(streamLabels), maxAttrs))
	for k, v := range streamLabels {
		if err := addAttr(attrs, k, v); err != nil {
			return orchestrator.IngestMessage{}, fmt.Errorf("stream label: %w", err)
		}
	}

	// Parse optional structured metadata (third element).
	if len(val) >= 3 {
		var metadata map[string]string
		if err := json.Unmarshal(val[2], &metadata); err != nil {
			return orchestrator.IngestMessage{}, fmt.Errorf("metadata must be an object: %w", err)
		}
		for k, v := range metadata {
			if err := addAttr(attrs, k, v); err != nil {
				return orchestrator.IngestMessage{}, fmt.Errorf("metadata: %w", err)
			}
		}
	}

	attrs["ingester_type"] = "http"

	return orchestrator.IngestMessage{
		Attrs:      attrs,
		Raw:        []byte(line),
		SourceTS:   sourceTS,
		IngestTS:   time.Now(),
		IngesterID: r.id,
	}, nil
}

// addAttr adds an attribute with validation. Returns error if limits exceeded.
func addAttr(attrs map[string]string, key, value string) error {
	if len(attrs) >= maxAttrs {
		return fmt.Errorf("too many attributes (max %d)", maxAttrs)
	}
	if len(key) > maxAttrKeyLen {
		return fmt.Errorf("attribute key too long: %d > %d", len(key), maxAttrKeyLen)
	}
	if len(value) > maxAttrValueLen {
		return fmt.Errorf("attribute value too long: %d > %d", len(value), maxAttrValueLen)
	}
	attrs[key] = value
	return nil
}
