// Package http provides an HTTP receiver that accepts log messages via the Loki Push API.
package http

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Receiver accepts log messages via the Loki Push API (POST /loki/api/v1/push).
// It implements orchestrator.Receiver.
//
// This is compatible with Promtail, Grafana Alloy, Fluent Bit, and other tools
// that support the Loki push protocol.
//
// Request modes:
//   - Fire-and-forget (default): Returns 204 No Content immediately after queuing
//   - Acknowledged: Returns 204 after message is persisted (X-Wait-Ack: true header)
//
// Note: X-Wait-Ack is a GastroLog extension not part of the Loki API.
type Receiver struct {
	addr     string
	listener net.Listener
	server   *http.Server
	out      chan<- orchestrator.IngestMessage
	logger   *slog.Logger
}

// Config holds HTTP receiver configuration.
type Config struct {
	// Addr is the address to listen on (e.g., ":3100", "127.0.0.1:3100").
	Addr string

	// Logger for structured logging.
	Logger *slog.Logger
}

// New creates a new HTTP receiver.
func New(cfg Config) *Receiver {
	return &Receiver{
		addr:   cfg.Addr,
		logger: logging.Default(cfg.Logger).With("component", "receiver", "type", "http"),
	}
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (r *Receiver) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
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
		Handler: mux,
	}

	// Create listener.
	var err error
	r.listener, err = net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}

	r.logger.Info("http receiver starting", "addr", r.listener.Addr().String())

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
		r.logger.Info("http receiver stopping")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		r.server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

// Addr returns the listener address. Only valid after Run() has started.
func (r *Receiver) Addr() net.Addr {
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
func (r *Receiver) handlePush(w http.ResponseWriter, req *http.Request) {
	waitAck := req.Header.Get("X-Wait-Ack") == "true"

	// Handle gzip compression.
	var body io.Reader = req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(req.Body)
		if err != nil {
			http.Error(w, "failed to decompress gzip body", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	}

	// Read and parse body.
	data, err := io.ReadAll(io.LimitReader(body, 10<<20)) // 10MB limit
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var pushReq PushRequest
	if err := json.Unmarshal(data, &pushReq); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Convert to IngestMessages.
	var messages []orchestrator.IngestMessage
	for _, stream := range pushReq.Streams {
		for _, val := range stream.Values {
			msg, err := r.parseValue(val, stream.Stream)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			messages = append(messages, msg)
		}
	}

	if len(messages) == 0 {
		// Loki returns 204 even for empty requests.
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Send messages.
	if waitAck {
		// Acknowledged mode: wait for all messages to be persisted.
		ackCh := make(chan error, len(messages))
		for i := range messages {
			messages[i].Ack = ackCh
		}

		// Send all messages.
		for _, msg := range messages {
			select {
			case r.out <- msg:
			case <-req.Context().Done():
				http.Error(w, "request cancelled", http.StatusServiceUnavailable)
				return
			}
		}

		// Wait for all acks.
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
	} else {
		// Fire-and-forget mode: queue messages and return immediately.
		for _, msg := range messages {
			select {
			case r.out <- msg:
			case <-req.Context().Done():
				http.Error(w, "request cancelled", http.StatusServiceUnavailable)
				return
			}
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// parseValue converts a Loki value to an IngestMessage.
// Value format: ["timestamp_ns", "line"] or ["timestamp_ns", "line", {metadata}]
func (r *Receiver) parseValue(val Value, streamLabels map[string]string) (orchestrator.IngestMessage, error) {
	if len(val) < 2 {
		return orchestrator.IngestMessage{}, errors.New("value must have at least 2 elements [timestamp, line]")
	}

	// Parse timestamp (nanoseconds since epoch as string).
	var tsStr string
	if err := json.Unmarshal(val[0], &tsStr); err != nil {
		return orchestrator.IngestMessage{}, fmt.Errorf("timestamp must be a string: %w", err)
	}

	tsNanos, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return orchestrator.IngestMessage{}, fmt.Errorf("invalid timestamp %q: %w", tsStr, err)
	}
	ingestTS := time.Unix(0, tsNanos)

	// Parse log line.
	var line string
	if err := json.Unmarshal(val[1], &line); err != nil {
		return orchestrator.IngestMessage{}, fmt.Errorf("log line must be a string: %w", err)
	}

	// Build attrs from stream labels.
	attrs := make(map[string]string, len(streamLabels)+8)
	for k, v := range streamLabels {
		attrs[k] = v
	}

	// Parse optional structured metadata (third element).
	if len(val) >= 3 {
		var metadata map[string]string
		if err := json.Unmarshal(val[2], &metadata); err != nil {
			return orchestrator.IngestMessage{}, fmt.Errorf("metadata must be an object: %w", err)
		}
		for k, v := range metadata {
			attrs[k] = v
		}
	}

	return orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      []byte(line),
		IngestTS: ingestTS,
	}, nil
}
