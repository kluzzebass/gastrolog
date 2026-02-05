// Package http provides an HTTP receiver that accepts log messages via POST requests.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Receiver accepts log messages via HTTP POST requests.
// It implements orchestrator.Receiver.
//
// Endpoints:
//   - POST /ingest - accepts log messages (single or batch)
//
// Request modes:
//   - Fire-and-forget: Returns 202 Accepted immediately after queuing
//   - Acknowledged: Returns 200 OK after message is persisted (X-Wait-Ack: true header)
//
// Request body formats:
//   - application/json: JSON object or array of objects
//   - text/plain: Raw log line(s), one per line
//
// Attributes can be passed via:
//   - X-Attrs-* headers (e.g., X-Attrs-Host: server1)
//   - JSON fields (for JSON format)
type Receiver struct {
	addr     string
	listener net.Listener
	server   *http.Server
	out      chan<- orchestrator.IngestMessage
	logger   *slog.Logger
}

// Config holds HTTP receiver configuration.
type Config struct {
	// Addr is the address to listen on (e.g., ":8080", "127.0.0.1:8080").
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
	mux.HandleFunc("POST /ingest", r.handleIngest)

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

// handleIngest handles POST /ingest requests.
func (r *Receiver) handleIngest(w http.ResponseWriter, req *http.Request) {
	ingestTS := time.Now()
	waitAck := req.Header.Get("X-Wait-Ack") == "true"

	// Extract attributes from headers.
	attrs := r.extractHeaderAttrs(req)

	// Read body.
	body, err := io.ReadAll(io.LimitReader(req.Body, 10<<20)) // 10MB limit
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Parse based on content type.
	var messages []orchestrator.IngestMessage
	contentType := req.Header.Get("Content-Type")

	switch {
	case contentType == "application/json" || contentType == "":
		messages, err = r.parseJSON(body, attrs, ingestTS)
	default:
		// Treat as plain text - one message per line or single message.
		messages = r.parsePlainText(body, attrs, ingestTS)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(messages) == 0 {
		http.Error(w, "no messages in request", http.StatusBadRequest)
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

		w.Header().Set("X-Messages-Received", strconv.Itoa(len(messages)))
		w.WriteHeader(http.StatusOK)
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

		w.Header().Set("X-Messages-Received", strconv.Itoa(len(messages)))
		w.WriteHeader(http.StatusAccepted)
	}
}

// extractHeaderAttrs extracts attributes from X-Attrs-* headers.
func (r *Receiver) extractHeaderAttrs(req *http.Request) map[string]string {
	attrs := make(map[string]string)
	for name, values := range req.Header {
		if len(name) > 8 && name[:8] == "X-Attrs-" {
			key := name[8:]
			if len(values) > 0 {
				attrs[key] = values[0]
			}
		}
	}
	return attrs
}

// parseJSON parses JSON body into messages.
// Accepts either a single object or an array of objects.
// Each object can have "raw" (string) and "attrs" (object) fields.
// If "raw" is missing, the entire object is serialized as raw.
func (r *Receiver) parseJSON(body []byte, headerAttrs map[string]string, ingestTS time.Time) ([]orchestrator.IngestMessage, error) {
	if len(body) == 0 {
		return nil, nil
	}

	// Try to parse as array first.
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		// Try as single object.
		items = []json.RawMessage{body}
	}

	messages := make([]orchestrator.IngestMessage, 0, len(items))
	for _, item := range items {
		msg, err := r.parseJSONItem(item, headerAttrs, ingestTS)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// parseJSONItem parses a single JSON object into a message.
func (r *Receiver) parseJSONItem(item json.RawMessage, headerAttrs map[string]string, ingestTS time.Time) (orchestrator.IngestMessage, error) {
	// Try structured format first: {"raw": "...", "attrs": {...}}
	var structured struct {
		Raw   string            `json:"raw"`
		Attrs map[string]string `json:"attrs"`
	}

	if err := json.Unmarshal(item, &structured); err == nil && structured.Raw != "" {
		// Merge header attrs with message attrs (header takes precedence).
		attrs := make(map[string]string, len(headerAttrs)+len(structured.Attrs))
		for k, v := range structured.Attrs {
			attrs[k] = v
		}
		for k, v := range headerAttrs {
			attrs[k] = v
		}

		return orchestrator.IngestMessage{
			Attrs:    attrs,
			Raw:      []byte(structured.Raw),
			IngestTS: ingestTS,
		}, nil
	}

	// Treat entire object as raw log message.
	attrs := make(map[string]string, len(headerAttrs))
	for k, v := range headerAttrs {
		attrs[k] = v
	}

	return orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      item,
		IngestTS: ingestTS,
	}, nil
}

// parsePlainText parses plain text body into messages.
// Each non-empty line becomes a separate message.
func (r *Receiver) parsePlainText(body []byte, headerAttrs map[string]string, ingestTS time.Time) []orchestrator.IngestMessage {
	if len(body) == 0 {
		return nil
	}

	// If body doesn't contain newlines, treat as single message.
	// Otherwise split by lines.
	lines := splitLines(body)
	messages := make([]orchestrator.IngestMessage, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		attrs := make(map[string]string, len(headerAttrs))
		for k, v := range headerAttrs {
			attrs[k] = v
		}

		messages = append(messages, orchestrator.IngestMessage{
			Attrs:    attrs,
			Raw:      line,
			IngestTS: ingestTS,
		})
	}

	return messages
}

// splitLines splits body by newlines, handling \r\n and \n.
func splitLines(body []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i := 0; i < len(body); i++ {
		if body[i] == '\n' {
			end := i
			if end > start && body[end-1] == '\r' {
				end--
			}
			lines = append(lines, body[start:end])
			start = i + 1
		}
	}
	// Add last line if not ending with newline.
	if start < len(body) {
		lines = append(lines, body[start:])
	}
	return lines
}
