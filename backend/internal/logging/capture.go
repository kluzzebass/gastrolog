package logging

import (
	"context"
	"log/slog"
)

// CapturedRecord holds a copy of an slog.Record plus any pre-resolved
// attributes from WithAttrs calls. This avoids the ingester needing to
// understand the handler chain's attribute accumulation.
type CapturedRecord struct {
	slog.Record
	// PreAttrs are attributes added via WithAttrs on ancestor handlers.
	PreAttrs []slog.Attr
}

// CaptureHandler wraps an slog.Handler, forwarding every record to the
// inner handler AND sending a copy to a bounded channel for ingestion.
//
// Records from pipeline-internal components (orchestrator, ingester, chunk,
// digest, etc.) are skipped to prevent feedback loops: those components
// produce logs as a side-effect of processing ingested records, so
// re-ingesting them would cause infinite recursion.
type CaptureHandler struct {
	inner    slog.Handler
	ch       chan<- CapturedRecord
	preAttrs []slog.Attr
	skip     map[string]struct{}
}

// NewCaptureHandler creates a handler that tees slog records to ch.
// skipComponents lists component names whose records should NOT be captured
// (to prevent feedback loops). The channel should be buffered; records are
// dropped silently if the channel is full.
func NewCaptureHandler(inner slog.Handler, ch chan<- CapturedRecord, skipComponents []string) *CaptureHandler {
	skip := make(map[string]struct{}, len(skipComponents))
	for _, c := range skipComponents {
		skip[c] = struct{}{}
	}
	return &CaptureHandler{
		inner: inner,
		ch:    ch,
		skip:  skip,
	}
}

func (h *CaptureHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *CaptureHandler) Handle(ctx context.Context, r slog.Record) error {
	// Check if this record's component is in the skip set.
	if comp := h.findComponent(r); comp != "" {
		if _, skip := h.skip[comp]; skip {
			return h.inner.Handle(ctx, r)
		}
	}

	// Non-blocking send: drop if channel is full.
	// Clone the record so the ingester gets a stable copy.
	clone := r.Clone()
	select {
	case h.ch <- CapturedRecord{Record: clone, PreAttrs: h.preAttrs}:
	default:
	}

	return h.inner.Handle(ctx, r)
}

func (h *CaptureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	newPre := make([]slog.Attr, len(h.preAttrs), len(h.preAttrs)+len(attrs))
	copy(newPre, h.preAttrs)
	newPre = append(newPre, attrs...)

	return &CaptureHandler{
		inner:    h.inner.WithAttrs(attrs),
		ch:       h.ch,
		preAttrs: newPre,
		skip:     h.skip, // shared (read-only)
	}
}

func (h *CaptureHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &CaptureHandler{
		inner:    h.inner.WithGroup(name),
		ch:       h.ch,
		preAttrs: h.preAttrs,
		skip:     h.skip,
	}
}

// findComponent extracts the "component" value from preAttrs or the record.
func (h *CaptureHandler) findComponent(r slog.Record) string {
	for _, a := range h.preAttrs {
		if a.Key == "component" {
			if s, ok := a.Value.Resolve().Any().(string); ok {
				return s
			}
		}
	}
	var comp string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			if s, ok := a.Value.Resolve().Any().(string); ok {
				comp = s
				return false
			}
		}
		return true
	})
	return comp
}
