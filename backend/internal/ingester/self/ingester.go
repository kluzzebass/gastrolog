package self

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

type ingester struct {
	id     string
	ch     <-chan logging.CapturedRecord
	logger *slog.Logger
}

func (ing *ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	ing.logger.Info("started")
	for {
		select {
		case <-ctx.Done():
			return nil
		case cr, ok := <-ing.ch:
			if !ok {
				return nil
			}
			msg := ing.convert(cr)
			select {
			case out <- msg:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// convert transforms a CapturedRecord into an IngestMessage with a JSON body.
func (ing *ingester) convert(cr logging.CapturedRecord) orchestrator.IngestMessage {
	// Build a flat map of all attributes for the JSON body.
	rec := make(map[string]any, 8)
	rec["time"] = cr.Time
	rec["level"] = cr.Level.String()
	rec["msg"] = cr.Message

	// Merge preAttrs (from WithAttrs) and record attrs.
	attrs := make(map[string]string, len(cr.PreAttrs)+4)
	for _, a := range cr.PreAttrs {
		k := a.Key
		v := a.Value.Resolve().String()
		rec[k] = v
		attrs[k] = v
	}
	cr.Attrs(func(a slog.Attr) bool {
		k := a.Key
		v := a.Value.Resolve()
		rec[k] = v.Any()
		attrs[k] = v.String()
		return true
	})

	// Map slog level to our level attribute.
	attrs["level"] = slogLevelToString(cr.Level)
	attrs["ingester_type"] = "self"

	raw, _ := json.Marshal(rec) //nolint:errchkjson // map[string]any never fails

	return orchestrator.IngestMessage{
		Attrs:      attrs,
		Raw:        raw,
		SourceTS:   cr.Time,
		IngestTS:   time.Now(),
		IngesterID: ing.id,
	}
}

func slogLevelToString(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "error"
	case l >= slog.LevelWarn:
		return "warn"
	case l >= slog.LevelInfo:
		return "info"
	default:
		return "debug"
	}
}

