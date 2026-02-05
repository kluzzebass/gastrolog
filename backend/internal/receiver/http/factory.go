package http

import (
	"fmt"
	"log/slog"

	"gastrolog/internal/orchestrator"
)

// NewFactory returns a ReceiverFactory for HTTP receivers.
func NewFactory() orchestrator.ReceiverFactory {
	return func(params map[string]string, logger *slog.Logger) (orchestrator.Receiver, error) {
		addr := params["addr"]
		if addr == "" {
			addr = ":8080"
		}

		// Validate addr format (basic check).
		if addr[0] != ':' && addr[0] != '[' {
			// Check for host:port format.
			hasColon := false
			for _, c := range addr {
				if c == ':' {
					hasColon = true
					break
				}
			}
			if !hasColon {
				return nil, fmt.Errorf("invalid addr %q: must be :port or host:port", addr)
			}
		}

		return New(Config{
			Addr:   addr,
			Logger: logger,
		}), nil
	}
}
