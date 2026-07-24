package ingestion

import (
	"log/slog"

	"gastrolog/internal/glid"
)

// Triggerable is an optional interface for ingesters that support on-demand
// record emission. The operator can trigger a one-shot burst via the UI without
// restarting the ingester.
type Triggerable interface {
	Trigger()
}

// ListenAddr describes a network address that a listener ingester will bind to.
type ListenAddr struct {
	Network string // "tcp", "udp"
	Address string
}

// IngesterFactory creates an Ingester from configuration parameters.
// Factories validate required params, apply defaults, and return a fully
// constructed ingester or a descriptive error.
// Factories must not start goroutines or perform I/O beyond validation.
//
// The logger parameter is optional. If nil, the ingester disables logging.
// Factories should scope the logger with component-specific attributes.
//
// This type lives in the ingestion package alongside the Ingester contract it
// produces. Concrete factory implementations live in their respective ingester
// packages (e.g., syslog.NewFactory()); the orchestrator never contains
// ingester construction logic — it only calls factories.
type IngesterFactory func(id glid.GLID, params map[string]string, logger *slog.Logger) (Ingester, error)
