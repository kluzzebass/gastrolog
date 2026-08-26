// Package mqtt provides an MQTT subscribing ingester supporting both
// MQTT v3.1.1 (paho.mqtt.golang) and v5 (paho.golang/autopaho).
package mqtt

import (
	"log/slog"

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/logging"
	"gastrolog/internal/logging/comp"
	"gastrolog/internal/pipeline/ingestion"
)

// pressureAware is the shared state for v3 and v5 MQTT ingesters to store
// the pressure gate from the orchestrator. Both v3Ingester and v5Ingester
// embed this.
type pressureAware struct {
	pressureGate *chanwatch.PressureGate
}

// SetPressureGate wires the orchestrator's pressure gate into the ingester.
// Implements ingestion.PressureAware.
func (p *pressureAware) SetPressureGate(gate *chanwatch.PressureGate) {
	p.pressureGate = gate
}

// Config holds MQTT ingester configuration.
type Config struct {
	ID           string
	Broker       string
	Topics       []string
	ClientID     string
	QoS          byte
	TLS          bool
	CleanSession bool
	Username     string
	Password     string
	Version      int // 3 or 5; default 3 (v3.1.1)
	Logger       *slog.Logger
}

// New creates an MQTT ingester for the configured protocol version.
func New(cfg Config) ingestion.Ingester {
	logger := comp.Ingester.Sub("mqtt").Desc("MQTT subscribing ingester — accepts messages from MQTT v3.1.1 and v5 brokers.").Apply(logging.Default(cfg.Logger))
	if cfg.Version == 5 {
		return &v5Ingester{cfg: cfg, logger: logger}
	}
	return &v3Ingester{cfg: cfg, logger: logger}
}
