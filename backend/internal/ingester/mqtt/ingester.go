// Package mqtt provides an MQTT subscribing ingester supporting both
// MQTT v3.1.1 (paho.mqtt.golang) and v5 (paho.golang/autopaho).
package mqtt

import (
	"log/slog"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

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
	Password     string //nolint:gosec // G117: config field, not a hardcoded credential
	Version      int    // 3 or 5; default 3 (v3.1.1)
	Logger       *slog.Logger
}

// New creates an MQTT ingester for the configured protocol version.
func New(cfg Config) orchestrator.Ingester {
	logger := logging.Default(cfg.Logger).With("component", "ingester", "type", "mqtt")
	if cfg.Version == 5 {
		return &v5Ingester{cfg: cfg, logger: logger}
	}
	return &v3Ingester{cfg: cfg, logger: logger}
}
