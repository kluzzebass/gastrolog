package mqtt

import (
	"cmp"
	"errors"
	"fmt"
	"gastrolog/internal/cert"
	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
	"strconv"
	"strings"
)

// ParamDefaults returns the default parameter values for an MQTT ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"version":       "3",
		"clean_session": "true",
	}
}

// NewFactory returns an IngesterFactory for MQTT ingesters.
func NewFactory(certMgr *cert.Manager) ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
		broker := params["broker"]
		if broker == "" {
			return nil, errors.New("mqtt ingester: broker param is required")
		}

		topicsRaw := params["topics"]
		if topicsRaw == "" {
			return nil, errors.New("mqtt ingester: topics param is required")
		}

		topics := strings.Split(topicsRaw, ",")
		for i := range topics {
			topics[i] = strings.TrimSpace(topics[i])
		}

		idStr := id.String()
		clientID := cmp.Or(params["client_id"], "gastrolog-"+idStr[len(idStr)-8:])

		const qos = 1 // Subscribe at QoS 1 (at least once); broker delivers at min(pub, sub).

		tlsCfg, insecure, err := ingesttls.Client("mqtt", params, certMgr)
		if err != nil {
			return nil, err
		}
		if insecure {
			logger.Warn("mqtt ingester: tls_verify=false disables broker verification — a network position between this node and the broker can read and forge records; tls_ca with a stored CA certificate covers the self-signed case safely")
		}
		cleanSession := params["clean_session"] != "false"

		version := 3
		if v := params["version"]; v != "" {
			switch v {
			case "3", "5":
				version, _ = strconv.Atoi(v)
			default:
				return nil, fmt.Errorf("mqtt ingester: invalid version %q (must be 3 or 5)", v)
			}
		}

		return New(Config{
			ID:           id.String(),
			Broker:       broker,
			Topics:       topics,
			ClientID:     clientID,
			QoS:          byte(qos),
			TLSConfig:    tlsCfg,
			CleanSession: cleanSession,
			Username:     params["username"],
			Password:     params["password"],
			Version:      version,
			Logger:       logger,
		}), nil
	}
}
