package mqtt

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for an MQTT ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"version":       "3",
		"clean_session": "true",
	}
}

// NewFactory returns an IngesterFactory for MQTT ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
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

		tls := params["tls"] == "true"
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
			TLS:          tls,
			CleanSession: cleanSession,
			Username:     params["username"],
			Password:     params["password"],
			Version:      version,
			Logger:       logger,
		}), nil
	}
}
