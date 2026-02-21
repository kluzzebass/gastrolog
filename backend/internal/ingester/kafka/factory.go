package kafka

import (
	"cmp"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

// NewFactory returns an IngesterFactory for Kafka ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		brokers := params["brokers"]
		if brokers == "" {
			return nil, fmt.Errorf("kafka ingester: brokers param is required")
		}

		topic := params["topic"]
		if topic == "" {
			return nil, fmt.Errorf("kafka ingester: topic param is required")
		}

		group := cmp.Or(params["group"], "gastrolog")
		tls := params["tls"] == "true"

		var sasl *SASLConfig
		if mech := params["sasl_mechanism"]; mech != "" {
			switch strings.ToLower(mech) {
			case "plain", "scram-sha-256", "scram-sha-512":
			default:
				return nil, fmt.Errorf("kafka ingester: unsupported sasl_mechanism %q (supported: plain, scram-sha-256, scram-sha-512)", mech)
			}
			sasl = &SASLConfig{
				Mechanism: strings.ToLower(mech),
				User:      params["sasl_user"],
				Password:  params["sasl_password"],
			}
		}

		brokerList := strings.Split(brokers, ",")
		for i := range brokerList {
			brokerList[i] = strings.TrimSpace(brokerList[i])
		}

		return New(Config{
			ID:      id.String(),
			Brokers: brokerList,
			Topic:   topic,
			Group:   group,
			TLS:     tls,
			SASL:    sasl,
			Logger:  logger,
		}), nil
	}
}
