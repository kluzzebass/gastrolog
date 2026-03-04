package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConnection creates a temporary Kafka client, pings the brokers,
// and closes the connection. Returns a human-readable summary on success.
func TestConnection(ctx context.Context, params map[string]string) (string, error) {
	brokers := params["brokers"]
	if brokers == "" {
		return "", errors.New("brokers param is required")
	}

	brokerList := strings.Split(brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokerList...),
	}

	useTLS := params["tls"] == "true"
	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}

	if mech := params["sasl_mechanism"]; mech != "" {
		saslCfg := &SASLConfig{
			Mechanism: strings.ToLower(mech),
			User:      params["sasl_user"],
			Password:  params["sasl_password"],
		}
		m, err := buildSASLMechanism(saslCfg)
		if err != nil {
			return "", err
		}
		opts = append(opts, kgo.SASL(m))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return "", fmt.Errorf("create client: %w", err)
	}
	defer client.Close()

	testCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := client.Ping(testCtx); err != nil {
		return "", err
	}

	return "Connected — Kafka at " + brokers, nil
}
