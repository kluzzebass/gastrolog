package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	pahov3 "github.com/eclipse/paho.mqtt.golang"

	"gastrolog/internal/orchestrator"
)

// v3Ingester uses the paho.mqtt.golang v3.1.1 client.
type v3Ingester struct {
	pressureAware
	cfg    Config
	logger *slog.Logger
}

func (ing *v3Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	opts := pahov3.NewClientOptions().
		AddBroker(ing.cfg.Broker).
		SetClientID(ing.cfg.ClientID).
		SetCleanSession(ing.cfg.CleanSession).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetOrderMatters(false)

	if ing.cfg.TLS {
		opts.SetTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})
	}

	if ing.cfg.Username != "" {
		opts.SetUsername(ing.cfg.Username)
		opts.SetPassword(ing.cfg.Password)
	}

	handler := func(_ pahov3.Client, m pahov3.Message) {
		// Backpressure: block in the handler while pressure is elevated.
		// Paho's in-flight limit then throttles reads from the broker, so
		// QoS 1/2 messages stay unACK'd at the broker — lossless.
		if ing.pressureGate != nil {
			if err := ing.pressureGate.Wait(ctx); err != nil {
				return
			}
		}
		msg := buildMessage(m.Topic(), m.Qos(), m.Retained(), m.MessageID(), m.Payload(), ing.cfg.ID, time.Now())

		select {
		case out <- msg:
		case <-ctx.Done():
		}
	}

	// Subscribe on every (re)connect.
	opts.SetOnConnectHandler(func(c pahov3.Client) {
		ing.logger.Info("mqtt connection up", "broker", ing.cfg.Broker)
		filters := make(map[string]byte, len(ing.cfg.Topics))
		for _, t := range ing.cfg.Topics {
			filters[t] = ing.cfg.QoS
		}
		token := c.SubscribeMultiple(filters, handler)
		if token.Wait() && token.Error() != nil {
			ing.logger.Error("mqtt subscribe failed", "error", token.Error())
		}
	})

	opts.SetConnectionLostHandler(func(_ pahov3.Client, err error) {
		ing.logger.Warn("mqtt connection lost", "error", err)
	})

	client := pahov3.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt ingester: %w", token.Error())
	}

	ing.logger.Info("mqtt ingester started",
		"broker", ing.cfg.Broker,
		"topics", ing.cfg.Topics,
		"client_id", ing.cfg.ClientID,
		"qos", ing.cfg.QoS,
		"version", "3.1.1",
	)

	<-ctx.Done()
	ing.logger.Info("mqtt ingester stopping")
	client.Disconnect(5000)

	return nil
}
