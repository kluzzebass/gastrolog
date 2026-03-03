package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"gastrolog/internal/orchestrator"
)

// v5Ingester uses the paho.golang autopaho v5 client.
type v5Ingester struct {
	cfg    Config
	logger *slog.Logger
}

func (ing *v5Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	brokerURL, err := url.Parse(ing.cfg.Broker)
	if err != nil {
		return fmt.Errorf("mqtt ingester: invalid broker URL %q: %w", ing.cfg.Broker, err)
	}

	subs := make([]paho.SubscribeOptions, len(ing.cfg.Topics))
	for i, t := range ing.cfg.Topics {
		subs[i] = paho.SubscribeOptions{Topic: t, QoS: ing.cfg.QoS}
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: ing.cfg.CleanSession,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			ing.logger.Info("mqtt connection up", "broker", ing.cfg.Broker)
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs}); err != nil {
				ing.logger.Error("mqtt subscribe failed", "error", err)
			}
		},
		OnConnectError: func(err error) {
			ing.logger.Warn("mqtt connection attempt failed", "error", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: ing.cfg.ClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					msg := orchestrator.IngestMessage{
						Attrs: map[string]string{
							"ingester_type":   "mqtt",
							"mqtt_topic":      pr.Packet.Topic,
							"mqtt_qos":        strconv.Itoa(int(pr.Packet.QoS)),
							"mqtt_retained":   strconv.FormatBool(pr.Packet.Retain),
							"mqtt_message_id": strconv.Itoa(int(pr.Packet.PacketID)),
						},
						Raw:        pr.Packet.Payload,
						IngestTS:   time.Now(),
						IngesterID: ing.cfg.ID,
					}

					select {
					case out <- msg:
					case <-ctx.Done():
					}
					return true, nil
				},
			},
			OnClientError: func(err error) {
				ing.logger.Warn("mqtt client error", "error", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					ing.logger.Warn("mqtt server disconnect", "reason", d.Properties.ReasonString)
				} else {
					ing.logger.Warn("mqtt server disconnect", "reason_code", d.ReasonCode)
				}
			},
		},
	}

	if ing.cfg.TLS {
		cliCfg.TlsCfg = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	if ing.cfg.Username != "" {
		cliCfg.ConnectUsername = ing.cfg.Username
		cliCfg.ConnectPassword = []byte(ing.cfg.Password)
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return fmt.Errorf("mqtt ingester: %w", err)
	}

	ing.logger.Info("mqtt ingester started",
		"broker", ing.cfg.Broker,
		"topics", ing.cfg.Topics,
		"client_id", ing.cfg.ClientID,
		"qos", ing.cfg.QoS,
		"version", 5,
	)

	<-ctx.Done()
	ing.logger.Info("mqtt ingester stopping")

	disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = cm.Disconnect(disconnectCtx)

	return nil
}
