package mqtt

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	pahov3 "github.com/eclipse/paho.mqtt.golang"
)

// TestConnection creates a temporary MQTT client, connects to the broker,
// and disconnects. Returns a human-readable summary on success.
func TestConnection(ctx context.Context, params map[string]string) (string, error) {
	broker := params["broker"]
	if broker == "" {
		return "", errors.New("broker param is required")
	}

	useTLS := params["tls"] == "true"

	version := 3
	if v := params["version"]; v != "" {
		switch v {
		case "3", "5":
			version, _ = strconv.Atoi(v)
		default:
			return "", fmt.Errorf("invalid version %q (must be 3 or 5)", v)
		}
	}

	testCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if version == 5 {
		return testV5(testCtx, broker, useTLS, params)
	}
	return testV3(testCtx, broker, useTLS, params)
}

func testV3(ctx context.Context, broker string, useTLS bool, params map[string]string) (string, error) {
	clientID := cmp.Or(params["client_id"], "gastrolog-test")

	opts := pahov3.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectTimeout(10 * time.Second)

	if useTLS {
		opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	if params["username"] != "" {
		opts.SetUsername(params["username"])
		opts.SetPassword(params["password"])
	}

	client := pahov3.NewClient(opts)
	token := client.Connect()

	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return "", fmt.Errorf("connection timed out: %w", ctx.Err())
	}

	if token.Error() != nil {
		return "", token.Error()
	}

	client.Disconnect(1000)

	topicsRaw := params["topics"]
	var topicInfo string
	if topicsRaw != "" {
		topics := strings.Split(topicsRaw, ",")
		topicInfo = fmt.Sprintf(", %d topic(s)", len(topics))
	}

	return fmt.Sprintf("Connected — MQTT v3.1.1 at %s%s", broker, topicInfo), nil
}

func testV5(ctx context.Context, broker string, useTLS bool, params map[string]string) (string, error) {
	brokerURL, err := url.Parse(broker)
	if err != nil {
		return "", fmt.Errorf("invalid broker URL %q: %w", broker, err)
	}

	clientID := cmp.Or(params["client_id"], "gastrolog-test")

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
		},
	}

	if useTLS {
		cliCfg.TlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if params["username"] != "" {
		cliCfg.ConnectUsername = params["username"]
		cliCfg.ConnectPassword = []byte(params["password"])
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return "", err
	}

	if err := cm.AwaitConnection(ctx); err != nil {
		return "", err
	}

	disconnectCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = cm.Disconnect(disconnectCtx)

	topicsRaw := params["topics"]
	var topicInfo string
	if topicsRaw != "" {
		topics := strings.Split(topicsRaw, ",")
		topicInfo = fmt.Sprintf(", %d topic(s)", len(topics))
	}

	return fmt.Sprintf("Connected — MQTT v5 at %s%s", broker, topicInfo), nil
}
