// Package kafka provides a Kafka consumer ingester using franz-go.
package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// SASLConfig holds SASL authentication parameters.
type SASLConfig struct {
	Mechanism string // "plain", "scram-sha-256", "scram-sha-512"
	User      string
	Password  string //nolint:gosec // G117: config field, not a hardcoded credential
}

// Config holds Kafka ingester configuration.
type Config struct {
	ID      string
	Brokers []string
	Topic   string
	Group   string
	TLS     bool
	SASL    *SASLConfig
	Logger  *slog.Logger
}

// Ingester consumes messages from a Kafka topic.
type Ingester struct {
	cfg    Config
	logger *slog.Logger
}

// New creates a new Kafka ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		cfg:    cfg,
		logger: logging.Default(cfg.Logger).With("component", "ingester", "type", "kafka"),
	}
}

// Run connects to Kafka and polls messages until ctx is cancelled.
func (ing *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(ing.cfg.Brokers...),
		kgo.ConsumeTopics(ing.cfg.Topic),
		kgo.ConsumerGroup(ing.cfg.Group),
	}

	if ing.cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}

	if ing.cfg.SASL != nil {
		mech, err := buildSASLMechanism(ing.cfg.SASL)
		if err != nil {
			return err
		}
		opts = append(opts, kgo.SASL(mech))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()

	ing.logger.Info("kafka consumer started",
		"brokers", ing.cfg.Brokers,
		"topic", ing.cfg.Topic,
		"group", ing.cfg.Group,
	)

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			ing.logger.Info("kafka consumer stopping")
			_ = client.CommitUncommittedOffsets(context.Background())
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				ing.logger.Warn("kafka fetch error",
					"topic", e.Topic,
					"partition", e.Partition,
					"error", e.Err,
				)
			}
		}

		now := time.Now()

		fetches.EachRecord(func(rec *kgo.Record) {
			attrs := make(map[string]string, len(rec.Headers)+6)
			attrs["ingester_type"] = "kafka"
			attrs["ingester_id"] = ing.cfg.ID
			attrs["kafka_topic"] = rec.Topic
			attrs["kafka_partition"] = strconv.Itoa(int(rec.Partition))
			attrs["kafka_offset"] = strconv.FormatInt(rec.Offset, 10)

			for _, h := range rec.Headers {
				attrs[h.Key] = string(h.Value)
			}

			msg := orchestrator.IngestMessage{
				Attrs:    attrs,
				Raw:      rec.Value,
				SourceTS: rec.Timestamp,
				IngestTS: now,
			}

			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		})
	}
}

// buildSASLMechanism constructs the appropriate SASL mechanism.
func buildSASLMechanism(cfg *SASLConfig) (sasl.Mechanism, error) {
	switch cfg.Mechanism {
	case "plain":
		return plain.Auth{
			User: cfg.User,
			Pass: cfg.Password,
		}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{
			User: cfg.User,
			Pass: cfg.Password,
		}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{
			User: cfg.User,
			Pass: cfg.Password,
		}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %q", cfg.Mechanism)
	}
}
