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

	"gastrolog/internal/chanwatch"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

const (
	backoffMin = 100 * time.Millisecond
	backoffMax = 5 * time.Second
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

	// pressureGate throttles PollFetches calls when the ingest pipeline is
	// backed up. Kafka offset tracking makes pausing lossless — we resume
	// from the same offset when pressure clears. Injected by the
	// orchestrator; nil means no throttling. See gastrolog-4fguu.
	pressureGate *chanwatch.PressureGate
}

// SetPressureGate wires the orchestrator's pressure gate into the ingester.
// Implements orchestrator.PressureAware.
func (ing *Ingester) SetPressureGate(gate *chanwatch.PressureGate) {
	ing.pressureGate = gate
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

	ing.logger.Info("kafka ingester starting",
		"brokers", ing.cfg.Brokers,
		"topic", ing.cfg.Topic,
		"group", ing.cfg.Group,
	)

	backoff := backoffMin

	for {
		if ing.shouldExit(ctx) {
			ing.shutdown(client)
			return nil
		}

		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			ing.shutdown(client)
			return nil
		}

		if ing.handleFetchErrors(fetches, &backoff, ctx) {
			continue
		}
		backoff = backoffMin // reset on successful fetch

		now := time.Now()

		fetches.EachRecord(func(rec *kgo.Record) {
			msg := buildMessage(rec, ing.cfg.ID, now)
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		})
	}
}

// shouldExit handles the pressure-gate wait and returns true if the loop
// should terminate due to ctx cancellation observed during Wait.
func (ing *Ingester) shouldExit(ctx context.Context) bool {
	// Backpressure: pause polling while the pipeline is backed up. This is
	// lossless — the consumer group offset stays put, so when we resume
	// we pick up from the same record.
	if ing.pressureGate != nil {
		_ = ing.pressureGate.Wait(ctx)
	}
	return ctx.Err() != nil
}

// shutdown logs the stop, then tries to commit uncommitted offsets on a
// background context so in-flight offsets aren't lost at shutdown.
func (ing *Ingester) shutdown(client *kgo.Client) {
	ing.logger.Info("kafka ingester stopping")
	if err := client.CommitUncommittedOffsets(context.Background()); err != nil {
		ing.logger.Warn("kafka: failed to commit offsets on shutdown", "error", err)
	}
}

// handleFetchErrors logs fetch errors and applies exponential backoff if
// the fetch returned only errors (no records). Returns true if the caller
// should `continue` the loop without processing records.
func (ing *Ingester) handleFetchErrors(fetches kgo.Fetches, backoff *time.Duration, ctx context.Context) bool {
	fetchErrs := fetches.Errors()
	for _, e := range fetchErrs {
		ing.logger.Warn("kafka fetch error",
			"topic", e.Topic,
			"partition", e.Partition,
			"error", e.Err,
		)
	}
	if fetches.NumRecords() > 0 || len(fetchErrs) == 0 {
		return false
	}
	// Errors with no records — back off to avoid tight-looping.
	select {
	case <-time.After(*backoff):
	case <-ctx.Done():
	}
	*backoff = min(*backoff*2, backoffMax)
	return true
}

// buildMessage converts a kgo.Record into an orchestrator.IngestMessage.
func buildMessage(rec *kgo.Record, ingesterID string, now time.Time) orchestrator.IngestMessage {
	attrs := make(map[string]string, len(rec.Headers)+5)
	attrs["ingester_type"] = "kafka"
	attrs["kafka_topic"] = rec.Topic
	attrs["kafka_partition"] = strconv.Itoa(int(rec.Partition))
	attrs["kafka_offset"] = strconv.FormatInt(rec.Offset, 10)
	for _, h := range rec.Headers {
		attrs[h.Key] = string(h.Value)
	}
	return orchestrator.IngestMessage{
		Attrs:      attrs,
		Raw:        rec.Value,
		SourceTS:   rec.Timestamp,
		IngestTS:   now,
		IngesterID: ingesterID,
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
