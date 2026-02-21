package kafka

import (
	"testing"

	"github.com/google/uuid"
)

// --- Factory Tests ---

func TestFactoryRequiresBrokers(t *testing.T) {
	factory := NewFactory()

	_, err := factory(uuid.New(), map[string]string{
		"topic": "logs",
	}, nil)
	if err == nil {
		t.Fatal("expected error when brokers is missing")
	}
}

func TestFactoryRequiresTopic(t *testing.T) {
	factory := NewFactory()

	_, err := factory(uuid.New(), map[string]string{
		"brokers": "localhost:9092",
	}, nil)
	if err == nil {
		t.Fatal("expected error when topic is missing")
	}
}

func TestFactoryMinimalParams(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "localhost:9092",
		"topic":   "logs",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	ki := ing.(*Ingester)
	if ki.cfg.Group != "gastrolog" {
		t.Errorf("default group: expected gastrolog, got %q", ki.cfg.Group)
	}
	if ki.cfg.TLS {
		t.Error("TLS should be false by default")
	}
	if ki.cfg.SASL != nil {
		t.Error("SASL should be nil by default")
	}
}

func TestFactoryMultipleBrokers(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "broker1:9092, broker2:9092 , broker3:9092",
		"topic":   "logs",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if len(ki.cfg.Brokers) != 3 {
		t.Fatalf("expected 3 brokers, got %d", len(ki.cfg.Brokers))
	}
	expected := []string{"broker1:9092", "broker2:9092", "broker3:9092"}
	for i, b := range ki.cfg.Brokers {
		if b != expected[i] {
			t.Errorf("broker %d: expected %q, got %q", i, expected[i], b)
		}
	}
}

func TestFactoryCustomGroup(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "localhost:9092",
		"topic":   "logs",
		"group":   "my-consumers",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.Group != "my-consumers" {
		t.Errorf("group: expected my-consumers, got %q", ki.cfg.Group)
	}
}

func TestFactoryTLSEnabled(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "localhost:9093",
		"topic":   "logs",
		"tls":     "true",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if !ki.cfg.TLS {
		t.Error("TLS should be true")
	}
}

func TestFactoryTLSNotEnabled(t *testing.T) {
	factory := NewFactory()

	// "tls" set to something other than "true" should be false.
	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "localhost:9092",
		"topic":   "logs",
		"tls":     "false",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.TLS {
		t.Error("TLS should be false when set to 'false'")
	}
}

// --- SASL Factory Tests ---

func TestFactorySASLPlain(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers":        "localhost:9092",
		"topic":          "logs",
		"sasl_mechanism": "plain",
		"sasl_user":      "alice",
		"sasl_password":  "secret",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.SASL == nil {
		t.Fatal("expected SASL config")
	}
	if ki.cfg.SASL.Mechanism != "plain" {
		t.Errorf("mechanism: expected plain, got %q", ki.cfg.SASL.Mechanism)
	}
	if ki.cfg.SASL.User != "alice" {
		t.Errorf("user: expected alice, got %q", ki.cfg.SASL.User)
	}
	if ki.cfg.SASL.Password != "secret" {
		t.Errorf("password: expected secret, got %q", ki.cfg.SASL.Password)
	}
}

func TestFactorySASLScramSHA256(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers":        "localhost:9092",
		"topic":          "logs",
		"sasl_mechanism": "scram-sha-256",
		"sasl_user":      "bob",
		"sasl_password":  "pass256",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.SASL.Mechanism != "scram-sha-256" {
		t.Errorf("mechanism: expected scram-sha-256, got %q", ki.cfg.SASL.Mechanism)
	}
}

func TestFactorySASLScramSHA512(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers":        "localhost:9092",
		"topic":          "logs",
		"sasl_mechanism": "scram-sha-512",
		"sasl_user":      "carol",
		"sasl_password":  "pass512",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.SASL.Mechanism != "scram-sha-512" {
		t.Errorf("mechanism: expected scram-sha-512, got %q", ki.cfg.SASL.Mechanism)
	}
}

func TestFactorySASLMechanismCaseInsensitive(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers":        "localhost:9092",
		"topic":          "logs",
		"sasl_mechanism": "PLAIN",
		"sasl_user":      "user",
		"sasl_password":  "pass",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.SASL.Mechanism != "plain" {
		t.Errorf("mechanism: expected plain (lowercased), got %q", ki.cfg.SASL.Mechanism)
	}
}

func TestFactorySASLUnsupportedMechanism(t *testing.T) {
	factory := NewFactory()

	_, err := factory(uuid.New(), map[string]string{
		"brokers":        "localhost:9092",
		"topic":          "logs",
		"sasl_mechanism": "kerberos",
	}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported SASL mechanism")
	}
}

func TestFactoryNoSASLWhenMechanismEmpty(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers":        "localhost:9092",
		"topic":          "logs",
		"sasl_mechanism": "",
		"sasl_user":      "ignored",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.SASL != nil {
		t.Error("SASL should be nil when mechanism is empty")
	}
}

func TestFactoryEmptyBrokersString(t *testing.T) {
	factory := NewFactory()

	_, err := factory(uuid.New(), map[string]string{
		"brokers": "",
		"topic":   "logs",
	}, nil)
	if err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestFactoryEmptyTopicString(t *testing.T) {
	factory := NewFactory()

	_, err := factory(uuid.New(), map[string]string{
		"brokers": "localhost:9092",
		"topic":   "",
	}, nil)
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestFactoryNilParams(t *testing.T) {
	factory := NewFactory()

	_, err := factory(uuid.New(), nil, nil)
	if err == nil {
		t.Fatal("expected error for nil params (missing brokers and topic)")
	}
}

// --- buildSASLMechanism Tests ---

func TestBuildSASLMechanismPlain(t *testing.T) {
	mech, err := buildSASLMechanism(&SASLConfig{
		Mechanism: "plain",
		User:      "user",
		Password:  "pass",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil mechanism")
	}
}

func TestBuildSASLMechanismScramSHA256(t *testing.T) {
	mech, err := buildSASLMechanism(&SASLConfig{
		Mechanism: "scram-sha-256",
		User:      "user",
		Password:  "pass",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil mechanism")
	}
}

func TestBuildSASLMechanismScramSHA512(t *testing.T) {
	mech, err := buildSASLMechanism(&SASLConfig{
		Mechanism: "scram-sha-512",
		User:      "user",
		Password:  "pass",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech == nil {
		t.Fatal("expected non-nil mechanism")
	}
}

func TestBuildSASLMechanismUnsupported(t *testing.T) {
	_, err := buildSASLMechanism(&SASLConfig{
		Mechanism: "oauthbearer",
	})
	if err == nil {
		t.Fatal("expected error for unsupported mechanism")
	}
}

// --- Config Construction Tests ---

func TestNewIngester(t *testing.T) {
	id := uuid.New().String()
	ing := New(Config{
		ID:      id,
		Brokers: []string{"b1:9092", "b2:9092"},
		Topic:   "test-topic",
		Group:   "test-group",
		TLS:     true,
		SASL: &SASLConfig{
			Mechanism: "plain",
			User:      "admin",
			Password:  "adminpass",
		},
	})

	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}
	if ing.cfg.ID != id {
		t.Errorf("ID: expected %q, got %q", id, ing.cfg.ID)
	}
	if ing.cfg.Topic != "test-topic" {
		t.Errorf("topic: expected test-topic, got %q", ing.cfg.Topic)
	}
	if ing.cfg.Group != "test-group" {
		t.Errorf("group: expected test-group, got %q", ing.cfg.Group)
	}
	if !ing.cfg.TLS {
		t.Error("TLS should be true")
	}
	if ing.cfg.SASL == nil {
		t.Fatal("SASL should not be nil")
	}
	if len(ing.cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(ing.cfg.Brokers))
	}
}

// --- Full Factory Integration Tests ---

func TestFactoryAllParams(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers":        "broker1:9092,broker2:9092",
		"topic":          "application-logs",
		"group":          "log-consumers",
		"tls":            "true",
		"sasl_mechanism": "SCRAM-SHA-512",
		"sasl_user":      "admin",
		"sasl_password":  "s3cret",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if len(ki.cfg.Brokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(ki.cfg.Brokers))
	}
	if ki.cfg.Brokers[0] != "broker1:9092" {
		t.Errorf("broker 0: expected broker1:9092, got %q", ki.cfg.Brokers[0])
	}
	if ki.cfg.Brokers[1] != "broker2:9092" {
		t.Errorf("broker 1: expected broker2:9092, got %q", ki.cfg.Brokers[1])
	}
	if ki.cfg.Topic != "application-logs" {
		t.Errorf("topic: expected application-logs, got %q", ki.cfg.Topic)
	}
	if ki.cfg.Group != "log-consumers" {
		t.Errorf("group: expected log-consumers, got %q", ki.cfg.Group)
	}
	if !ki.cfg.TLS {
		t.Error("TLS should be true")
	}
	if ki.cfg.SASL == nil {
		t.Fatal("SASL should not be nil")
	}
	if ki.cfg.SASL.Mechanism != "scram-sha-512" {
		t.Errorf("mechanism: expected scram-sha-512, got %q", ki.cfg.SASL.Mechanism)
	}
	if ki.cfg.SASL.User != "admin" {
		t.Errorf("user: expected admin, got %q", ki.cfg.SASL.User)
	}
	if ki.cfg.SASL.Password != "s3cret" {
		t.Errorf("password: expected s3cret, got %q", ki.cfg.SASL.Password)
	}
}

func TestFactorySingleBroker(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "kafka.example.com:9092",
		"topic":   "events",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if len(ki.cfg.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(ki.cfg.Brokers))
	}
	if ki.cfg.Brokers[0] != "kafka.example.com:9092" {
		t.Errorf("broker: expected kafka.example.com:9092, got %q", ki.cfg.Brokers[0])
	}
}

func TestFactoryBrokerWhitespaceTrimming(t *testing.T) {
	factory := NewFactory()

	ing, err := factory(uuid.New(), map[string]string{
		"brokers": "  b1:9092 ,  b2:9093  ,b3:9094  ",
		"topic":   "logs",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	expected := []string{"b1:9092", "b2:9093", "b3:9094"}
	if len(ki.cfg.Brokers) != 3 {
		t.Fatalf("expected 3 brokers, got %d", len(ki.cfg.Brokers))
	}
	for i, b := range ki.cfg.Brokers {
		if b != expected[i] {
			t.Errorf("broker %d: expected %q, got %q", i, expected[i], b)
		}
	}
}

func TestFactoryIDPropagated(t *testing.T) {
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"brokers": "localhost:9092",
		"topic":   "logs",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ki := ing.(*Ingester)
	if ki.cfg.ID != id.String() {
		t.Errorf("ID: expected %q, got %q", id.String(), ki.cfg.ID)
	}
}
