package mqtt

import (
	"testing"

	"github.com/google/uuid"
)

func TestFactory_RequiredParams(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	_, err := factory(id, map[string]string{}, nil)
	if err == nil || err.Error() != "mqtt ingester: broker param is required" {
		t.Fatalf("expected broker required error, got %v", err)
	}

	_, err = factory(id, map[string]string{"broker": "mqtt://localhost:1883"}, nil)
	if err == nil || err.Error() != "mqtt ingester: topics param is required" {
		t.Fatalf("expected topics required error, got %v", err)
	}
}

func TestFactory_DefaultsV3(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"broker": "mqtt://localhost:1883",
		"topics": "test/topic",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v3, ok := ing.(*v3Ingester)
	if !ok {
		t.Fatalf("expected v3Ingester by default, got %T", ing)
	}
	if !v3.cfg.CleanSession {
		t.Error("expected default clean_session true")
	}
	idStr := id.String()
	wantSuffix := idStr[len(idStr)-8:]
	if v3.cfg.ClientID != "gastrolog-"+wantSuffix {
		t.Errorf("expected default client ID gastrolog-%s, got %s", wantSuffix, v3.cfg.ClientID)
	}
}

func TestFactory_V5(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"broker":  "mqtt://localhost:1883",
		"topics":  "test",
		"version": "5",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := ing.(*v5Ingester); !ok {
		t.Fatalf("expected v5Ingester, got %T", ing)
	}
}

func TestFactory_VersionValidation(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	base := map[string]string{
		"broker": "mqtt://localhost:1883",
		"topics": "test",
	}

	for _, valid := range []string{"3", "5", ""} {
		params := copyMap(base)
		if valid != "" {
			params["version"] = valid
		}
		if _, err := factory(id, params, nil); err != nil {
			t.Errorf("version=%q should be valid, got error: %v", valid, err)
		}
	}

	for _, invalid := range []string{"4", "31", "abc"} {
		params := copyMap(base)
		params["version"] = invalid
		if _, err := factory(id, params, nil); err == nil {
			t.Errorf("version=%q should be invalid", invalid)
		}
	}
}


func TestFactory_TopicSplitting(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"broker": "mqtt://localhost:1883",
		"topics": "sensors/+/temp , home/# , raw",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v3 := ing.(*v3Ingester)
	expected := []string{"sensors/+/temp", "home/#", "raw"}
	if len(v3.cfg.Topics) != len(expected) {
		t.Fatalf("expected %d topics, got %d", len(expected), len(v3.cfg.Topics))
	}
	for i, topic := range v3.cfg.Topics {
		if topic != expected[i] {
			t.Errorf("topic[%d]: expected %q, got %q", i, expected[i], topic)
		}
	}
}

func TestFactory_ClientID(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"broker":    "mqtt://localhost:1883",
		"topics":    "test",
		"client_id": "my-client",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v3 := ing.(*v3Ingester)
	if v3.cfg.ClientID != "my-client" {
		t.Errorf("expected custom client ID, got %s", v3.cfg.ClientID)
	}
}

func TestFactory_TLSAndAuth(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"broker":   "ssl://broker:8883",
		"topics":   "test",
		"tls":      "true",
		"username": "user",
		"password": "pass",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v3 := ing.(*v3Ingester)
	if !v3.cfg.TLS {
		t.Error("expected TLS true")
	}
	if v3.cfg.Username != "user" {
		t.Errorf("expected username 'user', got %q", v3.cfg.Username)
	}
	if v3.cfg.Password != "pass" {
		t.Errorf("expected password 'pass', got %q", v3.cfg.Password)
	}
}

func TestFactory_CleanSessionFalse(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	id := uuid.New()

	ing, err := factory(id, map[string]string{
		"broker":        "mqtt://localhost:1883",
		"topics":        "test",
		"clean_session": "false",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	v3 := ing.(*v3Ingester)
	if v3.cfg.CleanSession {
		t.Error("expected clean_session false")
	}
}

func TestParamDefaults(t *testing.T) {
	t.Parallel()
	d := ParamDefaults()
	if d["version"] != "3" {
		t.Errorf("expected default version=3, got %q", d["version"])
	}
	if d["clean_session"] != "true" {
		t.Errorf("expected default clean_session=true, got %q", d["clean_session"])
	}
	if _, ok := d["qos"]; ok {
		t.Error("qos should not be in defaults (hardcoded)")
	}
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
