package level

import (
	"testing"

	"gastrolog/internal/orchestrator"
)

func TestDigest_KVFormat(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"level=ERROR", `level=ERROR msg="something failed"`, "error"},
		{"level=WARN", `level=WARN msg="retrying"`, "warn"},
		{"level=INFO", `level=INFO msg="request completed"`, "info"},
		{"level=DEBUG", `level=DEBUG msg="entering function"`, "debug"},
		{"level=error lowercase", `level=error msg="oops"`, "error"},
		{"level=warning", `level=warning msg="slow"`, "warn"},
		{"level=fatal", `level=fatal msg="crash"`, "error"},
		{"severity=error", `severity=error msg="bad"`, "error"},
		{"quoted value", `level="WARN" msg="test"`, "warn"},
		{"single quoted", `level='info' msg="test"`, "info"},
		{"mid-line", `ts=2024-01-01 level=ERROR msg="fail"`, "error"},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw:   []byte(tt.raw),
				Attrs: make(map[string]string),
			}
			d.Digest(msg)
			if got := msg.Attrs["level"]; got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDigest_JSONFormat(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"level error", `{"level":"error","msg":"fail"}`, "error"},
		{"level warn", `{"level":"warn","msg":"slow"}`, "warn"},
		{"level info", `{"level":"info","msg":"ok"}`, "info"},
		{"level debug", `{"level":"debug","msg":"trace"}`, "debug"},
		{"level ERROR uppercase", `{"level":"ERROR","msg":"fail"}`, "error"},
		{"severity field", `{"severity":"warning","msg":"hmm"}`, "warn"},
		{"spaced colon", `{"level": "error", "msg": "fail"}`, "error"},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw:   []byte(tt.raw),
				Attrs: make(map[string]string),
			}
			d.Digest(msg)
			if got := msg.Attrs["level"]; got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDigest_SyslogPriority(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"emerg (0)", "<0>Jan 1 00:00:00 host msg", "error"},
		{"alert (1)", "<9>Jan 1 00:00:00 host msg", "error"}, // facility=1, sev=1
		{"crit (2)", "<2>Jan 1 00:00:00 host msg", "error"},
		{"err (3)", "<11>Jan 1 00:00:00 host msg", "error"},             // facility=1, sev=3
		{"warning (4)", "<12>Jan 1 00:00:00 host msg", "warn"},          // facility=1, sev=4
		{"notice (5)", "<13>Jan 1 00:00:00 host msg", "info"},           // facility=1, sev=5
		{"info (6)", "<14>Jan 1 00:00:00 host msg", "info"},             // facility=1, sev=6
		{"debug (7)", "<15>Jan 1 00:00:00 host msg", "debug"},           // facility=1, sev=7
		{"high priority", "<37>Feb 6 15:04:05 host sshd: fail", "info"}, // 37%8=5=notice
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw:   []byte(tt.raw),
				Attrs: make(map[string]string),
			}
			d.Digest(msg)
			if got := msg.Attrs["level"]; got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDigest_NoMatch(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"plain text", "starting worker pool"},
		{"access log", `192.168.1.5 - - [02/Jan/2006:15:04:05] "GET /api HTTP/1.1" 200 1234`},
		{"random data", "abc123 xyz789"},
		{"empty", ""},
		{"word warning in text", "hostname mx.example.com does not resolve to address 1.2.3.4"},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw:   []byte(tt.raw),
				Attrs: make(map[string]string),
			}
			d.Digest(msg)
			if got := msg.Attrs["level"]; got != "" {
				t.Errorf("expected no level, got %q", got)
			}
		})
	}
}

func TestDigest_SkipsExistingAttr(t *testing.T) {
	d := New()

	for _, key := range []string{"level", "severity", "severity_name"} {
		t.Run(key, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw:   []byte(`level=ERROR msg="fail"`),
				Attrs: map[string]string{key: "custom"},
			}
			d.Digest(msg)
			if msg.Attrs[key] != "custom" {
				t.Errorf("existing %s attr was overwritten", key)
			}
			if key != "level" {
				if _, ok := msg.Attrs["level"]; ok {
					t.Error("level attr should not be added when", key, "exists")
				}
			}
		})
	}
}

func TestDigest_NilAttrs(t *testing.T) {
	d := New()
	msg := &orchestrator.IngestMessage{
		Raw: []byte(`level=ERROR msg="fail"`),
	}
	d.Digest(msg)
	if msg.Attrs == nil {
		t.Fatal("expected attrs to be initialized")
	}
	if got := msg.Attrs["level"]; got != "error" {
		t.Errorf("got %q, want %q", got, "error")
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"ERROR", "error"},
		{"err", "error"},
		{"FATAL", "error"},
		{"critical", "error"},
		{"EMERG", "error"},
		{"alert", "error"},
		{"crit", "error"},
		{"WARN", "warn"},
		{"warning", "warn"},
		{"INFO", "info"},
		{"notice", "info"},
		{"informational", "info"},
		{"DEBUG", "debug"},
		{"TRACE", "trace"},
		{"unknown", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := normalize(tt.input); got != tt.want {
				t.Errorf("normalize(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractSyslogPriority(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"<0>msg", "error"},
		{"<4>msg", "warn"},
		{"<6>msg", "info"},
		{"<7>msg", "debug"},
		{"<134>msg", "info"}, // 134%8=6=info
		{"no priority", ""},
		{"<>msg", ""},
		{"<abc>msg", ""},
		{"<999>msg", "debug"}, // 999%8=7=debug
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			if got := extractSyslogPriority([]byte(tt.raw)); got != tt.want {
				t.Errorf("extractSyslogPriority(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestNoFalsePositives(t *testing.T) {
	// These should NOT trigger level detection.
	tests := []struct {
		name string
		raw  string
	}{
		{"sublevel word", "This is a sublevel category"},
		{"severity in URL", "GET /api/severity/check HTTP/1.1"},
		{"level in URL path", "fetching https://example.com/level/3"},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw:   []byte(tt.raw),
				Attrs: make(map[string]string),
			}
			d.Digest(msg)
			if got := msg.Attrs["level"]; got != "" {
				t.Errorf("false positive: got level=%q for %q", got, tt.raw)
			}
		})
	}
}
