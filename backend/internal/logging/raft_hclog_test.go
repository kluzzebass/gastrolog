package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNewRaftGroupHclog_setsGroupAttrOnly(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := NewRaftGroupHclog(logger, "cluster-ctl")
	h.Warn("heartbeat timeout reached, starting election", "last-leader-id", "node-1")

	out := buf.String()
	if !strings.Contains(out, "group=cluster-ctl") {
		t.Fatalf("expected group attr, got: %s", out)
	}
	if strings.Contains(out, "[cluster-ctl]") {
		t.Fatalf("group id must not be prefixed into msg, got: %s", out)
	}
	if !strings.Contains(out, "heartbeat timeout reached, starting election") {
		t.Fatalf("expected plain message text, got: %s", out)
	}
}

func TestNewRaftGroupHclog_withCarriesGroupAttr(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := NewRaftGroupHclog(logger, "vault/foo/ctl").With("term", 4)
	h.Info("entering candidate state")

	out := buf.String()
	if !strings.Contains(out, "group=vault/foo/ctl") {
		t.Fatalf("expected group attr after With, got: %s", out)
	}
	if strings.Contains(out, "[vault/foo/ctl]") {
		t.Fatalf("group id must not be prefixed into msg, got: %s", out)
	}
}
