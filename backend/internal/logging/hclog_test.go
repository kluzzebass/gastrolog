package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
)

func TestEnsureHclogMinLevel_overridesDowngrade(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	base := NewHclogAdapter(slogLogger)
	logger := DowngradeHclogToDebug(
		EnsureHclogMinLevel(base, hclog.Warn, "failed to contact quorum of nodes, stepping down"),
		"pipelining replication",
	)

	logger.Warn("failed to contact quorum of nodes, stepping down")

	out := buf.String()
	if !strings.Contains(out, "failed to contact quorum of nodes, stepping down") {
		t.Fatalf("expected quorum step-down at WARN, got: %q", out)
	}
	if strings.Contains(out, "level=DEBUG") {
		t.Fatalf("quorum step-down was downgraded to DEBUG: %q", out)
	}
}

func TestDowngradeHclogToDebug_hidesQuorumWithoutEnsure(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	base := NewHclogAdapter(slogLogger)
	logger := DowngradeHclogToDebug(base, "failed to contact")

	logger.Warn("failed to contact quorum of nodes, stepping down")

	out := buf.String()
	if strings.Contains(out, "level=WARN") {
		t.Fatalf("broad downgrade pattern should hide quorum step-down at WARN, got: %q", out)
	}
}
