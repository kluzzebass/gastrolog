package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/logging"
)

// newLogLevelsServer builds a minimal SystemServer with just the log-level
// surface wired. Other dependencies are nil — the log-level handlers don't
// touch them.
func newLogLevelsServer(defaultLevel slog.Level) *SystemServer {
	base := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	filter := logging.NewComponentFilterHandler(base, defaultLevel)
	return &SystemServer{logLevels: filter}
}

// TestGetLogLevels_DefaultOnly returns the default level + empty overrides
// when nothing has been overridden.
func TestGetLogLevels_DefaultOnly(t *testing.T) {
	t.Parallel()
	s := newLogLevelsServer(slog.LevelInfo)

	resp, err := s.GetLogLevels(context.Background(), connect.NewRequest(&apiv1.GetLogLevelsRequest{}))
	if err != nil {
		t.Fatalf("GetLogLevels: %v", err)
	}
	if resp.Msg.GetDefaultLevel() != apiv1.LogLevel_LOG_LEVEL_INFO {
		t.Errorf("default = %v, want INFO", resp.Msg.GetDefaultLevel())
	}
	if len(resp.Msg.GetOverrides()) != 0 {
		t.Errorf("overrides = %v, want empty", resp.Msg.GetOverrides())
	}
}

// TestSetLogLevel covers both an explicit component override and the
// default-level update path (empty component).
func TestSetLogLevel(t *testing.T) {
	t.Parallel()
	s := newLogLevelsServer(slog.LevelInfo)

	// Override one component.
	_, err := s.SetLogLevel(context.Background(), connect.NewRequest(&apiv1.SetLogLevelRequest{
		Component: "chunk-manager",
		Level:     apiv1.LogLevel_LOG_LEVEL_DEBUG,
	}))
	if err != nil {
		t.Fatalf("SetLogLevel: %v", err)
	}
	if got := s.logLevels.Level("chunk-manager"); got != slog.LevelDebug {
		t.Errorf("level after Set = %v, want DEBUG", got)
	}

	// Bump the default to WARN.
	_, err = s.SetLogLevel(context.Background(), connect.NewRequest(&apiv1.SetLogLevelRequest{
		Component: "",
		Level:     apiv1.LogLevel_LOG_LEVEL_WARN,
	}))
	if err != nil {
		t.Fatalf("SetLogLevel(default): %v", err)
	}
	if got := s.logLevels.DefaultLevel(); got != slog.LevelWarn {
		t.Errorf("default after Set = %v, want WARN", got)
	}
	// Component override stays put even after the default changes.
	if got := s.logLevels.Level("chunk-manager"); got != slog.LevelDebug {
		t.Errorf("override leaked: chunk-manager = %v, want DEBUG", got)
	}
}

// TestClearLogLevel removes a previously-set override.
func TestClearLogLevel(t *testing.T) {
	t.Parallel()
	s := newLogLevelsServer(slog.LevelInfo)

	_, _ = s.SetLogLevel(context.Background(), connect.NewRequest(&apiv1.SetLogLevelRequest{
		Component: "replication",
		Level:     apiv1.LogLevel_LOG_LEVEL_DEBUG,
	}))

	_, err := s.ClearLogLevel(context.Background(), connect.NewRequest(&apiv1.ClearLogLevelRequest{
		Component: "replication",
	}))
	if err != nil {
		t.Fatalf("ClearLogLevel: %v", err)
	}
	if got := s.logLevels.Level("replication"); got != slog.LevelInfo {
		t.Errorf("level after Clear = %v, want default INFO", got)
	}
}

// TestLogLevels_NotWired returns Unimplemented when the SystemServer was
// built without a filter handle.
func TestLogLevels_NotWired(t *testing.T) {
	t.Parallel()
	s := &SystemServer{logLevels: nil}
	if _, err := s.GetLogLevels(context.Background(), connect.NewRequest(&apiv1.GetLogLevelsRequest{})); err == nil {
		t.Error("GetLogLevels with nil handler should error")
	}
	if _, err := s.SetLogLevel(context.Background(), connect.NewRequest(&apiv1.SetLogLevelRequest{})); err == nil {
		t.Error("SetLogLevel with nil handler should error")
	}
	if _, err := s.ClearLogLevel(context.Background(), connect.NewRequest(&apiv1.ClearLogLevelRequest{})); err == nil {
		t.Error("ClearLogLevel with nil handler should error")
	}
}
