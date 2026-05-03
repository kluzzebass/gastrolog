package main

import (
	"io"
	"log/slog"
	"testing"

	"gastrolog/internal/logging"
)

func newTestFilter() *logging.ComponentFilterHandler {
	base := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	return logging.NewComponentFilterHandler(base, slog.LevelInfo)
}

// TestApplyLogLevelSpec covers the --log-level startup flag parser:
// component=level pairs go into per-component overrides; the magic
// "default" key updates the fallback level; whitespace and trailing
// commas are tolerated.
func TestApplyLogLevelSpec(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		spec      string
		wantDef   slog.Level
		wantComps map[string]slog.Level
		wantErr   bool
	}{
		{
			name:    "single component",
			spec:    "chunk=debug",
			wantDef: slog.LevelInfo,
			wantComps: map[string]slog.Level{
				"chunk": slog.LevelDebug,
			},
		},
		{
			name:    "multiple components and default",
			spec:    "default=warn,chunk=debug,replication=info",
			wantDef: slog.LevelWarn,
			wantComps: map[string]slog.Level{
				"chunk":       slog.LevelDebug,
				"replication": slog.LevelInfo,
			},
		},
		{
			name:    "trailing whitespace and commas",
			spec:    "  chunk = debug , replication=warn,  ",
			wantDef: slog.LevelInfo,
			wantComps: map[string]slog.Level{
				"chunk":       slog.LevelDebug,
				"replication": slog.LevelWarn,
			},
		},
		{
			name:    "level alias 'warning'",
			spec:    "x=warning",
			wantDef: slog.LevelInfo,
			wantComps: map[string]slog.Level{
				"x": slog.LevelWarn,
			},
		},
		{
			name:    "missing equals sign",
			spec:    "chunk-debug",
			wantErr: true,
		},
		{
			name:    "unknown level",
			spec:    "chunk=verbose",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFilter()
			err := applyLogLevelSpec(h, tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := h.DefaultLevel(); got != tc.wantDef {
				t.Errorf("default = %v, want %v", got, tc.wantDef)
			}
			overrides := h.Overrides()
			if len(overrides) != len(tc.wantComps) {
				t.Errorf("overrides len = %d, want %d (%v vs %v)",
					len(overrides), len(tc.wantComps), overrides, tc.wantComps)
			}
			for k, want := range tc.wantComps {
				if got := overrides[k]; got != want {
					t.Errorf("override[%s] = %v, want %v", k, got, want)
				}
			}
		})
	}
}
