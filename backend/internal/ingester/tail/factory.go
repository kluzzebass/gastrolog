package tail

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for a tail ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"poll_interval": "30s",
	}
}

// NewFactory returns an IngesterFactory for file tail ingesters.
func NewFactory() orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		cfg, err := parseConfig(id.String(), params, logger)
		if err != nil {
			return nil, err
		}
		return newIngester(cfg), nil
	}
}

// config holds parsed configuration for a tail ingester.
type config struct {
	ID           string
	Patterns     []string
	PollInterval time.Duration
	StateFile    string
	Logger       *slog.Logger
}

func parseConfig(id string, params map[string]string, logger *slog.Logger) (config, error) {
	pathsJSON := params["paths"]
	if pathsJSON == "" {
		return config{}, fmt.Errorf("tail ingester %q: paths param required (JSON array of glob patterns)", id)
	}

	var patterns []string
	if err := json.Unmarshal([]byte(pathsJSON), &patterns); err != nil {
		return config{}, fmt.Errorf("tail ingester %q: invalid paths JSON: %w", id, err)
	}
	if len(patterns) == 0 {
		return config{}, fmt.Errorf("tail ingester %q: paths must contain at least one pattern", id)
	}

	pollInterval := 30 * time.Second
	if v := params["poll_interval"]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return config{}, fmt.Errorf("tail ingester %q: invalid poll_interval %q: %w", id, v, err)
		}
		if d < 0 {
			return config{}, fmt.Errorf("tail ingester %q: poll_interval must be non-negative", id)
		}
		pollInterval = d
	}

	var stateFile string
	if stateDir := params["_state_dir"]; stateDir != "" {
		stateFile = filepath.Join(stateDir, "state", "tail", id+".json")
	}

	return config{
		ID:           id,
		Patterns:     patterns,
		PollInterval: pollInterval,
		StateFile:    stateFile,
		Logger:       logging.Default(logger).With("component", "ingester", "type", "tail", "instance", id),
	}, nil
}
