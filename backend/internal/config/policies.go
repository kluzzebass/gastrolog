package config

import (
	"errors"
	"fmt"
	"time"

	"gastrolog/internal/chunk"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------

// FilterConfig defines a named filter expression.
// Vaults reference filters by UUID to determine which messages they receive.
type FilterConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// Expression is the filter expression string.
	// Special values:
	//   - "*": catch-all, receives all messages
	//   - "+": catch-the-rest, receives messages that matched no other filter
	//   - any other value: querylang expression matched against message attrs
	//     (e.g., "env=prod AND level=error")
	// Empty expression means the vault receives nothing.
	Expression string `json:"expression"`
}

// RotationPolicyConfig defines when chunks should be rotated.
// Multiple conditions can be specified; rotation occurs when ANY condition is met.
// All fields are optional (nil = not set).
type RotationPolicyConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// MaxBytes rotates when chunk size exceeds this value.
	// Supports suffixes: B, KB, MB, GB (e.g., "64MB", "1GB").
	MaxBytes *string `json:"maxBytes,omitempty"`

	// MaxAge rotates when chunk age exceeds this duration.
	// Uses Go duration format (e.g., "1h", "30m", "24h").
	MaxAge *string `json:"maxAge,omitempty"`

	// MaxRecords rotates when record count exceeds this value.
	MaxRecords *int64 `json:"maxRecords,omitempty"`

	// Cron rotates on a fixed schedule using cron syntax.
	// Supports standard 5-field (minute-level) or 6-field (second-level) expressions.
	// 5-field: "0 * * * *" (every hour at minute 0)
	// 6-field: "30 0 * * * *" (every hour at second 30 of minute 0)
	// This runs as a background job, independent of the per-append threshold checks.
	Cron *string `json:"cron,omitempty"`
}

// ValidateCron checks whether the Cron field contains a valid cron expression.
// Supports both 5-field (minute-level) and 6-field (second-level) syntax.
// Returns nil if Cron is nil or valid, an error otherwise.
func (c RotationPolicyConfig) ValidateCron() error {
	if c.Cron == nil || *c.Cron == "" {
		return nil
	}
	cr := gocron.NewDefaultCron(true)
	if err := cr.IsValid(*c.Cron, time.UTC, time.Now()); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}

// ToRotationPolicy converts a RotationPolicyConfig to a chunk.RotationPolicy.
// Returns nil if no conditions are specified.
func (c RotationPolicyConfig) ToRotationPolicy() (chunk.RotationPolicy, error) {
	var policies []chunk.RotationPolicy

	if c.MaxBytes != nil {
		bytes, err := ParseBytes(*c.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid maxBytes: %w", err)
		}
		policies = append(policies, chunk.NewSizePolicy(bytes))
	}

	if c.MaxAge != nil {
		d, err := time.ParseDuration(*c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge: %w", err)
		}
		if d <= 0 {
			return nil, errors.New("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewAgePolicy(d, nil))
	}

	if c.MaxRecords != nil {
		policies = append(policies, chunk.NewRecordCountPolicy(uint64(*c.MaxRecords))) //nolint:gosec // G115: maxRecords is a positive config value
	}

	if len(policies) == 0 {
		return nil, nil
	}

	if len(policies) == 1 {
		return policies[0], nil
	}

	return chunk.NewCompositePolicy(policies...), nil
}

// RetentionPolicyConfig defines when sealed chunks should be deleted.
// Multiple conditions can be specified; a chunk is deleted if ANY condition is met.
// All fields are optional (nil = not set).
type RetentionPolicyConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// MaxAge deletes sealed chunks older than this duration.
	// Uses Go duration format (e.g., "720h", "24h").
	MaxAge *string `json:"maxAge,omitempty"`

	// MaxBytes deletes oldest sealed chunks when total vault size exceeds this value.
	// Supports suffixes: B, KB, MB, GB (e.g., "10GB", "500MB").
	MaxBytes *string `json:"maxBytes,omitempty"`

	// MaxChunks keeps at most this many sealed chunks, deleting the oldest.
	MaxChunks *int64 `json:"maxChunks,omitempty"`
}

// ToRetentionPolicy converts a RetentionPolicyConfig to a chunk.RetentionPolicy.
// Returns nil if no conditions are specified.
func (c RetentionPolicyConfig) ToRetentionPolicy() (chunk.RetentionPolicy, error) {
	var policies []chunk.RetentionPolicy

	if c.MaxAge != nil {
		d, err := time.ParseDuration(*c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge: %w", err)
		}
		if d <= 0 {
			return nil, errors.New("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewTTLRetentionPolicy(d))
	}

	if c.MaxBytes != nil {
		bytes, err := ParseBytes(*c.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid maxBytes: %w", err)
		}
		policies = append(policies, chunk.NewSizeRetentionPolicy(int64(bytes))) //nolint:gosec // G115: parsed byte count is always reasonable
	}

	if c.MaxChunks != nil {
		if *c.MaxChunks <= 0 {
			return nil, errors.New("invalid maxChunks: must be positive")
		}
		policies = append(policies, chunk.NewCountRetentionPolicy(int(*c.MaxChunks)))
	}

	if len(policies) == 0 {
		return nil, nil
	}

	if len(policies) == 1 {
		return policies[0], nil
	}

	return chunk.NewCompositeRetentionPolicy(policies...), nil
}

// RetentionAction describes what happens when a retention policy matches chunks.
type RetentionAction string

const (
	// RetentionActionExpire deletes matching chunks (the default behavior).
	RetentionActionExpire RetentionAction = "expire"
	// RetentionActionEject streams matching chunks' records through named routes.
	RetentionActionEject RetentionAction = "eject"
	// RetentionActionTransition streams matching chunks' records to the next tier in the vault's chain.
	RetentionActionTransition RetentionAction = "transition"
)

// RetentionRule pairs a retention policy with an action.
type RetentionRule struct {
	RetentionPolicyID uuid.UUID       `json:"retentionPolicyId"`
	Action            RetentionAction `json:"action"`
	EjectRouteIDs     []uuid.UUID     `json:"ejectRouteIds,omitempty"` // target routes, only for eject
}

