package system

import (
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"time"

	"gastrolog/internal/chunk"

	"github.com/go-co-op/gocron/v2"
)

// ---------------------------------------------------------------------------

// gastrolog-4kkoo (Phase 5): FilterConfig type removed. Filters are now
// inlined as RouteConfig.Stages[].Match.Expression — no separate entity.
// The expression syntax (including the "*" catch-all and "+" catch-rest
// wildcards) lives on RouteConfig.MatchStage.Expression.

// RotationPolicyConfig defines when chunks should be rotated.
// Multiple conditions can be specified; rotation occurs when ANY condition is met.
// All fields are optional (nil = not set).
type RotationPolicyConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID glid.GLID `json:"id"`

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

// IsEmpty reports whether this rotation policy has no conditions set —
// all of MaxBytes, MaxAge, MaxRecords, and Cron are nil or empty. An
// empty policy is a no-op when assigned to a vault (chunks never rotate),
// which is almost certainly an operator mistake rather than an intent.
// PutRotationPolicy uses this check to reject empty configs at the
// admission boundary. See gastrolog-1rbuf.
func (c RotationPolicyConfig) IsEmpty() bool {
	if c.MaxBytes != nil && *c.MaxBytes != "" {
		return false
	}
	if c.MaxAge != nil && *c.MaxAge != "" {
		return false
	}
	if c.MaxRecords != nil {
		return false
	}
	if c.Cron != nil && *c.Cron != "" {
		return false
	}
	return true
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
		bytes, err := ParseSize(*c.MaxBytes)
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
	ID glid.GLID `json:"id"`

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

// IsEmpty reports whether this retention policy has no conditions set —
// all of MaxAge, MaxBytes, and MaxChunks are nil or empty. An empty
// retention policy is a no-op (chunks accumulate indefinitely), almost
// certainly an operator mistake. PutRetentionPolicy uses this check to
// reject empty configs at the admission boundary. See gastrolog-1rbuf.
func (c RetentionPolicyConfig) IsEmpty() bool {
	if c.MaxAge != nil && *c.MaxAge != "" {
		return false
	}
	if c.MaxBytes != nil && *c.MaxBytes != "" {
		return false
	}
	if c.MaxChunks != nil {
		return false
	}
	return true
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
		bytes, err := ParseSize(*c.MaxBytes)
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

// RetentionRule binds a retention policy to a vault. Phase 4 (gastrolog-42f9z)
// collapsed the prior expire/eject/transition/archive action enum: a fired
// retention event always streams the chunk's records through the routing
// engine with `source = retention-trigger(vault_id)` and always destroys
// the original chunk. The routing engine's verdict drives placement.
type RetentionRule struct {
	RetentionPolicyID glid.GLID `json:"retentionPolicyId"`
}
