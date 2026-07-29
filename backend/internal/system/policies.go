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
// The expression syntax (including the "*" catch-all and the empty-string
// match-nothing form) lives on RouteConfig.MatchStage.Expression.

// RotationPolicyConfig defines when chunks should be rotated.
// Multiple conditions can be specified; rotation occurs when ANY condition is met.
// All fields are optional (nil = not set).
type RotationPolicyConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID glid.GLID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// MaxSize rotates when chunk size exceeds this size expression ("64MB").
	// Stored as the operator wrote it and resolved with SizeOrDefault at use
	// (gastrolog-etcjdx) — this replaces an earlier numeric-at-rest rule.
	MaxSize *string `json:"maxSize,omitempty"`

	// MaxAge rotates when chunk age exceeds this duration expression ("1m").
	// Resolved with DurationOrDefault at use; full precision is preserved
	// because the operator's own text is kept.
	MaxAge *string `json:"maxAge,omitempty"`

	// MaxRecords rotates when record count exceeds this value. Numeric: a
	// record count is unitless, so there is no expression to preserve.
	MaxRecords *int64 `json:"maxRecords,omitempty"`

	// Cron rotates on a fixed schedule using cron syntax.
	// Supports standard 5-field (minute-level) or 6-field (second-level) expressions.
	// 5-field: "0 * * * *" (every hour at minute 0)
	// 6-field: "30 0 * * * *" (every hour at second 30 of minute 0)
	// This runs as a background job, independent of the per-append threshold checks.
	Cron *string `json:"cron,omitempty"`
}

// IsEmpty reports whether this rotation policy has no conditions set —
// all of MaxBytes, MaxAgeNanos, MaxRecords, and Cron are nil or zero. An
// empty policy is a no-op when assigned to a vault (chunks never rotate),
// which is almost certainly an operator mistake rather than an intent.
// PutRotationPolicy uses this check to reject empty configs at the
// admission boundary. See gastrolog-1rbuf.
func (c RotationPolicyConfig) IsEmpty() bool {
	if positiveSize(c.MaxSize) || positiveDuration(c.MaxAge) {
		return false
	}
	if c.MaxRecords != nil && *c.MaxRecords > 0 {
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

	// Resolve the operator's expressions here, at use, through the shared
	// resolver — this is the only place a rotation policy's quantities become
	// numbers (gastrolog-etcjdx).
	if c.MaxSize != nil && !IsQuantityUnset(*c.MaxSize) {
		size, err := ParseSize(*c.MaxSize)
		if err != nil {
			return nil, fmt.Errorf("invalid maxSize %q: %w", *c.MaxSize, err)
		}
		if size > 0 {
			policies = append(policies, chunk.NewSizePolicy(size))
		}
	}

	if c.MaxAge != nil && !IsQuantityUnset(*c.MaxAge) {
		age, err := ParseDuration(*c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge %q: %w", *c.MaxAge, err)
		}
		if age <= 0 {
			return nil, errors.New("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewAgePolicy(age, nil))
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

	// MaxAge deletes sealed chunks older than this duration expression ("3m").
	// Resolved at use through the shared parser (gastrolog-etcjdx).
	MaxAge *string `json:"maxAge,omitempty"`

	// MaxSize is the vault's disk-claim bound, as a size expression
	// ("50GB"), and it can mean BOTH things at once (gastrolog-33ul6h
	// correction — an earlier design split this into two fields; that
	// design was superseded before implementation, see the design doc
	// history under docs/):
	//   - DRAIN: oldest sealed chunks past the bound are drained (per the
	//     vault's disposition), exactly as SizeRetentionPolicy always did.
	//     Unconditional — every set MaxSize drains, regardless of Refuse.
	//   - REFUSE: while the vault's local disk claim is at/over the bound,
	//     admission refuses cluster-wide — the backstop while drain catches
	//     up or is deferred. Gated by Refuse (gastrolog-5yfaqj, default
	//     off): a MaxSize policy with no explicit Refuse:true only drains,
	//     it does not refuse. Resolved at the config→runtime boundary by
	//     refreshVaultDiskGuards: min over every REFUSE-ELIGIBLE attached
	//     policy's parsed MaxSize; no bound at all when none states one
	//     (refuse-only — a default must never destroy data) only when no
	//     attached policy STATES a MaxSize at all — a stated-but-soft
	//     MaxSize does not re-engage the floor either; the operator's
	//     opt-out stands.
	MaxSize *string `json:"maxSize,omitempty"`

	// MaxChunks keeps at most this many sealed chunks, deleting the oldest.
	MaxChunks *int64 `json:"maxChunks,omitempty"`

	// Refuse generalizes MaxSize's refuse behavior to every bound this
	// policy states (gastrolog-5yfaqj). DEFAULT OFF (operator decision:
	// bounds are drain-first, refusal is the explicit hard mode — nil
	// reads as false; a policy must opt IN explicitly to become a "hard
	// bound"). true means: while any of MaxAge/MaxSize/MaxChunks is
	// violated, admission refuses. false (unset or explicit) is the
	// "soft bound" default: the policy still drains past its bounds, but
	// only the node-level disk guard backstops the vault while violated —
	// including a MaxSize policy with no explicit Refuse: it drains like
	// always, but no longer refuses unless the operator opts in.
	//
	// MaxSize's refuse check stays instantaneous (the disk guard's cap
	// machinery, unchanged). MaxAge/MaxChunks refuse only once the
	// retention runner has swept and failed to clear the violation — see
	// orchestrator/retention.go's post-sweep bound check — never on the
	// normal transient between a chunk's seal and the next sweep.
	//
	// Per-vault resolution is per bound KIND: min over every attached
	// policy that states it, with refuse-eligibility following the
	// STATING policy's own flag — a vault mixing a hard and a soft policy
	// refuses only on the hard one's bounds. When NO attached policy
	// opts in, there is no refuse bound at all for that kind. Since
	// gastrolog-vl2p98 there is no per-vault default to fall back to
	// either: a vault with no stated (or no refusing) size bound simply
	// has none, and the volume's free-space thresholds are the backstop.
	Refuse *bool `json:"refuse,omitempty"`
}

// RefuseEnabled reports whether this policy's stated bounds refuse
// admission while violated. Unset (nil) defaults to false (gastrolog-5yfaqj
// operator decision: bounds are drain-first) — a policy must opt IN to
// refusal explicitly, not out of it.
func (c RetentionPolicyConfig) RefuseEnabled() bool {
	return c.Refuse != nil && *c.Refuse
}

// IsEmpty reports whether this retention policy has no conditions set —
// MaxAge, MaxSize, and MaxChunks are all nil or unset. An empty retention
// policy is a no-op (chunks accumulate indefinitely, no bound backstops
// it), almost certainly an operator mistake. PutRetentionPolicy uses this
// check to reject empty configs at the admission boundary. See
// gastrolog-1rbuf.
func (c RetentionPolicyConfig) IsEmpty() bool {
	if positiveDuration(c.MaxAge) || positiveSize(c.MaxSize) {
		return false
	}
	if c.MaxChunks != nil && *c.MaxChunks > 0 {
		return false
	}
	return true
}

// positiveSize / positiveDuration report whether a size/duration expression
// pointer is set and resolves to a positive quantity. A nil, empty, "0", or
// unparseable value contributes nothing — the same "no-op condition" the
// numeric zero used to mean. The one parser, again (gastrolog-etcjdx).
func positiveSize(expr *string) bool {
	if expr == nil || IsQuantityUnset(*expr) {
		return false
	}
	n, err := ParseSize(*expr)
	return err == nil && n > 0
}

func positiveDuration(expr *string) bool {
	if expr == nil || IsQuantityUnset(*expr) {
		return false
	}
	d, err := ParseDuration(*expr)
	return err == nil && d > 0
}

// ToRetentionPolicy converts a RetentionPolicyConfig to a chunk.RetentionPolicy.
// Returns nil if no conditions are specified.
func (c RetentionPolicyConfig) ToRetentionPolicy() (chunk.RetentionPolicy, error) {
	var policies []chunk.RetentionPolicy

	// Resolve expressions at use, via the shared parser.
	if c.MaxAge != nil && !IsQuantityUnset(*c.MaxAge) {
		age, err := ParseDuration(*c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge %q: %w", *c.MaxAge, err)
		}
		if age <= 0 {
			return nil, errors.New("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewTTLRetentionPolicy(age))
	}

	if c.MaxSize != nil && !IsQuantityUnset(*c.MaxSize) {
		size, err := ParseSize(*c.MaxSize)
		if err != nil {
			return nil, fmt.Errorf("invalid maxSize %q: %w", *c.MaxSize, err)
		}
		if size > 0 {
			policies = append(policies, chunk.NewSizeRetentionPolicy(int64(size))) //nolint:gosec // G115: config byte count is always reasonable
		}
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
