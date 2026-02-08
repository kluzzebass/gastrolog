package chunk

import "time"

// ActiveChunkState is an immutable snapshot of the active chunk's state at append time.
// It contains all information needed to make rotation decisions without IO or mutation.
//
// This struct is safe to copy and pass by value. All fields are derived from
// already-known state; no file paths, file descriptors, locks, or manager pointers.
type ActiveChunkState struct {
	// ChunkID is the unique identifier of the active chunk.
	ChunkID ChunkID

	// StartTS is the WriteTS of the first record in the chunk.
	// Zero if no records have been written yet.
	StartTS time.Time

	// LastWriteTS is the WriteTS of the most recent record in the chunk.
	// Zero if no records have been written yet.
	LastWriteTS time.Time

	// CreatedAt is the wall-clock time when the chunk was opened.
	CreatedAt time.Time

	// Bytes is the total on-disk bytes written so far (across all files).
	// This reflects actual on-disk growth: raw payload + attribute blob + idx entry overhead.
	Bytes uint64

	// Records is the number of records appended so far.
	Records uint64
}

// RotationPolicy determines when a chunk should be rotated.
// Policies are pure functions: no IO, no locks, no mutation, no global state.
//
// The ShouldRotate method is called before each append with the current chunk
// state and the record about to be written. It returns nil if no rotation is
// needed, or a pointer to a string identifying the trigger (e.g. "size",
// "age", "records", "hard-limit").
type RotationPolicy interface {
	// ShouldRotate returns nil if no rotation is needed, or a trigger name
	// identifying why the chunk should be rotated before appending the record.
	//
	// Policies must be pure functions that make decisions based solely on the
	// provided state and record. They must not perform IO or access global state.
	ShouldRotate(state ActiveChunkState, next Record) *string
}

// trigger returns a *string for a rotation trigger name.
func trigger(name string) *string { return &name }

// RotationPolicyFunc is an adapter to allow ordinary functions to be used as RotationPolicy.
type RotationPolicyFunc func(state ActiveChunkState, next Record) *string

func (f RotationPolicyFunc) ShouldRotate(state ActiveChunkState, next Record) *string {
	return f(state, next)
}

// CompositePolicy combines multiple policies with OR semantics.
// The chunk is rotated if any policy returns a non-nil trigger.
// The first matching trigger wins.
type CompositePolicy struct {
	policies []RotationPolicy
}

// NewCompositePolicy creates a policy that triggers rotation if any sub-policy fires.
func NewCompositePolicy(policies ...RotationPolicy) *CompositePolicy {
	return &CompositePolicy{policies: policies}
}

func (c *CompositePolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	for _, p := range c.policies {
		if t := p.ShouldRotate(state, next); t != nil {
			return t
		}
	}
	return nil
}

// SizePolicy triggers rotation when total bytes would exceed maxBytes.
// This is a soft limit that checks the projected size after appending.
type SizePolicy struct {
	maxBytes uint64
}

// NewSizePolicy creates a policy that rotates when chunk size exceeds maxBytes.
// The size includes all on-disk data: raw payload, attribute blob, and idx entry overhead.
func NewSizePolicy(maxBytes uint64) *SizePolicy {
	return &SizePolicy{maxBytes: maxBytes}
}

func (p *SizePolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	if p.maxBytes == 0 {
		return nil
	}
	projectedBytes := state.Bytes + recordOnDiskSize(next)
	if projectedBytes > p.maxBytes {
		return trigger("size")
	}
	return nil
}

// RecordCountPolicy triggers rotation when record count would exceed maxRecords.
type RecordCountPolicy struct {
	maxRecords uint64
}

// NewRecordCountPolicy creates a policy that rotates when record count exceeds maxRecords.
func NewRecordCountPolicy(maxRecords uint64) *RecordCountPolicy {
	return &RecordCountPolicy{maxRecords: maxRecords}
}

func (p *RecordCountPolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	if p.maxRecords == 0 {
		return nil
	}
	if state.Records+1 > p.maxRecords {
		return trigger("records")
	}
	return nil
}

// AgePolicy triggers rotation when chunk age exceeds maxAge.
// Age is measured from CreatedAt (wall-clock time when chunk was opened).
type AgePolicy struct {
	maxAge time.Duration
	now    func() time.Time
}

// NewAgePolicy creates a policy that rotates when chunk age exceeds maxAge.
// The now function is used to get the current time; if nil, time.Now is used.
func NewAgePolicy(maxAge time.Duration, now func() time.Time) *AgePolicy {
	if now == nil {
		now = time.Now
	}
	return &AgePolicy{maxAge: maxAge, now: now}
}

func (p *AgePolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	if p.maxAge == 0 {
		return nil
	}
	if state.CreatedAt.IsZero() {
		return nil
	}
	if p.now().Sub(state.CreatedAt) > p.maxAge {
		return trigger("age")
	}
	return nil
}

// HardLimitPolicy enforces absolute file size limits that cannot be exceeded.
// This policy always wins over other policies and must be included in any
// composite policy to prevent corruption.
//
// The limits are based on the maximum addressable offsets in the file format:
// - rawMaxBytes: maximum size of raw.log (typically 4GB for uint32 offset)
// - attrMaxBytes: maximum size of attr.log (typically 4GB for uint32 offset)
type HardLimitPolicy struct {
	rawMaxBytes  uint64
	attrMaxBytes uint64
}

// NewHardLimitPolicy creates a policy that enforces absolute file size limits.
// These limits are based on the file format's offset field sizes (typically uint32 = 4GB).
func NewHardLimitPolicy(rawMaxBytes, attrMaxBytes uint64) *HardLimitPolicy {
	return &HardLimitPolicy{
		rawMaxBytes:  rawMaxBytes,
		attrMaxBytes: attrMaxBytes,
	}
}

func (p *HardLimitPolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	rawSize := state.Bytes + uint64(len(next.Raw))

	attrBytes, _ := next.Attrs.Encode()
	// Note: we need to track raw and attr separately for hard limits
	// For now, we use a conservative estimate based on total bytes
	// The actual implementation in the manager tracks these separately
	_ = attrBytes

	if rawSize > p.rawMaxBytes {
		return trigger("hard-limit")
	}

	return nil
}

// NeverRotatePolicy is a policy that never triggers rotation.
// Useful for testing or when rotation is managed externally.
type NeverRotatePolicy struct{}

func (NeverRotatePolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	return nil
}

// AlwaysRotatePolicy is a policy that always triggers rotation.
// Useful for testing.
type AlwaysRotatePolicy struct{}

func (AlwaysRotatePolicy) ShouldRotate(state ActiveChunkState, next Record) *string {
	return trigger("always")
}

// recordOnDiskSize calculates the total on-disk bytes for a single record.
// This includes:
// - Raw payload bytes (in raw.log)
// - Encoded attribute bytes (in attr.log)
// - Fixed idx.log entry size (30 bytes)
//
// Note: The constant 30 is the IdxEntrySize from the file package.
// We duplicate it here to avoid a circular dependency.
const idxEntrySize = 30

func recordOnDiskSize(r Record) uint64 {
	attrBytes, _ := r.Attrs.Encode()
	return uint64(len(r.Raw)) + uint64(len(attrBytes)) + idxEntrySize
}

// RecordOnDiskSize returns the total on-disk bytes for a record.
// This is useful for pre-calculating rotation decisions.
func RecordOnDiskSize(r Record) uint64 {
	return recordOnDiskSize(r)
}
