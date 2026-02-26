package chunk

import "time"

// VaultState is an immutable snapshot of all sealed chunks in a vault.
// It contains all information needed to make retention decisions without IO.
type VaultState struct {
	// Chunks contains metadata for all sealed chunks, sorted by StartTS ascending (oldest first).
	Chunks []ChunkMeta

	// Now is the current wall-clock time.
	Now time.Time
}

// RetentionPolicy decides which sealed chunks should be deleted.
// Policies are pure functions: no IO, no locks, no mutation.
//
// Apply is called periodically by the retention goroutine with a snapshot
// of the vault's sealed chunks. It returns the IDs of chunks to delete.
type RetentionPolicy interface {
	Apply(state VaultState) []ChunkID
}

// RetentionPolicyFunc is an adapter to allow ordinary functions to be used as RetentionPolicy.
type RetentionPolicyFunc func(state VaultState) []ChunkID

func (f RetentionPolicyFunc) Apply(state VaultState) []ChunkID {
	return f(state)
}

// CompositeRetentionPolicy combines multiple policies with union semantics.
// A chunk is deleted if any sub-policy says it should be deleted.
type CompositeRetentionPolicy struct {
	policies []RetentionPolicy
}

// NewCompositeRetentionPolicy creates a policy that deletes a chunk if any sub-policy returns it.
func NewCompositeRetentionPolicy(policies ...RetentionPolicy) *CompositeRetentionPolicy {
	return &CompositeRetentionPolicy{policies: policies}
}

func (c *CompositeRetentionPolicy) Apply(state VaultState) []ChunkID {
	seen := make(map[ChunkID]struct{})
	var result []ChunkID

	for _, p := range c.policies {
		for _, id := range p.Apply(state) {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				result = append(result, id)
			}
		}
	}

	return result
}

// TTLRetentionPolicy deletes sealed chunks older than maxAge.
// Age is measured from the chunk's EndTS (last record written).
type TTLRetentionPolicy struct {
	maxAge time.Duration
}

// NewTTLRetentionPolicy creates a policy that deletes chunks older than maxAge.
func NewTTLRetentionPolicy(maxAge time.Duration) *TTLRetentionPolicy {
	return &TTLRetentionPolicy{maxAge: maxAge}
}

func (p *TTLRetentionPolicy) Apply(state VaultState) []ChunkID {
	if p.maxAge <= 0 {
		return nil
	}

	var result []ChunkID
	cutoff := state.Now.Add(-p.maxAge)

	for _, meta := range state.Chunks {
		if meta.EndTS.Before(cutoff) {
			result = append(result, meta.ID)
		}
	}

	return result
}

// SizeRetentionPolicy deletes the oldest sealed chunks when total vault size
// exceeds maxBytes. Keeps the newest chunks that fit within the budget.
type SizeRetentionPolicy struct {
	maxBytes int64
}

// NewSizeRetentionPolicy creates a policy that keeps total vault size under maxBytes.
func NewSizeRetentionPolicy(maxBytes int64) *SizeRetentionPolicy {
	return &SizeRetentionPolicy{maxBytes: maxBytes}
}

func (p *SizeRetentionPolicy) Apply(state VaultState) []ChunkID {
	if p.maxBytes <= 0 {
		return nil
	}

	// Sum from newest to oldest, mark everything beyond budget for deletion.
	var budget int64
	keep := make(map[ChunkID]struct{})

	// Walk backwards (newest first).
	for i := len(state.Chunks) - 1; i >= 0; i-- {
		meta := state.Chunks[i]
		if budget+meta.Bytes <= p.maxBytes {
			budget += meta.Bytes
			keep[meta.ID] = struct{}{}
		}
	}

	var result []ChunkID
	for _, meta := range state.Chunks {
		if _, ok := keep[meta.ID]; !ok {
			result = append(result, meta.ID)
		}
	}

	return result
}

// CountRetentionPolicy keeps at most maxChunks newest sealed chunks,
// deleting the rest.
type CountRetentionPolicy struct {
	maxChunks int
}

// NewCountRetentionPolicy creates a policy that keeps at most maxChunks sealed chunks.
func NewCountRetentionPolicy(maxChunks int) *CountRetentionPolicy {
	return &CountRetentionPolicy{maxChunks: maxChunks}
}

func (p *CountRetentionPolicy) Apply(state VaultState) []ChunkID {
	if p.maxChunks <= 0 || len(state.Chunks) <= p.maxChunks {
		return nil
	}

	// Chunks are sorted oldest first; delete the excess from the front.
	excess := len(state.Chunks) - p.maxChunks
	result := make([]ChunkID, excess)
	for i := range excess {
		result[i] = state.Chunks[i].ID
	}

	return result
}

// NeverRetainPolicy is a retention policy that never deletes anything.
type NeverRetainPolicy struct{}

func (NeverRetainPolicy) Apply(state VaultState) []ChunkID {
	return nil
}
