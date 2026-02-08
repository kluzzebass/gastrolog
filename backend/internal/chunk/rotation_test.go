package chunk

import (
	"testing"
	"time"
)

// =============================================================================
// ActiveChunkState Tests
// =============================================================================

func TestActiveChunkStateIsValueType(t *testing.T) {
	// Verify that ActiveChunkState can be copied by value
	state1 := ActiveChunkState{
		ChunkID:     NewChunkID(),
		StartTS:     time.Now(),
		LastWriteTS: time.Now(),
		CreatedAt:   time.Now(),
		Bytes:       1000,
		Records:     10,
	}

	state2 := state1 // Copy by value

	// Modify state1
	state1.Bytes = 2000
	state1.Records = 20

	// state2 should be unaffected
	if state2.Bytes != 1000 {
		t.Fatalf("state2.Bytes was modified: got %d, want 1000", state2.Bytes)
	}
	if state2.Records != 10 {
		t.Fatalf("state2.Records was modified: got %d, want 10", state2.Records)
	}
}

func TestActiveChunkStateZeroValue(t *testing.T) {
	var state ActiveChunkState

	// Zero value should be valid and represent an empty/new chunk
	if !state.StartTS.IsZero() {
		t.Fatal("zero StartTS should be zero time")
	}
	if !state.LastWriteTS.IsZero() {
		t.Fatal("zero LastWriteTS should be zero time")
	}
	if !state.CreatedAt.IsZero() {
		t.Fatal("zero CreatedAt should be zero time")
	}
	if state.Bytes != 0 {
		t.Fatalf("zero Bytes should be 0, got %d", state.Bytes)
	}
	if state.Records != 0 {
		t.Fatalf("zero Records should be 0, got %d", state.Records)
	}
}

// =============================================================================
// SizePolicy Tests
// =============================================================================

func TestSizePolicyBasic(t *testing.T) {
	policy := NewSizePolicy(1000) // 1000 bytes max

	testCases := []struct {
		name       string
		stateBytes uint64
		rawLen     int
		wantRotate bool
	}{
		{"empty_chunk_small_record", 0, 100, false},
		{"half_full_small_record", 500, 100, false},
		// 960 bytes + record (8 raw + 30 idx + 2 attrs) = 1000, exactly at limit
		{"exactly_at_limit", 960, 8, false},
		{"would_exceed_limit", 980, 50, true},
		{"already_at_limit", 1000, 1, true},
		{"large_record_on_empty", 0, 1001, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := ActiveChunkState{Bytes: tc.stateBytes}
			record := Record{Raw: make([]byte, tc.rawLen)}

			got := policy.ShouldRotate(state, record)
			if (got != nil) != tc.wantRotate {
				t.Fatalf("ShouldRotate() = %v, wantRotate %v", got, tc.wantRotate)
			}
		})
	}
}

func TestSizePolicyTriggerName(t *testing.T) {
	policy := NewSizePolicy(100)
	state := ActiveChunkState{Bytes: 200}
	record := Record{Raw: []byte("x")}

	got := policy.ShouldRotate(state, record)
	if got == nil || *got != "size" {
		t.Fatalf("expected trigger 'size', got %v", got)
	}
}

func TestSizePolicyZeroMaxBytes(t *testing.T) {
	policy := NewSizePolicy(0) // No limit

	state := ActiveChunkState{Bytes: 1000000000} // 1GB already written
	record := Record{Raw: make([]byte, 1000000)} // 1MB record

	if policy.ShouldRotate(state, record) != nil {
		t.Fatal("zero maxBytes should never trigger rotation")
	}
}

func TestSizePolicyIncludesOverhead(t *testing.T) {
	// The policy should account for total on-disk size, not just raw bytes
	policy := NewSizePolicy(100)

	state := ActiveChunkState{Bytes: 50}
	// Record with 20 bytes raw, but total on-disk is 20 + attrs + 30 (idx)
	record := Record{
		Raw:   make([]byte, 20),
		Attrs: Attributes{"key": "value"}, // ~14 bytes encoded
	}

	// Total: 50 (existing) + 20 (raw) + ~14 (attrs) + 30 (idx) = ~114 > 100
	if policy.ShouldRotate(state, record) == nil {
		t.Fatal("policy should account for overhead (attrs + idx entry)")
	}
}

// =============================================================================
// RecordCountPolicy Tests
// =============================================================================

func TestRecordCountPolicyBasic(t *testing.T) {
	policy := NewRecordCountPolicy(100)

	testCases := []struct {
		name       string
		records    uint64
		wantRotate bool
	}{
		{"empty_chunk", 0, false},
		{"half_full", 50, false},
		{"one_before_limit", 99, false},
		{"at_limit", 100, true},
		{"over_limit", 150, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := ActiveChunkState{Records: tc.records}
			record := Record{Raw: []byte("test")}

			got := policy.ShouldRotate(state, record)
			if (got != nil) != tc.wantRotate {
				t.Fatalf("ShouldRotate() = %v, wantRotate %v", got, tc.wantRotate)
			}
		})
	}
}

func TestRecordCountPolicyTriggerName(t *testing.T) {
	policy := NewRecordCountPolicy(10)
	state := ActiveChunkState{Records: 100}
	record := Record{Raw: []byte("x")}

	got := policy.ShouldRotate(state, record)
	if got == nil || *got != "records" {
		t.Fatalf("expected trigger 'records', got %v", got)
	}
}

func TestRecordCountPolicyZeroMax(t *testing.T) {
	policy := NewRecordCountPolicy(0)

	state := ActiveChunkState{Records: 1000000}
	record := Record{Raw: []byte("test")}

	if policy.ShouldRotate(state, record) != nil {
		t.Fatal("zero maxRecords should never trigger rotation")
	}
}

// =============================================================================
// AgePolicy Tests
// =============================================================================

func TestAgePolicyBasic(t *testing.T) {
	baseTime := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name       string
		maxAge     time.Duration
		createdAt  time.Time
		now        time.Time
		wantRotate bool
	}{
		{
			name:       "young_chunk",
			maxAge:     time.Hour,
			createdAt:  baseTime,
			now:        baseTime.Add(30 * time.Minute),
			wantRotate: false,
		},
		{
			name:       "exactly_at_limit",
			maxAge:     time.Hour,
			createdAt:  baseTime,
			now:        baseTime.Add(time.Hour),
			wantRotate: false, // Not over, exactly at
		},
		{
			name:       "over_limit",
			maxAge:     time.Hour,
			createdAt:  baseTime,
			now:        baseTime.Add(time.Hour + time.Second),
			wantRotate: true,
		},
		{
			name:       "way_over_limit",
			maxAge:     time.Hour,
			createdAt:  baseTime,
			now:        baseTime.Add(24 * time.Hour),
			wantRotate: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nowFunc := func() time.Time { return tc.now }
			policy := NewAgePolicy(tc.maxAge, nowFunc)

			state := ActiveChunkState{CreatedAt: tc.createdAt}
			record := Record{Raw: []byte("test")}

			got := policy.ShouldRotate(state, record)
			if (got != nil) != tc.wantRotate {
				t.Fatalf("ShouldRotate() = %v, wantRotate %v", got, tc.wantRotate)
			}
		})
	}
}

func TestAgePolicyTriggerName(t *testing.T) {
	now := time.Now()
	policy := NewAgePolicy(time.Hour, func() time.Time { return now })
	state := ActiveChunkState{CreatedAt: now.Add(-2 * time.Hour)}
	record := Record{Raw: []byte("x")}

	got := policy.ShouldRotate(state, record)
	if got == nil || *got != "age" {
		t.Fatalf("expected trigger 'age', got %v", got)
	}
}

func TestAgePolicyZeroMaxAge(t *testing.T) {
	nowFunc := func() time.Time { return time.Now() }
	policy := NewAgePolicy(0, nowFunc)

	state := ActiveChunkState{
		CreatedAt: time.Now().Add(-365 * 24 * time.Hour), // 1 year old
	}
	record := Record{Raw: []byte("test")}

	if policy.ShouldRotate(state, record) != nil {
		t.Fatal("zero maxAge should never trigger rotation")
	}
}

func TestAgePolicyZeroCreatedAt(t *testing.T) {
	nowFunc := func() time.Time { return time.Now() }
	policy := NewAgePolicy(time.Hour, nowFunc)

	state := ActiveChunkState{} // CreatedAt is zero
	record := Record{Raw: []byte("test")}

	if policy.ShouldRotate(state, record) != nil {
		t.Fatal("zero CreatedAt should not trigger rotation")
	}
}

func TestAgePolicyNilNowFunc(t *testing.T) {
	// Should default to time.Now
	policy := NewAgePolicy(time.Hour, nil)

	state := ActiveChunkState{
		CreatedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
	}
	record := Record{Raw: []byte("test")}

	if policy.ShouldRotate(state, record) == nil {
		t.Fatal("policy with nil now func should use time.Now")
	}
}

// =============================================================================
// HardLimitPolicy Tests
// =============================================================================

func TestHardLimitPolicyBasic(t *testing.T) {
	// 4GB limits (typical uint32 max)
	policy := NewHardLimitPolicy(1<<32-1, 1<<32-1)

	testCases := []struct {
		name       string
		stateBytes uint64
		rawLen     int
		wantRotate bool
	}{
		{"well_under_limit", 1000, 100, false},
		{"near_limit", 1<<32 - 1000, 100, false},
		{"would_exceed_limit", 1<<32 - 100, 200, true},
		{"at_limit", 1<<32 - 1, 1, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := ActiveChunkState{Bytes: tc.stateBytes}
			record := Record{Raw: make([]byte, tc.rawLen)}

			got := policy.ShouldRotate(state, record)
			if (got != nil) != tc.wantRotate {
				t.Fatalf("ShouldRotate() = %v, wantRotate %v", got, tc.wantRotate)
			}
		})
	}
}

func TestHardLimitPolicyTriggerName(t *testing.T) {
	policy := NewHardLimitPolicy(100, 100)
	state := ActiveChunkState{Bytes: 200}
	record := Record{Raw: []byte("x")}

	got := policy.ShouldRotate(state, record)
	if got == nil || *got != "hard-limit" {
		t.Fatalf("expected trigger 'hard-limit', got %v", got)
	}
}

// =============================================================================
// CompositePolicy Tests
// =============================================================================

func TestCompositePolicyORSemantics(t *testing.T) {
	sizePolicy := NewSizePolicy(1000)
	countPolicy := NewRecordCountPolicy(10)

	composite := NewCompositePolicy(sizePolicy, countPolicy)

	testCases := []struct {
		name       string
		bytes      uint64
		records    uint64
		wantRotate bool
	}{
		{"neither_triggers", 100, 5, false},
		{"size_triggers", 2000, 5, true},
		{"count_triggers", 100, 15, true},
		{"both_trigger", 2000, 15, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := ActiveChunkState{
				Bytes:   tc.bytes,
				Records: tc.records,
			}
			record := Record{Raw: []byte("test")}

			got := composite.ShouldRotate(state, record)
			if (got != nil) != tc.wantRotate {
				t.Fatalf("ShouldRotate() = %v, wantRotate %v", got, tc.wantRotate)
			}
		})
	}
}

func TestCompositePolicyFirstTriggerWins(t *testing.T) {
	// Both would fire, but size is checked first
	composite := NewCompositePolicy(
		NewSizePolicy(100),
		NewRecordCountPolicy(5),
	)

	state := ActiveChunkState{Bytes: 200, Records: 10}
	record := Record{Raw: []byte("x")}

	got := composite.ShouldRotate(state, record)
	if got == nil || *got != "size" {
		t.Fatalf("expected first trigger 'size', got %v", got)
	}
}

func TestCompositePolicyEmpty(t *testing.T) {
	composite := NewCompositePolicy() // No policies

	state := ActiveChunkState{Bytes: 1000000, Records: 1000000}
	record := Record{Raw: make([]byte, 1000000)}

	if composite.ShouldRotate(state, record) != nil {
		t.Fatal("empty composite should never trigger rotation")
	}
}

func TestCompositePolicyShortCircuits(t *testing.T) {
	callCount := 0

	policy1 := RotationPolicyFunc(func(state ActiveChunkState, next Record) *string {
		callCount++
		return trigger("test")
	})

	policy2 := RotationPolicyFunc(func(state ActiveChunkState, next Record) *string {
		callCount++
		return nil
	})

	composite := NewCompositePolicy(policy1, policy2)

	state := ActiveChunkState{}
	record := Record{}

	composite.ShouldRotate(state, record)

	if callCount != 1 {
		t.Fatalf("expected short-circuit after first trigger, got %d calls", callCount)
	}
}

// =============================================================================
// NeverRotatePolicy Tests
// =============================================================================

func TestNeverRotatePolicy(t *testing.T) {
	policy := NeverRotatePolicy{}

	// Should never rotate regardless of state
	testCases := []struct {
		name    string
		bytes   uint64
		records uint64
	}{
		{"empty", 0, 0},
		{"small", 100, 10},
		{"huge", 1 << 40, 1 << 30},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := ActiveChunkState{
				Bytes:   tc.bytes,
				Records: tc.records,
			}
			record := Record{Raw: make([]byte, 1000000)}

			if policy.ShouldRotate(state, record) != nil {
				t.Fatal("NeverRotatePolicy should never return a trigger")
			}
		})
	}
}

// =============================================================================
// AlwaysRotatePolicy Tests
// =============================================================================

func TestAlwaysRotatePolicy(t *testing.T) {
	policy := AlwaysRotatePolicy{}

	// Should always rotate regardless of state
	testCases := []struct {
		name    string
		bytes   uint64
		records uint64
	}{
		{"empty", 0, 0},
		{"small", 100, 10},
		{"huge", 1 << 40, 1 << 30},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := ActiveChunkState{
				Bytes:   tc.bytes,
				Records: tc.records,
			}
			record := Record{}

			got := policy.ShouldRotate(state, record)
			if got == nil {
				t.Fatal("AlwaysRotatePolicy should always return a trigger")
			}
			if *got != "always" {
				t.Fatalf("expected trigger 'always', got %q", *got)
			}
		})
	}
}

// =============================================================================
// RotationPolicyFunc Tests
// =============================================================================

func TestRotationPolicyFunc(t *testing.T) {
	// Test that functions can be used as policies
	called := false
	var capturedState ActiveChunkState
	var capturedRecord Record

	fn := RotationPolicyFunc(func(state ActiveChunkState, next Record) *string {
		called = true
		capturedState = state
		capturedRecord = next
		if state.Bytes > 1000 {
			return trigger("custom")
		}
		return nil
	})

	state := ActiveChunkState{Bytes: 500}
	record := Record{Raw: []byte("test")}

	result := fn.ShouldRotate(state, record)

	if !called {
		t.Fatal("function was not called")
	}
	if capturedState.Bytes != 500 {
		t.Fatalf("state not captured correctly: got %d", capturedState.Bytes)
	}
	if string(capturedRecord.Raw) != "test" {
		t.Fatal("record not captured correctly")
	}
	if result != nil {
		t.Fatal("expected nil for bytes=500")
	}

	// Test returning a trigger
	state.Bytes = 1500
	result = fn.ShouldRotate(state, record)
	if result == nil || *result != "custom" {
		t.Fatalf("expected trigger 'custom' for bytes=1500, got %v", result)
	}
}

// =============================================================================
// RecordOnDiskSize Tests
// =============================================================================

func TestRecordOnDiskSize(t *testing.T) {
	testCases := []struct {
		name     string
		rawLen   int
		attrs    Attributes
		minBytes uint64 // At least this many bytes
	}{
		{
			name:     "empty_record",
			rawLen:   0,
			attrs:    nil,
			minBytes: idxEntrySize + 2, // 30 (idx) + 2 (empty attrs encoding)
		},
		{
			name:     "raw_only",
			rawLen:   100,
			attrs:    nil,
			minBytes: 100 + idxEntrySize + 2,
		},
		{
			name:     "with_attrs",
			rawLen:   100,
			attrs:    Attributes{"key": "value"},
			minBytes: 100 + idxEntrySize + 10, // attrs add overhead
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			record := Record{
				Raw:   make([]byte, tc.rawLen),
				Attrs: tc.attrs,
			}

			size := RecordOnDiskSize(record)
			if size < tc.minBytes {
				t.Fatalf("size %d is less than minimum %d", size, tc.minBytes)
			}
		})
	}
}

func TestRecordOnDiskSizeConsistency(t *testing.T) {
	// Same record should always produce same size
	record := Record{
		Raw:   []byte("test data"),
		Attrs: Attributes{"host": "server-001", "env": "prod"},
	}

	size1 := RecordOnDiskSize(record)
	size2 := RecordOnDiskSize(record)

	if size1 != size2 {
		t.Fatalf("inconsistent sizes: %d vs %d", size1, size2)
	}
}

// =============================================================================
// Policy Purity Tests
// =============================================================================

func TestPoliciesArePure(t *testing.T) {
	// Policies should not modify their inputs
	state := ActiveChunkState{
		ChunkID:     NewChunkID(),
		StartTS:     time.Now(),
		LastWriteTS: time.Now(),
		CreatedAt:   time.Now(),
		Bytes:       1000,
		Records:     10,
	}

	record := Record{
		Raw:   []byte("test"),
		Attrs: Attributes{"key": "value"},
	}

	originalBytes := state.Bytes
	originalRecords := state.Records
	originalRaw := string(record.Raw)

	policies := []RotationPolicy{
		NewSizePolicy(500),
		NewRecordCountPolicy(5),
		NewAgePolicy(time.Hour, nil),
		NewHardLimitPolicy(1<<32-1, 1<<32-1),
		NewCompositePolicy(NewSizePolicy(500), NewRecordCountPolicy(5)),
		NeverRotatePolicy{},
		AlwaysRotatePolicy{},
	}

	for _, policy := range policies {
		policy.ShouldRotate(state, record)

		if state.Bytes != originalBytes {
			t.Fatal("policy modified state.Bytes")
		}
		if state.Records != originalRecords {
			t.Fatal("policy modified state.Records")
		}
		if string(record.Raw) != originalRaw {
			t.Fatal("policy modified record.Raw")
		}
	}
}

// =============================================================================
// Integration-Style Tests (Still Pure, No IO)
// =============================================================================

func TestTypicalProductionPolicy(t *testing.T) {
	// A typical production setup: size limit, age limit, and hard limits
	policy := NewCompositePolicy(
		NewHardLimitPolicy(1<<32-1, 1<<32-1), // 4GB hard limits
		NewSizePolicy(1<<30),                 // 1GB soft limit
		NewAgePolicy(24*time.Hour, nil),      // 24 hour age limit
	)

	// Fresh chunk with small record - should not rotate
	state := ActiveChunkState{
		CreatedAt: time.Now(),
		Bytes:     0,
		Records:   0,
	}
	record := Record{Raw: make([]byte, 1000)}

	if policy.ShouldRotate(state, record) != nil {
		t.Fatal("fresh chunk with small record should not rotate")
	}

	// Chunk near size limit - should rotate
	state.Bytes = 1<<30 - 100
	if policy.ShouldRotate(state, record) == nil {
		t.Fatal("chunk near size limit should rotate")
	}
}

func TestHardLimitAlwaysWins(t *testing.T) {
	// Even if soft limits say no, hard limit says yes
	neverPolicy := NeverRotatePolicy{}
	hardLimit := NewHardLimitPolicy(1000, 1000)

	composite := NewCompositePolicy(neverPolicy, hardLimit)

	state := ActiveChunkState{Bytes: 900}
	record := Record{Raw: make([]byte, 200)} // Would push over 1000

	if composite.ShouldRotate(state, record) == nil {
		t.Fatal("hard limit should override never-rotate policy")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkSizePolicyShouldRotate(b *testing.B) {
	policy := NewSizePolicy(1 << 30)
	state := ActiveChunkState{Bytes: 1000}
	record := Record{Raw: make([]byte, 100)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		policy.ShouldRotate(state, record)
	}
}

func BenchmarkCompositePolicyShouldRotate(b *testing.B) {
	policy := NewCompositePolicy(
		NewHardLimitPolicy(1<<32-1, 1<<32-1),
		NewSizePolicy(1<<30),
		NewRecordCountPolicy(1000000),
		NewAgePolicy(24*time.Hour, time.Now),
	)
	state := ActiveChunkState{
		Bytes:     1000,
		Records:   100,
		CreatedAt: time.Now(),
	}
	record := Record{Raw: make([]byte, 100)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		policy.ShouldRotate(state, record)
	}
}

func BenchmarkRecordOnDiskSize(b *testing.B) {
	record := Record{
		Raw:   make([]byte, 200),
		Attrs: Attributes{"host": "server-001", "env": "production"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RecordOnDiskSize(record)
	}
}
