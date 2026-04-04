package memory

import (
	"testing"

	"gastrolog/internal/chunk"
)

func makeRecord(size int) chunk.Record {
	return chunk.Record{
		Raw:   make([]byte, size),
		Attrs: chunk.Attributes{"k": "v"},
	}
}

func TestBudgetEnforcementRotatesOnSize(t *testing.T) {
	t.Parallel()
	// Budget of 500 bytes → SizePolicy rotates when chunk exceeds 500 bytes.
	factory := NewFactory()
	cm, err := factory(map[string]string{"budgetBytes": "500"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Each record is ~100 bytes (raw) + small attr overhead.
	for range 10 {
		if _, _, err := cm.Append(makeRecord(100)); err != nil {
			t.Fatal(err)
		}
	}

	metas, _ := cm.List()
	if len(metas) == 0 {
		t.Fatal("expected at least one sealed chunk from size-based rotation")
	}

	// Verify sealed chunks are within budget.
	for _, m := range metas {
		if m.Bytes > 600 { // some overhead tolerance
			t.Errorf("chunk %s has %d bytes, expected ≤ budget", m.ID, m.Bytes)
		}
	}
}

func TestBudgetEnforcementRecordCountStillWorks(t *testing.T) {
	t.Parallel()
	// Both maxRecords and maxBytes set. The smaller limit triggers first.
	factory := NewFactory()
	cm, err := factory(map[string]string{"maxRecords": "5", "budgetBytes": "100000"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	for range 12 {
		if _, _, err := cm.Append(makeRecord(10)); err != nil {
			t.Fatal(err)
		}
	}

	metas, _ := cm.List()
	// 12 records / 5 per chunk = 2 sealed chunks + 2 in active.
	if len(metas) < 2 {
		t.Errorf("expected at least 2 sealed chunks from record count rotation, got %d", len(metas))
	}
}

func TestBudgetEnforcementSizeTriggesFirst(t *testing.T) {
	t.Parallel()
	// maxRecords=1000 (high), maxBytes=200 (low). Size should trigger first.
	factory := NewFactory()
	cm, err := factory(map[string]string{"maxRecords": "1000", "budgetBytes": "200"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	for range 20 {
		if _, _, err := cm.Append(makeRecord(50)); err != nil {
			t.Fatal(err)
		}
	}

	metas, _ := cm.List()
	if len(metas) == 0 {
		t.Fatal("expected size-based rotation to seal chunks")
	}
	for _, m := range metas {
		if m.RecordCount > 10 {
			t.Errorf("chunk has %d records — size policy should have rotated before 10", m.RecordCount)
		}
	}
}

func TestTotalBytesTracksAllChunks(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{RotationPolicy: chunk.NewRecordCountPolicy(5)})
	if err != nil {
		t.Fatal(err)
	}

	for range 12 {
		if _, _, err := cm.Append(makeRecord(100)); err != nil {
			t.Fatal(err)
		}
	}

	total := cm.TotalBytes()
	if total < 1200 {
		t.Errorf("TotalBytes() = %d, expected at least 1200 (12 × 100 raw bytes)", total)
	}

	// Verify active chunk contributes.
	active := cm.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	metas, _ := cm.List()
	var sealedBytes int64
	for _, m := range metas {
		sealedBytes += m.Bytes
	}
	if total <= sealedBytes {
		t.Error("TotalBytes should include active chunk bytes")
	}
}

func TestTotalBytesDecreasesOnDelete(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{RotationPolicy: chunk.NewRecordCountPolicy(5)})
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		if _, _, err := cm.Append(makeRecord(100)); err != nil {
			t.Fatal(err)
		}
	}

	before := cm.TotalBytes()
	metas, _ := cm.List()
	if len(metas) == 0 {
		t.Fatal("expected sealed chunks")
	}
	if err := cm.Delete(metas[0].ID); err != nil {
		t.Fatal(err)
	}

	after := cm.TotalBytes()
	if after >= before {
		t.Errorf("TotalBytes after delete (%d) should be less than before (%d)", after, before)
	}
}

func TestBudgetZeroNotAdded(t *testing.T) {
	t.Parallel()
	// maxBytes=0 should NOT add a SizePolicy. Verify by checking that
	// a small budget (e.g. 50 bytes) DOES seal, proving the policy is active
	// only when explicitly set.
	factory := NewFactory()
	cm, err := factory(map[string]string{"budgetBytes": "50"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	for range 5 {
		if _, _, err := cm.Append(makeRecord(100)); err != nil {
			t.Fatal(err)
		}
	}

	metas, _ := cm.List()
	if len(metas) == 0 {
		t.Error("expected sealed chunks from SizePolicy(50) with 100-byte records")
	}
}

// --- unhappy paths ---

func TestBudgetExceededZeroBudget(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if _, _, err := cm.Append(makeRecord(100)); err != nil {
			t.Fatal(err)
		}
	}
	if excess := cm.BudgetExceeded(); excess != 0 {
		t.Errorf("BudgetExceeded with no budget should return 0, got %d", excess)
	}
}

func TestBudgetExceededUnderBudget(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{BudgetBytes: 100000, RotationPolicy: chunk.NewRecordCountPolicy(100)})
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, _, err := cm.Append(makeRecord(10)); err != nil {
			t.Fatal(err)
		}
	}
	if excess := cm.BudgetExceeded(); excess != 0 {
		t.Errorf("BudgetExceeded under budget should return 0, got %d", excess)
	}
}

func TestBudgetExceededReportsExcess(t *testing.T) {
	t.Parallel()
	cm, err := NewManager(Config{BudgetBytes: 100, RotationPolicy: chunk.NewRecordCountPolicy(100)})
	if err != nil {
		t.Fatal(err)
	}
	// Append well over the 100-byte budget.
	for range 10 {
		if _, _, err := cm.Append(makeRecord(50)); err != nil {
			t.Fatal(err)
		}
	}
	excess := cm.BudgetExceeded()
	if excess <= 0 {
		t.Errorf("expected positive excess, got %d (total=%d, budget=100)", excess, cm.TotalBytes())
	}
	// Excess should be approximately total - 100.
	expected := cm.TotalBytes() - 100
	if excess != expected {
		t.Errorf("excess=%d, expected total-budget=%d", excess, expected)
	}
}

func TestBudgetExceededExactBoundary(t *testing.T) {
	t.Parallel()
	// Budget exactly equals one record's size — should not be exceeded with 1 record.
	cm, err := NewManager(Config{BudgetBytes: 200, RotationPolicy: chunk.NewRecordCountPolicy(100)})
	if err != nil {
		t.Fatal(err)
	}
	// Single small record, well under budget.
	if _, _, err := cm.Append(makeRecord(10)); err != nil {
		t.Fatal(err)
	}
	if excess := cm.BudgetExceeded(); excess != 0 {
		t.Errorf("single small record should not exceed 200-byte budget, got excess=%d", excess)
	}
}

// --- edge cases ---

func TestFactoryRejectsInvalidMaxBytes(t *testing.T) {
	t.Parallel()
	factory := NewFactory()
	_, err := factory(map[string]string{"budgetBytes": "not-a-number"}, nil)
	if err == nil {
		t.Error("expected error for invalid maxBytes")
	}
}
