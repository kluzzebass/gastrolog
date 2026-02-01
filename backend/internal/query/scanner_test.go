package query

import (
	"gastrolog/internal/chunk"
	"slices"
	"testing"
)

func TestPrunePositions(t *testing.T) {
	tests := []struct {
		name      string
		positions []uint64
		minPos    uint64
		want      []uint64
	}{
		{
			name:      "empty slice",
			positions: []uint64{},
			minPos:    0,
			want:      []uint64{},
		},
		{
			name:      "nil slice",
			positions: nil,
			minPos:    0,
			want:      nil,
		},
		{
			name:      "all positions above min",
			positions: []uint64{10, 20, 30},
			minPos:    5,
			want:      []uint64{10, 20, 30},
		},
		{
			name:      "some positions below min",
			positions: []uint64{10, 20, 30},
			minPos:    15,
			want:      []uint64{20, 30},
		},
		{
			name:      "all positions below min",
			positions: []uint64{10, 20, 30},
			minPos:    100,
			want:      []uint64{},
		},
		{
			name:      "min equals first position",
			positions: []uint64{10, 20, 30},
			minPos:    10,
			want:      []uint64{10, 20, 30},
		},
		{
			name:      "min equals last position",
			positions: []uint64{10, 20, 30},
			minPos:    30,
			want:      []uint64{30},
		},
		{
			name:      "single element above min",
			positions: []uint64{50},
			minPos:    10,
			want:      []uint64{50},
		},
		{
			name:      "single element below min",
			positions: []uint64{5},
			minPos:    10,
			want:      []uint64{},
		},
		{
			name:      "single element equals min",
			positions: []uint64{10},
			minPos:    10,
			want:      []uint64{10},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := prunePositions(tc.positions, tc.minPos)
			if !slices.Equal(got, tc.want) {
				t.Errorf("prunePositions(%v, %d) = %v, want %v", tc.positions, tc.minPos, got, tc.want)
			}
		})
	}
}

func TestIntersectPositions(t *testing.T) {
	tests := []struct {
		name string
		a, b []uint64
		want []uint64
	}{
		{
			name: "both empty",
			a:    []uint64{},
			b:    []uint64{},
			want: nil,
		},
		{
			name: "first empty",
			a:    []uint64{},
			b:    []uint64{1, 2, 3},
			want: nil,
		},
		{
			name: "second empty",
			a:    []uint64{1, 2, 3},
			b:    []uint64{},
			want: nil,
		},
		{
			name: "no overlap",
			a:    []uint64{1, 2, 3},
			b:    []uint64{4, 5, 6},
			want: nil,
		},
		{
			name: "complete overlap",
			a:    []uint64{1, 2, 3},
			b:    []uint64{1, 2, 3},
			want: []uint64{1, 2, 3},
		},
		{
			name: "partial overlap",
			a:    []uint64{1, 2, 3, 4},
			b:    []uint64{2, 3, 5, 6},
			want: []uint64{2, 3},
		},
		{
			name: "single common element",
			a:    []uint64{1, 5, 10},
			b:    []uint64{2, 5, 20},
			want: []uint64{5},
		},
		{
			name: "first subset of second",
			a:    []uint64{2, 3},
			b:    []uint64{1, 2, 3, 4, 5},
			want: []uint64{2, 3},
		},
		{
			name: "second subset of first",
			a:    []uint64{1, 2, 3, 4, 5},
			b:    []uint64{2, 3},
			want: []uint64{2, 3},
		},
		{
			name: "interleaved with some overlap",
			a:    []uint64{1, 3, 5, 7, 9},
			b:    []uint64{2, 3, 6, 7, 10},
			want: []uint64{3, 7},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := intersectPositions(tc.a, tc.b)
			if !slices.Equal(got, tc.want) {
				t.Errorf("intersectPositions(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestUnionPositions(t *testing.T) {
	tests := []struct {
		name string
		a, b []uint64
		want []uint64
	}{
		{
			name: "both empty",
			a:    []uint64{},
			b:    []uint64{},
			want: []uint64{},
		},
		{
			name: "first empty",
			a:    []uint64{},
			b:    []uint64{1, 2, 3},
			want: []uint64{1, 2, 3},
		},
		{
			name: "second empty",
			a:    []uint64{1, 2, 3},
			b:    []uint64{},
			want: []uint64{1, 2, 3},
		},
		{
			name: "no overlap",
			a:    []uint64{1, 2, 3},
			b:    []uint64{4, 5, 6},
			want: []uint64{1, 2, 3, 4, 5, 6},
		},
		{
			name: "complete overlap",
			a:    []uint64{1, 2, 3},
			b:    []uint64{1, 2, 3},
			want: []uint64{1, 2, 3},
		},
		{
			name: "partial overlap",
			a:    []uint64{1, 2, 3, 4},
			b:    []uint64{2, 3, 5, 6},
			want: []uint64{1, 2, 3, 4, 5, 6},
		},
		{
			name: "interleaved no overlap",
			a:    []uint64{1, 3, 5},
			b:    []uint64{2, 4, 6},
			want: []uint64{1, 2, 3, 4, 5, 6},
		},
		{
			name: "interleaved with overlap",
			a:    []uint64{1, 3, 5, 7},
			b:    []uint64{2, 3, 6, 7},
			want: []uint64{1, 2, 3, 5, 6, 7},
		},
		{
			name: "first subset of second",
			a:    []uint64{2, 3},
			b:    []uint64{1, 2, 3, 4, 5},
			want: []uint64{1, 2, 3, 4, 5},
		},
		{
			name: "second subset of first",
			a:    []uint64{1, 2, 3, 4, 5},
			b:    []uint64{2, 3},
			want: []uint64{1, 2, 3, 4, 5},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := unionPositions(tc.a, tc.b)
			if !slices.Equal(got, tc.want) {
				t.Errorf("unionPositions(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestScannerBuilderBasic(t *testing.T) {
	// Test that scannerBuilder correctly tracks state.
	chunkID := [16]byte{1, 2, 3}

	b := newScannerBuilder(chunkID)

	// Initially positions should be nil (sequential scan).
	if b.positions != nil {
		t.Error("expected nil positions initially")
	}

	// Add first positions.
	if !b.addPositions([]uint64{10, 20, 30}) {
		t.Error("expected addPositions to return true")
	}
	if !slices.Equal(b.positions, []uint64{10, 20, 30}) {
		t.Errorf("expected [10, 20, 30], got %v", b.positions)
	}

	// Add second positions (intersect).
	if !b.addPositions([]uint64{15, 20, 25, 30}) {
		t.Error("expected addPositions to return true")
	}
	if !slices.Equal(b.positions, []uint64{20, 30}) {
		t.Errorf("expected [20, 30], got %v", b.positions)
	}
}

func TestScannerBuilderEmptyIntersection(t *testing.T) {
	chunkID := [16]byte{1, 2, 3}
	b := newScannerBuilder(chunkID)

	b.addPositions([]uint64{10, 20, 30})
	// Add positions with no overlap.
	if b.addPositions([]uint64{100, 200, 300}) {
		t.Error("expected addPositions to return false for empty intersection")
	}
	if len(b.positions) != 0 {
		t.Errorf("expected empty positions, got %v", b.positions)
	}
}

func TestScannerBuilderMinPosition(t *testing.T) {
	chunkID := [16]byte{1, 2, 3}
	b := newScannerBuilder(chunkID)

	b.setMinPosition(25)
	b.addPositions([]uint64{10, 20, 30, 40})

	// Positions below 25 should be pruned.
	if !slices.Equal(b.positions, []uint64{30, 40}) {
		t.Errorf("expected [30, 40], got %v", b.positions)
	}
}

func TestScannerBuilderFilters(t *testing.T) {
	chunkID := [16]byte{1, 2, 3}
	b := newScannerBuilder(chunkID)

	filter1 := func(r chunk.Record) bool { return true }
	filter2 := func(r chunk.Record) bool { return false }

	b.addFilter(filter1)
	b.addFilter(filter2)

	if len(b.filters) != 2 {
		t.Errorf("expected 2 filters, got %d", len(b.filters))
	}
}
