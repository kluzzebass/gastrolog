package query

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
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
	chunkID := chunk.NewChunkID()

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
	chunkID := chunk.NewChunkID()
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
	chunkID := chunk.NewChunkID()
	b := newScannerBuilder(chunkID)

	b.setMinPosition(25)
	b.addPositions([]uint64{10, 20, 30, 40})

	// Positions below 25 should be pruned.
	if !slices.Equal(b.positions, []uint64{30, 40}) {
		t.Errorf("expected [30, 40], got %v", b.positions)
	}
}

func TestMatchesSingleToken(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		token string
		want  bool
	}{
		// Indexable tokens: standard tokenized matching
		{
			name:  "indexable token present",
			raw:   "error in service-auth module",
			token: "error",
			want:  true,
		},
		{
			name:  "indexable token absent",
			raw:   "info request completed",
			token: "error",
			want:  false,
		},
		{
			name:  "indexable token case insensitive",
			raw:   "ERROR in service",
			token: "error",
			want:  true,
		},
		// Non-indexable: IP addresses (contain dots, numeric segments)
		{
			name:  "IP address present",
			raw:   "request from 72.11.138.26 to server",
			token: "72.11.138.26",
			want:  true,
		},
		{
			name:  "IP address absent",
			raw:   "request from 10.0.0.1 to server",
			token: "72.11.138.26",
			want:  false,
		},
		{
			name:  "partial IP does not false-match",
			raw:   "request from 172.11.138.26 to server",
			token: "72.11.138.26",
			want:  true, // substring match — 72.11.138.26 is inside 172.11.138.26
		},
		// Non-indexable: dotted names (Java packages, DNS)
		{
			name:  "dotted package name present",
			raw:   "exception in com.example.auth.UserService",
			token: "com.example.auth.UserService",
			want:  true,
		},
		{
			name:  "dotted package name absent",
			raw:   "exception in org.apache.kafka.Producer",
			token: "com.example.auth.UserService",
			want:  false,
		},
		{
			name:  "dotted name case insensitive",
			raw:   "exception in COM.EXAMPLE.AUTH.UserService",
			token: "com.example.auth.userservice",
			want:  true,
		},
		// Non-indexable: pure numeric strings
		{
			name:  "pure number present",
			raw:   "exit code 42 from process",
			token: "42",
			want:  true,
		},
		{
			name:  "pure number absent",
			raw:   "exit code 0 from process",
			token: "42",
			want:  false,
		},
		// Non-indexable: single character
		{
			name:  "single char token",
			raw:   "level=E something happened",
			token: "E",
			want:  true,
		},
		// Non-indexable: too long (>16 chars of valid token bytes)
		{
			name:  "very long token present",
			raw:   "id=abcdefghijklmnopqrstuvwxyz in log",
			token: "abcdefghijklmnopqrstuvwxyz",
			want:  true,
		},
		// Non-indexable: contains special chars
		{
			name:  "token with colon",
			raw:   "listening on localhost:8080",
			token: "localhost:8080",
			want:  true,
		},
		{
			name:  "token with slash",
			raw:   "GET /api/v1/users HTTP/1.1",
			token: "/api/v1/users",
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := matchesSingleToken([]byte(tc.raw), tc.token)
			if got != tc.want {
				t.Errorf("matchesSingleToken(%q, %q) = %v, want %v", tc.raw, tc.token, got, tc.want)
			}
		})
	}
}

func TestMatchesTokens(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		tokens []string
		want   bool
	}{
		{
			name:   "empty tokens match everything",
			raw:    "anything here",
			tokens: nil,
			want:   true,
		},
		{
			name:   "mixed indexable and non-indexable all present",
			raw:    "error from 72.11.138.26 in service-auth",
			tokens: []string{"error", "72.11.138.26"},
			want:   true,
		},
		{
			name:   "indexable present but non-indexable absent",
			raw:    "error from 10.0.0.1 in service-auth",
			tokens: []string{"error", "72.11.138.26"},
			want:   false,
		},
		{
			name:   "non-indexable present but indexable absent",
			raw:    "info from 72.11.138.26 in service-auth",
			tokens: []string{"error", "72.11.138.26"},
			want:   false,
		},
		{
			name:   "multiple non-indexable tokens",
			raw:    "request from 72.11.138.26 to 10.0.0.1",
			tokens: []string{"72.11.138.26", "10.0.0.1"},
			want:   true,
		},
		{
			name:   "multiple non-indexable one missing",
			raw:    "request from 72.11.138.26 to server",
			tokens: []string{"72.11.138.26", "10.0.0.1"},
			want:   false,
		},
		{
			name:   "dotted name with indexable token",
			raw:    "exception in com.example.Controller error",
			tokens: []string{"exception", "com.example.Controller"},
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := matchesTokens([]byte(tc.raw), tc.tokens)
			if got != tc.want {
				t.Errorf("matchesTokens(%q, %v) = %v, want %v", tc.raw, tc.tokens, got, tc.want)
			}
		})
	}
}

func TestEvalPredicateExpr(t *testing.T) {
	tests := []struct {
		name  string
		query string
		raw   string
		attrs chunk.Attributes
		want  bool
	}{
		{
			name:  "len > threshold matches",
			query: "len(message) > 5",
			attrs: chunk.Attributes{"message": "hello world"},
			want:  true,
		},
		{
			name:  "len > threshold does not match",
			query: "len(message) > 100",
			attrs: chunk.Attributes{"message": "hello"},
			want:  false,
		},
		{
			name:  "len = exact match",
			query: "len(message) = 5",
			attrs: chunk.Attributes{"message": "hello"},
			want:  true,
		},
		{
			name:  "lower equality",
			query: "lower(level) = error",
			attrs: chunk.Attributes{"level": "ERROR"},
			want:  true,
		},
		{
			name:  "abs comparison",
			query: "abs(value) > 5",
			attrs: chunk.Attributes{"value": "-10"},
			want:  true,
		},
		{
			name:  "missing field returns false",
			query: "len(nonexistent) > 0",
			attrs: chunk.Attributes{},
			want:  false,
		},
		{
			name:  "len from raw text KV",
			query: "len(msg) > 3",
			raw:   "msg=hello",
			want:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := querylang.Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.query, err)
			}
			pred, ok := expr.(*querylang.PredicateExpr)
			if !ok {
				t.Fatalf("Parse(%q) = %T, want *PredicateExpr", tc.query, expr)
			}
			if pred.Kind != querylang.PredExpr {
				t.Fatalf("Parse(%q).Kind = %v, want PredExpr", tc.query, pred.Kind)
			}

			rec := chunk.Record{
				Attrs: tc.attrs,
				Raw:   []byte(tc.raw),
			}
			got := evalPredicate(pred, rec)
			if got != tc.want {
				t.Errorf("evalPredicate(%q, ...) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestConjunctionToFiltersExprPredicate(t *testing.T) {
	// Expression predicates should end up as runtime filters (no index acceleration).
	expr, err := querylang.Parse("len(message) > 5")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	dnf := querylang.ToDNF(expr)
	if len(dnf.Branches) != 1 {
		t.Fatalf("expected 1 branch, got %d", len(dnf.Branches))
	}

	tokens, kv, globs, negFilter := ConjunctionToFilters(&dnf.Branches[0])

	// No index-accelerated filters for expression predicates.
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens, got %d", len(tokens))
	}
	if len(kv) != 0 {
		t.Errorf("expected 0 kv filters, got %d", len(kv))
	}
	if len(globs) != 0 {
		t.Errorf("expected 0 globs, got %d", len(globs))
	}

	// Expression predicate should be in the negFilter (runtime filter).
	if negFilter == nil {
		t.Fatal("expected non-nil negFilter for expression predicate")
	}

	// Test that the filter works.
	recMatch := chunk.Record{Attrs: chunk.Attributes{"message": "hello world"}}
	recNoMatch := chunk.Record{Attrs: chunk.Attributes{"message": "hi"}}

	if !negFilter(recMatch) {
		t.Error("expected filter to match record with long message")
	}
	if negFilter(recNoMatch) {
		t.Error("expected filter to reject record with short message")
	}
}

func TestMatchTimestampFieldPrecision(t *testing.T) {
	// Go stores nanoseconds; JavaScript toISOString() sends milliseconds.
	// Equality must match despite differing precision.
	nanoVal := "2026-03-12T10:30:00.123456789Z" // from record (RFC3339Nano)
	milliFilter := "2026-03-12T10:30:00.123Z"   // from JS toISOString()

	f := KeyValueFilter{Key: "ingest_ts", Value: milliFilter, Op: querylang.OpEq}
	if !matchTimestampField(nanoVal, f) {
		t.Error("expected millisecond-precision filter to match nanosecond-precision record")
	}

	// Exact nano match should also work.
	f2 := KeyValueFilter{Key: "ingest_ts", Value: nanoVal, Op: querylang.OpEq}
	if !matchTimestampField(nanoVal, f2) {
		t.Error("expected nano-precision filter to match nano-precision record")
	}

	// Different millisecond should not match.
	f3 := KeyValueFilter{Key: "ingest_ts", Value: "2026-03-12T10:30:00.124Z", Op: querylang.OpEq}
	if matchTimestampField(nanoVal, f3) {
		t.Error("expected different millisecond to not match")
	}

	// Second-precision filter should match if same second.
	secVal := "2026-03-12T10:30:00Z"
	f4 := KeyValueFilter{Key: "ingest_ts", Value: secVal, Op: querylang.OpEq}
	if !matchTimestampField(nanoVal, f4) {
		t.Error("expected second-precision filter to match (same second)")
	}

	// Comparison operators use full precision.
	f5 := KeyValueFilter{Key: "ingest_ts", Value: milliFilter, Op: querylang.OpGt}
	if !matchTimestampField(nanoVal, f5) {
		t.Error("expected nano > milli (nano has sub-ms digits)")
	}
}

func TestScannerBuilderFilters(t *testing.T) {
	chunkID := chunk.NewChunkID()
	b := newScannerBuilder(chunkID)

	filter1 := func(r chunk.Record) bool { return true }
	filter2 := func(r chunk.Record) bool { return false }

	b.addFilter(filter1)
	b.addFilter(filter2)

	if len(b.filters) != 2 {
		t.Errorf("expected 2 filters, got %d", len(b.filters))
	}
}

// ─── Benchmarks ──────────────────────────────────────────────────────────────

// makePositions generates a sorted slice of n positions with the given step.
func makePositions(n int, start, step uint64) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = start + uint64(i)*step
	}
	return out
}

// BenchmarkIntersectPositions_Dense benchmarks intersection where ~50% of elements overlap.
func BenchmarkIntersectPositions_Dense(b *testing.B) {
	a := makePositions(10000, 0, 2)  // 0, 2, 4, ..., 19998
	bSlice := makePositions(10000, 0, 3) // 0, 3, 6, ..., 29997
	for b.Loop() {
		_ = intersectPositions(a, bSlice)
	}
}

// BenchmarkIntersectPositions_Sparse benchmarks intersection with minimal overlap.
func BenchmarkIntersectPositions_Sparse(b *testing.B) {
	a := makePositions(10000, 0, 2)      // even numbers
	bSlice := makePositions(10000, 1, 2) // odd numbers — zero overlap
	for b.Loop() {
		_ = intersectPositions(a, bSlice)
	}
}

// BenchmarkUnionPositions benchmarks union of two large position lists.
func BenchmarkUnionPositions(b *testing.B) {
	a := makePositions(10000, 0, 3) // 0, 3, 6, ...
	bSlice := makePositions(10000, 0, 5) // 0, 5, 10, ...
	for b.Loop() {
		_ = unionPositions(a, bSlice)
	}
}

// BenchmarkPrunePositions benchmarks binary-search-based position pruning.
func BenchmarkPrunePositions(b *testing.B) {
	positions := makePositions(10000, 0, 1)
	for b.Loop() {
		_ = prunePositions(positions, 5000)
	}
}

// BenchmarkMatchesTokens_Indexable benchmarks token matching for indexable tokens.
func BenchmarkMatchesTokens_Indexable(b *testing.B) {
	raw := []byte(`2024-01-15T10:22:15.123Z ERROR [auth-service] Authentication failed for user admin from host db-primary-01 timeout reached after retry`)
	tokens := []string{"error", "timeout", "retry"}
	for b.Loop() {
		_ = matchesTokens(raw, tokens)
	}
}

// BenchmarkMatchesTokens_NonIndexable benchmarks matching tokens that require substring fallback.
func BenchmarkMatchesTokens_NonIndexable(b *testing.B) {
	raw := []byte(`2024-01-15T10:22:15.123Z ERROR request from 192.168.1.100 to server.example.com failed`)
	tokens := []string{"192.168.1.100", "server.example.com"}
	for b.Loop() {
		_ = matchesTokens(raw, tokens)
	}
}

// BenchmarkMatchesSingleToken benchmarks the single-token fast path.
func BenchmarkMatchesSingleToken(b *testing.B) {
	raw := []byte(`level=ERROR msg="connection refused" host=db-primary-01 port=5432 retry=3`)
	for b.Loop() {
		_ = matchesSingleToken(raw, "error")
	}
}

// BenchmarkEvalPredicate benchmarks expression predicate evaluation.
func BenchmarkEvalPredicate(b *testing.B) {
	expr, err := querylang.Parse("len(message) > 5")
	if err != nil {
		b.Fatal(err)
	}
	pred := expr.(*querylang.PredicateExpr)
	rec := chunk.Record{Attrs: chunk.Attributes{"message": "hello world authentication failed"}}
	for b.Loop() {
		_ = evalPredicate(pred, rec)
	}
}
