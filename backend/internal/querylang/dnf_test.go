package querylang

import (
	"testing"
)

func TestToDNF(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantBranches int
		wantString   string
	}{
		{
			name:         "single token",
			input:        "error",
			wantBranches: 1,
			wantString:   "token(error)",
		},
		{
			name:         "two tokens AND",
			input:        "error AND warn",
			wantBranches: 1,
			wantString:   "(token(error) AND token(warn))",
		},
		{
			name:         "two tokens OR",
			input:        "error OR warn",
			wantBranches: 2,
			wantString:   "token(error) OR token(warn)",
		},
		{
			name:         "NOT token",
			input:        "NOT error",
			wantBranches: 1,
			wantString:   "NOT token(error)",
		},
		{
			name:         "token AND NOT token",
			input:        "error AND NOT debug",
			wantBranches: 1,
			wantString:   "(token(error) AND NOT token(debug))",
		},
		{
			name:         "OR with NOT - distributes",
			input:        "(error OR warn) AND NOT debug",
			wantBranches: 2,
			wantString:   "(token(error) AND NOT token(debug)) OR (token(warn) AND NOT token(debug))",
		},
		{
			name:         "complex AND OR",
			input:        "(a AND b) OR (c AND d)",
			wantBranches: 2,
			wantString:   "(token(a) AND token(b)) OR (token(c) AND token(d))",
		},
		{
			name:         "three way OR",
			input:        "a OR b OR c",
			wantBranches: 3,
			wantString:   "token(a) OR token(b) OR token(c)",
		},
		{
			name:         "double NOT",
			input:        "NOT NOT error",
			wantBranches: 1,
			wantString:   "token(error)",
		},
		{
			name:         "De Morgan AND",
			input:        "NOT (a AND b)",
			wantBranches: 2,
			wantString:   "NOT token(a) OR NOT token(b)",
		},
		{
			name:         "De Morgan OR",
			input:        "NOT (a OR b)",
			wantBranches: 1,
			wantString:   "(NOT token(a) AND NOT token(b))",
		},
		{
			name:         "key=value",
			input:        "level=error",
			wantBranches: 1,
			wantString:   "level=error",
		},
		{
			name:         "key=value OR key=value",
			input:        "level=error OR level=warn",
			wantBranches: 2,
			wantString:   "level=error OR level=warn",
		},
		{
			name:         "mixed predicates",
			input:        "error AND level=warn",
			wantBranches: 1,
			wantString:   "(token(error) AND level=warn)",
		},
		{
			name:         "complex with KV",
			input:        "env=dev AND NOT host=host-4",
			wantBranches: 1,
			wantString:   "(env=dev AND NOT host=host-4)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}

			dnf := ToDNF(expr)

			if len(dnf.Branches) != tt.wantBranches {
				t.Errorf("ToDNF(%q) branches = %d, want %d", tt.input, len(dnf.Branches), tt.wantBranches)
			}

			gotString := dnf.String()
			if gotString != tt.wantString {
				t.Errorf("ToDNF(%q).String() = %q, want %q", tt.input, gotString, tt.wantString)
			}
		})
	}
}

func TestConjunctionPredicates(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantPositive int
		wantNegative int
	}{
		{"single positive", "error", 1, 0},
		{"single negative", "NOT error", 0, 1},
		{"two positive", "error AND warn", 2, 0},
		{"one positive one negative", "error AND NOT debug", 1, 1},
		{"two negative", "NOT a AND NOT b", 0, 2},
		{"mixed", "a AND b AND NOT c AND NOT d", 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}

			dnf := ToDNF(expr)
			if len(dnf.Branches) != 1 {
				t.Fatalf("expected 1 branch, got %d", len(dnf.Branches))
			}

			branch := dnf.Branches[0]
			if len(branch.Positive) != tt.wantPositive {
				t.Errorf("Positive predicates = %d, want %d", len(branch.Positive), tt.wantPositive)
			}
			if len(branch.Negative) != tt.wantNegative {
				t.Errorf("Negative predicates = %d, want %d", len(branch.Negative), tt.wantNegative)
			}
		})
	}
}

func TestDNFDistribution(t *testing.T) {
	// (a OR b) AND (c OR d) should produce 4 branches:
	// (a AND c), (a AND d), (b AND c), (b AND d)
	expr, err := Parse("(a OR b) AND (c OR d)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	dnf := ToDNF(expr)
	if len(dnf.Branches) != 4 {
		t.Fatalf("expected 4 branches, got %d: %s", len(dnf.Branches), dnf.String())
	}

	// Each branch should have exactly 2 positive predicates
	for i, branch := range dnf.Branches {
		if len(branch.Positive) != 2 {
			t.Errorf("branch %d: expected 2 positive predicates, got %d", i, len(branch.Positive))
		}
		if len(branch.Negative) != 0 {
			t.Errorf("branch %d: expected 0 negative predicates, got %d", i, len(branch.Negative))
		}
	}
}

func TestDNFWithComplex(t *testing.T) {
	// (error OR warn) AND level=info AND NOT debug
	// Should produce 2 branches:
	// (error AND level=info AND NOT debug)
	// (warn AND level=info AND NOT debug)
	expr, err := Parse("(error OR warn) AND level=info AND NOT debug")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	dnf := ToDNF(expr)
	if len(dnf.Branches) != 2 {
		t.Fatalf("expected 2 branches, got %d: %s", len(dnf.Branches), dnf.String())
	}

	for i, branch := range dnf.Branches {
		if len(branch.Positive) != 2 {
			t.Errorf("branch %d: expected 2 positive predicates, got %d", i, len(branch.Positive))
		}
		if len(branch.Negative) != 1 {
			t.Errorf("branch %d: expected 1 negative predicate, got %d", i, len(branch.Negative))
		}
	}
}
