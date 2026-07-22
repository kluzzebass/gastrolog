package cli

import (
	"strings"
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

func TestStorageVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		st     *v1.StorageState
		expect string
	}{
		{
			name:   "healthy",
			st:     &v1.StorageState{},
			expect: "ok",
		},
		{
			// Warn-band verdicts (gastrolog-9akebz alarm pair: low/exhausted)
			// render as "warn" — server-computed, never re-derived here.
			name:   "warn band",
			st:     &v1.StorageState{WarnVerdict: true},
			expect: "warn",
		},
		{
			// Protect supersedes warn — the badge names the worse condition,
			// mirroring the alarm pair's low/exhausted split.
			name:   "protect supersedes warn",
			st:     &v1.StorageState{WarnVerdict: true, ProtectVerdict: true},
			expect: "protected",
		},
		{
			name:   "protect only",
			st:     &v1.StorageState{ProtectVerdict: true},
			expect: "protected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := storageVerdict(tt.st); got != tt.expect {
				t.Errorf("storageVerdict(%+v) = %q, want %q", tt.st, got, tt.expect)
			}
		})
	}
}

func TestThresholdLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		expr      string
		inherited bool
		bytes     uint64
		expect    string
	}{
		{
			// Operator directive (gastrolog-9akebz): render the wire — the
			// effective resolved value always leads, with the source named.
			name:      "inherited",
			expr:      "",
			inherited: true,
			bytes:     10 << 30,
			expect:    "10.0 GiB (inherited)",
		},
		{
			name:      "explicit percentage",
			expr:      "20%",
			inherited: false,
			bytes:     40 << 30,
			expect:    "40.0 GiB (20%)",
		},
		{
			name:      "explicit absolute size",
			expr:      "5GiB",
			inherited: false,
			bytes:     5 << 30,
			expect:    "5.0 GiB (5GiB)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := thresholdLabel(tt.expr, tt.inherited, tt.bytes); got != tt.expect {
				t.Errorf("thresholdLabel(%q, %v, %d) = %q, want %q", tt.expr, tt.inherited, tt.bytes, got, tt.expect)
			}
		})
	}
}

func TestRenderPlacedVaults(t *testing.T) {
	t.Parallel()

	// Empty list renders as the same "—" convention as
	// renderReplicaResidency, never a blank cell.
	if got := renderPlacedVaults(nil, nil); got != "—" {
		t.Errorf("renderPlacedVaults(nil) = %q, want %q", got, "—")
	}

	vaultA, vaultB := glid.New(), glid.New()
	names := map[string]string{vaultA.String(): "logs-prod"}

	got := renderPlacedVaults([][]byte{vaultA.ToProto(), vaultB.ToProto()}, names)
	// vaultA resolves to its name; vaultB (unknown to the lookup) falls
	// back to its raw ID — sorted for a stable join, same contract as
	// renderReplicaResidency.
	if !strings.Contains(got, "logs-prod") {
		t.Errorf("renderPlacedVaults must resolve a known vault to its name, got %q", got)
	}
	if !strings.Contains(got, vaultB.String()) {
		t.Errorf("renderPlacedVaults must fall back to the raw ID for an unknown vault, got %q", got)
	}
}
