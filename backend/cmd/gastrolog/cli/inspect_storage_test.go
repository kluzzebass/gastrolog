package cli

import (
	"strings"
	"testing"

	v1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sampled sets SampledAt on a StorageState fixture, so tests exercising
// warn/protect/ok can be distinguished from the "never sampled" case (a
// never-sampled storage must never render as "ok").
func sampled(st *v1.StorageState) *v1.StorageState {
	st.SampledAt = timestamppb.Now()
	return st
}

func TestStorageVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		st     *v1.StorageState
		expect string
	}{
		{
			// A storage the owning node hasn't statfs'd yet must never
			// render as "ok" — "no sample" is the honest fallback
			// (facts before speculation).
			name:   "never sampled",
			st:     &v1.StorageState{},
			expect: "no sample",
		},
		{
			name:   "healthy",
			st:     sampled(&v1.StorageState{}),
			expect: "ok",
		},
		{
			// Warn-band verdicts (the low/exhausted alarm pair) render as
			// "warn" — server-computed, never re-derived here.
			name:   "warn band",
			st:     sampled(&v1.StorageState{WarnVerdict: true}),
			expect: "warn",
		},
		{
			// Protect supersedes warn — the badge names the worse condition,
			// mirroring the alarm pair's low/exhausted split.
			name:   "protect supersedes warn",
			st:     sampled(&v1.StorageState{WarnVerdict: true, ProtectVerdict: true}),
			expect: "protected",
		},
		{
			name:   "protect only",
			st:     sampled(&v1.StorageState{ProtectVerdict: true}),
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

func TestFreeTotalLabel(t *testing.T) {
	t.Parallel()

	// Never sampled: "—"/"—", never "0 B"/"0 B" (would read as a full disk).
	free, total := freeTotalLabel(&v1.StorageState{})
	if free != "—" || total != "—" {
		t.Errorf("freeTotalLabel(never sampled) = (%q, %q), want (\"—\", \"—\")", free, total)
	}

	st := sampled(&v1.StorageState{FreeBytes: 5 << 30, TotalBytes: 100 << 30})
	free, total = freeTotalLabel(st)
	if free != "5.0 GiB" || total != "100.0 GiB" {
		t.Errorf("freeTotalLabel(sampled) = (%q, %q), want (\"5.0 GiB\", \"100.0 GiB\")", free, total)
	}
}

func TestThresholdLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		expr      string // EFFECTIVE expression — always non-empty on the real wire
		isDefault bool
		bytes     uint64
		expect    string
	}{
		{
			// There is no configurable node-level override to inherit
			// from — a defaulted threshold is labeled "default", never
			// "inherited", and the wire's effective expression ("10%")
			// still renders verbatim alongside it.
			name:      "default",
			expr:      "10%",
			isDefault: true,
			bytes:     10 << 30,
			expect:    "10.0 GiB (10%, default)",
		},
		{
			// A percentage carries information the byte count alone can't
			// (it rescales with the volume) — shown even when explicit.
			name:      "explicit percentage",
			expr:      "20%",
			isDefault: false,
			bytes:     40 << 30,
			expect:    "40.0 GiB (20%)",
		},
		{
			// An explicit absolute-size expression resolves to exactly the
			// shown byte count — appending "(5GiB)" next to "5.0 GiB" would
			// just repeat the same number in a second spelling, so the
			// terse form omits it.
			name:      "explicit absolute size",
			expr:      "5GiB",
			isDefault: false,
			bytes:     5 << 30,
			expect:    "5.0 GiB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := thresholdLabel(tt.expr, tt.isDefault, tt.bytes); got != tt.expect {
				t.Errorf("thresholdLabel(%q, %v, %d) = %q, want %q", tt.expr, tt.isDefault, tt.bytes, got, tt.expect)
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
