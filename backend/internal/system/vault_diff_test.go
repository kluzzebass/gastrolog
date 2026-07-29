package system

import (
	"slices"
	"testing"

	"gastrolog/internal/glid"
)

// A diff that says "something changed" is barely better than silence, so the
// field that changed must be named exactly (gastrolog-1jnfco).
func TestVaultConfigDiffNamesTheChangedFieldOnly(t *testing.T) {
	t.Parallel()
	base := VaultConfig{
		ID: glid.New(), Name: "v", Type: VaultTypeFile,
		RetentionDisposition: RetentionDispositionRoute,
	}

	cases := []struct {
		field  string
		mutate func(*VaultConfig)
	}{
		{"retentionDisposition", func(v *VaultConfig) { v.RetentionDisposition = RetentionDispositionDelete }},
		{"name", func(v *VaultConfig) { v.Name = "renamed" }},
		{"enabled", func(v *VaultConfig) { v.Enabled = true }},
		{"replicationFactor", func(v *VaultConfig) { v.ReplicationFactor = 3 }},
		{"storageClass", func(v *VaultConfig) { v.StorageClass = 2 }},
		{"cacheEviction", func(v *VaultConfig) { v.CacheEviction = "ttl" }},
	}
	for _, tc := range cases {
		next := base
		tc.mutate(&next)
		got := base.DiffFields(next)
		if len(got) != 1 || got[0] != tc.field {
			t.Errorf("changing %s reported %v, want exactly [%s]", tc.field, got, tc.field)
		}
	}
}

// The transfer target is a pointer; nil-vs-set and set-vs-different must both
// register, since retargeting a transfer redirects where records land.
func TestVaultConfigDiffDetectsTransferTargetChanges(t *testing.T) {
	t.Parallel()
	a, b := glid.New(), glid.New()
	base := VaultConfig{ID: glid.New(), Name: "v"}

	set := base
	set.RetentionTransferTargetVaultID = &a
	if got := base.DiffFields(set); !slices.Contains(got, "retentionTransferTargetVaultId") {
		t.Errorf("nil -> set reported %v", got)
	}

	retargeted := set
	retargeted.RetentionTransferTargetVaultID = &b
	if got := set.DiffFields(retargeted); !slices.Contains(got, "retentionTransferTargetVaultId") {
		t.Errorf("set -> different reported %v", got)
	}

	same := set
	if got := set.DiffFields(same); len(got) != 0 {
		t.Errorf("identical configs reported %v, want no changes", got)
	}
}

// Retention rules decide what retention does at all; a change to them must not
// be invisible just because the slice compares by reference elsewhere.
func TestVaultConfigDiffDetectsRetentionRuleChanges(t *testing.T) {
	t.Parallel()
	p1, p2 := glid.New(), glid.New()
	base := VaultConfig{ID: glid.New(), Name: "v", RetentionRules: []RetentionRule{{RetentionPolicyID: p1}}}

	swapped := base
	swapped.RetentionRules = []RetentionRule{{RetentionPolicyID: p2}}
	if got := swapped.DiffFields(base); !slices.Contains(got, "retentionRules") {
		t.Errorf("swapping the policy reported %v", got)
	}

	detached := base
	detached.RetentionRules = nil
	if got := base.DiffFields(detached); !slices.Contains(got, "retentionRules") {
		t.Errorf("detaching every rule reported %v", got)
	}

	identical := base
	identical.RetentionRules = []RetentionRule{{RetentionPolicyID: p1}}
	if got := base.DiffFields(identical); len(got) != 0 {
		t.Errorf("equal-by-value rules reported %v, want no changes", got)
	}
}
