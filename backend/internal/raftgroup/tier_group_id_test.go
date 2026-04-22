package raftgroup

import (
	"testing"

	"gastrolog/internal/glid"
)

func TestTierMetadataGroupID_format(t *testing.T) {
	t.Parallel()
	v := glid.New()
	tier := glid.New()
	got := TierMetadataGroupID(v, tier)
	want := "vault/" + v.String() + "/tier/" + tier.String()
	if got != want {
		t.Fatalf("TierMetadataGroupID = %q, want %q", got, want)
	}
}
