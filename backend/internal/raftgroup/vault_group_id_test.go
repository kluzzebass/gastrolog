package raftgroup

import (
	"testing"

	"gastrolog/internal/glid"
)

func TestVaultControlPlaneGroupID_format(t *testing.T) {
	t.Parallel()
	v := glid.New()
	got := VaultControlPlaneGroupID(v)
	want := "vault/" + v.String() + "/ctl"
	if got != want {
		t.Fatalf("VaultControlPlaneGroupID = %q, want %q", got, want)
	}
}
