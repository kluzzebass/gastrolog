package cli

import "testing"

// The seal summary distinguishes "sealed N chunks" from "nothing was open",
// so a zero never reads as a successful seal.
func TestSealSummary(t *testing.T) {
	t.Parallel()
	cases := []struct {
		sealed int32
		want   string
	}{
		{0, "Nothing to seal in vault prod: no open chunk holds records"},
		{1, "Sealed 1 open chunk(s) in vault prod"},
		{2, "Sealed 2 open chunk(s) in vault prod"},
	}
	for _, c := range cases {
		if got := sealSummary(c.sealed, "prod"); got != c.want {
			t.Errorf("sealSummary(%d) = %q, want %q", c.sealed, got, c.want)
		}
	}
}
