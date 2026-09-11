package auth

// A login for a username that does not exist has no hash to verify against.
// Skipping the verification answers it sooner than a wrong password for a
// real user, and that difference tells an unauthenticated caller which
// usernames exist. The decoy is what makes the two cost the same.

import "testing"

func TestDecoyHashCostsWhatARealVerificationCosts(t *testing.T) {
	t.Parallel()
	encoded := decoyHash()
	if encoded == "" {
		t.Fatal("no decoy hash was produced, so an unknown user does no work at all")
	}

	_, _, memory, iterations, threads, keyLen, err := parsePHC(encoded)
	if err != nil {
		t.Fatalf("the decoy is not a usable hash: %v", err)
	}
	if memory != argonMemory || iterations != argonTime || threads != argonThreads || keyLen != argonKeyLen {
		t.Errorf("decoy parameters are m=%d t=%d p=%d len=%d, want m=%d t=%d p=%d len=%d — "+
			"verifying against it must cost what verifying a real password costs",
			memory, iterations, threads, keyLen, argonMemory, argonTime, argonThreads, argonKeyLen)
	}
}

func TestDecoyMatchesNoPassword(t *testing.T) {
	t.Parallel()
	for _, candidate := range []string{"", "password", "correct horse battery staple"} {
		ok, err := VerifyPassword(candidate, decoyHash())
		if err != nil {
			t.Fatalf("verify against decoy: %v", err)
		}
		if ok {
			t.Errorf("the decoy accepted %q", candidate)
		}
	}
}
