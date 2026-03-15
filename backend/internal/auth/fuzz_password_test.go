package auth

import (
	"testing"
)

// FuzzVerifyPassword feeds random password and encoded-hash combinations
// to VerifyPassword. It must return (false, error) for malformed hashes
// and never panic on any input.
func FuzzVerifyPassword(f *testing.F) {
	// Seed: valid PHC-format hash produced by HashPassword("test").
	validHash, _ := HashPassword("test")
	f.Add("test", validHash)
	f.Add("", "")
	f.Add("password", "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Add("password", "not-a-hash")
	f.Add("password", "$bcrypt$something$else$here$now")
	f.Add("", "$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA")

	f.Fuzz(func(t *testing.T, password, encoded string) {
		// VerifyPassword may return true, false, or error.
		// Catch panics from the underlying argon2 library on degenerate inputs
		// (e.g. keyLen=0) and report them as test failures.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("VerifyPassword panicked: password=%q encoded=%q: %v", password, encoded, r)
			}
		}()
		_, _ = VerifyPassword(password, encoded)
	})
}

// FuzzParsePHC feeds random strings to the PHC format parser.
func FuzzParsePHC(f *testing.F) {
	f.Add("")
	f.Add("$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA")
	f.Add("$$$$$")
	f.Add("not a PHC string at all")

	f.Fuzz(func(t *testing.T, encoded string) {
		_, _, _, _, _, _, _ = parsePHC(encoded)
	})
}
