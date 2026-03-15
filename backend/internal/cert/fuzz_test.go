package cert

import (
	"crypto/tls"
	"testing"
)

// FuzzAddFromPEM feeds random PEM-like data to the certificate manager.
// tls.X509KeyPair is expected to reject invalid input with an error;
// the manager must never panic.
func FuzzAddFromPEM(f *testing.F) {
	f.Add("", "")
	f.Add("not a cert", "not a key")
	f.Add("-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----",
		"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----")
	f.Add("-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----",
		"-----BEGIN EC PRIVATE KEY-----\nYWJj\n-----END EC PRIVATE KEY-----")

	m := New(Config{})

	f.Fuzz(func(t *testing.T, certPEM, keyPEM string) {
		// AddFromPEM returns an error for invalid certs; must not panic.
		_ = m.AddFromPEM("fuzz", certPEM, keyPEM)
	})
}

// FuzzX509KeyPairDirect fuzzes the stdlib tls.X509KeyPair through the
// same code path the Manager uses, ensuring no panics on arbitrary PEM input.
func FuzzX509KeyPairDirect(f *testing.F) {
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"),
		[]byte("-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----"))
	f.Add([]byte{0xff, 0xfe, 0xfd}, []byte{0xff, 0xfe, 0xfd})

	f.Fuzz(func(t *testing.T, certPEM, keyPEM []byte) {
		_, _ = tls.X509KeyPair(certPEM, keyPEM)
	})
}
