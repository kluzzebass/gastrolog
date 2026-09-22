package tlsutil_test

// A join token is minted when someone asks for one and dies shortly after, so
// what matters is that the cluster can tell its own tokens from anyone else's,
// that a holder cannot extend one, and that an expired one is refused.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/cluster/tlsutil"
)

func mintFixture(t *testing.T, ttl time.Duration) (key []byte, ca tlsutil.CAKeyPair, token string) {
	t.Helper()
	key, err := tlsutil.GenerateJoinTokenKey()
	if err != nil {
		t.Fatal(err)
	}
	ca, err = tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	token, err = tlsutil.MintJoinToken(key, ca.CertPEM, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return key, ca, token
}

func TestAMintedTokenIsAccepted(t *testing.T) {
	t.Parallel()
	key, ca, token := mintFixture(t, time.Hour)

	credential, caHash, err := tlsutil.ParseJoinToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if err := tlsutil.VerifyJoinToken(key, credential, time.Now()); err != nil {
		t.Fatalf("the cluster refused a token it minted: %v", err)
	}

	// The CA half is what a joiner pins the answering node against, so it has
	// to be the fingerprint of this cluster's CA and nothing else.
	block, _ := pem.Decode(ca.CertPEM)
	sum := sha256.Sum256(block.Bytes)
	if hex.EncodeToString(sum[:]) != caHash {
		t.Fatal("the token does not carry this cluster's CA fingerprint")
	}
}

// The window is the whole point: a token found in a terminal history or a
// config backup after it lapsed must not admit anyone.
func TestAnExpiredTokenIsRefused(t *testing.T) {
	t.Parallel()
	key, _, token := mintFixture(t, time.Hour)
	credential, _, err := tlsutil.ParseJoinToken(token)
	if err != nil {
		t.Fatal(err)
	}

	// Judged at a moment past the window rather than by waiting for one —
	// a test that sleeps for its subject is testing the clock.
	after := time.Now().Add(2 * time.Hour)
	err = tlsutil.VerifyJoinToken(key, credential, after)
	if !errors.Is(err, tlsutil.ErrJoinTokenExpired) {
		t.Fatalf("got %v, want ErrJoinTokenExpired", err)
	}
}

// Expiry is only meaningful if the holder cannot move it. The HMAC covers the
// expiry, so editing it invalidates the token rather than extending it.
func TestAnExtendedTokenIsRefused(t *testing.T) {
	t.Parallel()
	key, _, token := mintFixture(t, time.Hour)
	credential, _, err := tlsutil.ParseJoinToken(token)
	if err != nil {
		t.Fatal(err)
	}

	_, mac, _ := strings.Cut(credential, ".")
	farFuture := strconv.FormatInt(time.Now().Add(100*365*24*time.Hour).Unix(), 10)

	if err := tlsutil.VerifyJoinToken(key, farFuture+"."+mac, time.Now()); err == nil {
		t.Fatal("a token whose expiry was rewritten was accepted")
	}
}

// A token minted by a different cluster carries a valid-looking credential
// that this cluster's key did not sign.
func TestAnotherClustersTokenIsRefused(t *testing.T) {
	t.Parallel()
	ourKey, _, _ := mintFixture(t, time.Hour)
	_, _, theirToken := mintFixture(t, time.Hour)

	credential, _, err := tlsutil.ParseJoinToken(theirToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := tlsutil.VerifyJoinToken(ourKey, credential, time.Now()); err == nil {
		t.Fatal("a token from another cluster was accepted")
	}
	if errors.Is(err, tlsutil.ErrJoinTokenExpired) {
		t.Fatal("a forged token was reported as merely expired, which tells a caller its shape was otherwise right")
	}
}

func TestVerifyRejectsMalformedCredentials(t *testing.T) {
	t.Parallel()
	key, _, _ := mintFixture(t, time.Hour)

	for _, credential := range []string{"", "no-separator", "notanumber.abcd", "1." + strings.Repeat("z", 8)} {
		if err := tlsutil.VerifyJoinToken(key, credential, time.Now()); err == nil {
			t.Fatalf("accepted a malformed credential: %q", credential)
		}
	}
}

// Without a key there is nothing to verify against, and answering "fine"
// would admit anyone on a node whose cluster TLS never loaded.
func TestVerifyWithoutAKeyRefuses(t *testing.T) {
	t.Parallel()
	if err := tlsutil.VerifyJoinToken(nil, "1.abcd", time.Now()); err == nil {
		t.Fatal("verified a token with no key")
	}
	if _, err := tlsutil.MintJoinToken(nil, nil, time.Hour); err == nil {
		t.Fatal("minted a token with no key")
	}
}
