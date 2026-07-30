package app

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietPeerTTLLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestPeerTTLMultiplier_DefaultWhenUnset verifies the documented fallback
// when GLOG_PEER_TTL_MULTIPLIER is unset.
//
// The multiplier scales the 5s NodeStats broadcast interval, so the default
// of 3 gives a 15s stats TTL — two consecutive missed broadcasts of slack.
// Jitter in that window does not flap peer liveness, which is derived from
// Raft last-contact instead.
func TestPeerTTLMultiplier_DefaultWhenUnset(t *testing.T) {
	// Not parallel — touches process env.
	t.Setenv("GLOG_PEER_TTL_MULTIPLIER", "")
	if got := peerTTLMultiplier(quietPeerTTLLogger()); got != time.Duration(defaultPeerTTLMultiplier) {
		t.Fatalf("default multiplier: got %d, want %d", got, defaultPeerTTLMultiplier)
	}
}

// TestPeerTTLMultiplier_EnvOverride verifies an operator on a
// jitter-prone network can dial the multiplier up via env.
func TestPeerTTLMultiplier_EnvOverride(t *testing.T) {
	t.Setenv("GLOG_PEER_TTL_MULTIPLIER", "12")
	if got := peerTTLMultiplier(quietPeerTTLLogger()); got != 12 {
		t.Fatalf("env override: got %d, want 12", got)
	}
}

// TestPeerTTLMultiplier_InvalidFallsBack verifies that a garbage
// env value falls back to the documented default rather than
// silently breaking peer detection at compile time.
func TestPeerTTLMultiplier_InvalidFallsBack(t *testing.T) {
	t.Setenv("GLOG_PEER_TTL_MULTIPLIER", "not-a-number")
	if got := peerTTLMultiplier(quietPeerTTLLogger()); got != time.Duration(defaultPeerTTLMultiplier) {
		t.Fatalf("invalid value: got %d, want %d", got, defaultPeerTTLMultiplier)
	}
}

// TestPeerTTLMultiplier_NonPositiveFallsBack verifies that 0 / -1
// don't collapse the TTL to 0s (which would treat every peer as
// instantly offline).
func TestPeerTTLMultiplier_NonPositiveFallsBack(t *testing.T) {
	for _, v := range []string{"0", "-1", "-100"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("GLOG_PEER_TTL_MULTIPLIER", v)
			if got := peerTTLMultiplier(quietPeerTTLLogger()); got != time.Duration(defaultPeerTTLMultiplier) {
				t.Fatalf("non-positive %q: got %d, want %d", v, got, defaultPeerTTLMultiplier)
			}
		})
	}
}
