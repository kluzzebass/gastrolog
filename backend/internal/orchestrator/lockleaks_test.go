package orchestrator

import (
	"testing"
)

// TestLockTrackingDefaultOnAndEnvOff: o.mu tracking is on by default (so the
// next orphaned hold names its acquisition site) and GLOG_LOCK_TRACKING=off
// disables it.
func TestLockTrackingDefaultOnAndEnvOff(t *testing.T) {
	t.Setenv("GLOG_LOCK_TRACKING", "")
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if !o.mu.TrackingEnabled() {
		t.Fatal("lock tracking must default ON")
	}

	t.Setenv("GLOG_LOCK_TRACKING", "off")
	o2, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if o2.mu.TrackingEnabled() {
		t.Fatal("GLOG_LOCK_TRACKING=off must disable tracking")
	}
}
