package alert

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestRaiseStampsCatalogFields(t *testing.T) {
	// vault-leaderless carries a catalog DelayOn; drive the injected clock
	// past it so the stamped fields are observable through Standing().
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	c.Raise("vault-leaderless", "vault-1", "Vault a has had no placement leader for 90s.")
	clk.Advance(leaderlessWindow(t) + time.Second)

	alarms := c.Standing()
	if len(alarms) != 1 {
		t.Fatalf("got %d alarms, want 1", len(alarms))
	}
	a := alarms[0]
	if a.ID != "vault-leaderless:vault-1" {
		t.Errorf("ID = %q, want vault-leaderless:vault-1", a.ID)
	}
	if a.TypeID != "vault-leaderless" || a.InstanceKey != "vault-1" {
		t.Errorf("type/instance = %q/%q", a.TypeID, a.InstanceKey)
	}
	if a.Priority != High {
		t.Errorf("priority = %v, want High — priority must come from the catalog, not the caller", a.Priority)
	}
	if a.Source != "placement" {
		t.Errorf("source = %q, want placement", a.Source)
	}
	if a.Cause == "" || a.Response == "" {
		t.Error("cause/response must be stamped from the catalog")
	}
	if a.Detail != "Vault a has had no placement leader for 90s." {
		t.Errorf("detail = %q", a.Detail)
	}
	if a.SoftwareFault {
		t.Error("vault-leaderless is a process alarm, not a software fault")
	}
}

func TestRaiseEmptyInstanceKeyUsesBareTypeID(t *testing.T) {
	clk := newSuppressionClock()
	c := NewWithClock(clk.Now)

	c.Raise("node-disk-space-exhausted", "", "12 GiB free of 400 GiB")

	alarms := c.Standing()
	if len(alarms) != 1 {
		t.Fatalf("got %d alarms, want 1", len(alarms))
	}
	if alarms[0].ID != "node-disk-space-exhausted" {
		t.Errorf("ID = %q, want bare type ID for node-scoped alarms", alarms[0].ID)
	}
	if alarms[0].Priority != High {
		t.Errorf("priority = %v, want High (node-disk-space-exhausted catalog row)", alarms[0].Priority)
	}
	// A per-instance type raised with an empty key still surfaces (bare ID)
	// rather than being dropped — once its catalog delay-on elapses.
	c.Raise("vault-leaderless", "", "no instance key")
	clk.Advance(leaderlessWindow(t) + time.Second)
	if a := findAlarm(c, "vault-leaderless"); a == nil {
		t.Error("empty instance key on an instance-scoped type must still surface")
	}
}

func TestRaiseUnknownTypeIsLoudNotSilent(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer slog.SetDefault(prev)

	c := New()
	c.Raise("no-such-alarm-type", "instance-9", "the condition detail")

	// Loud in the log…
	if !strings.Contains(logBuf.String(), "no-such-alarm-type") {
		t.Errorf("unregistered raise must log the type ID; log was: %s", logBuf.String())
	}
	// …and still surfaced, as a software fault, with the raiser's detail.
	alarms := c.Standing()
	if len(alarms) != 1 {
		t.Fatalf("unregistered raise must surface an alarm, got %d", len(alarms))
	}
	a := alarms[0]
	if a.ID != "no-such-alarm-type:instance-9" {
		t.Errorf("ID = %q", a.ID)
	}
	if !a.SoftwareFault {
		t.Error("an unregistered type is a software defect and must be marked SoftwareFault")
	}
	if a.Detail != "the condition detail" {
		t.Errorf("detail = %q — the underlying condition must not be lost", a.Detail)
	}
	if a.Cause == "" || a.Response == "" {
		t.Error("even the unregistered fallback carries cause/response")
	}
}

func TestRaisePreservesFirstSeen(t *testing.T) {
	c := New()

	c.Raise("cloud-store", "v1", "first detail")
	first := c.Standing()[0].FirstSeen

	c.Raise("cloud-store", "v1", "updated detail")
	alarms := c.Standing()

	if len(alarms) != 1 {
		t.Fatalf("got %d alarms, want 1 (same type+instance dedups)", len(alarms))
	}
	if alarms[0].FirstSeen != first {
		t.Error("FirstSeen changed on re-raise")
	}
	if alarms[0].Detail != "updated detail" {
		t.Errorf("detail = %q, want 'updated detail'", alarms[0].Detail)
	}
	if alarms[0].LastSeen.Before(first) {
		t.Error("LastSeen must be >= FirstSeen")
	}
}

// TestRetentionRateIsCataloged pins the gastrolog-1cruar fold: retention-rate
// is an ordinary catalog row (a Low process condition), raised through the
// one Raise path like every other type — there is no operator-defined
// category and no priority chosen at a call site.
func TestRetentionRateIsCataloged(t *testing.T) {
	typ, ok := TypeByID("retention-rate")
	if !ok {
		t.Fatal("retention-rate missing from the catalog")
	}
	if typ.Priority != Low || typ.Source != "retention" {
		t.Errorf("retention-rate catalog row = %+v, want Low priority from source retention", typ)
	}

	c := New()
	c.Raise("retention-rate", "vault-2", "rate 12.00/s")
	a := findAlarm(c, "retention-rate:vault-2")
	if a == nil {
		t.Fatal("retention-rate alarm not surfaced")
	}
	if a.Priority != Low || a.Source != "retention" || a.Cause == "" || a.Response == "" {
		t.Errorf("retention-rate fields must be stamped from the catalog: %+v", a)
	}
}

func TestClearIsTypeAndInstanceAddressed(t *testing.T) {
	c := New()

	c.Raise("cloud-store", "v1", "msg")
	c.Raise("cloud-store", "v2", "msg")
	c.Clear("cloud-store", "v1")

	if findAlarm(c, "cloud-store:v1") != nil {
		t.Error("cleared alarm still active")
	}
	if findAlarm(c, "cloud-store:v2") == nil {
		t.Error("clear must not touch other instances of the same type")
	}

	c.Raise("node-disk-space-exhausted", "", "msg")
	c.Clear("node-disk-space-exhausted", "")
	if findAlarm(c, "node-disk-space-exhausted") != nil {
		t.Error("bare-ID clear failed")
	}
}

func TestClearNonExistent(t *testing.T) {
	c := New()
	c.Clear("cloud-store", "does-not-exist") // must not panic
	c.Clear("no-such-type", "")              // unknown type clear is a no-op too
}

func TestStandingEmpty(t *testing.T) {
	c := New()
	if alarms := c.Standing(); alarms != nil {
		t.Errorf("empty collector should return nil, got %v", alarms)
	}
}

func TestCount(t *testing.T) {
	c := New()
	if c.Count() != 0 {
		t.Fatalf("count = %d, want 0", c.Count())
	}
	c.Raise("cloud-store", "a", "msg")
	c.Raise("cloud-store", "b", "msg")
	if c.Count() != 2 {
		t.Fatalf("count = %d, want 2", c.Count())
	}
	c.Clear("cloud-store", "a")
	if c.Count() != 1 {
		t.Fatalf("count = %d, want 1", c.Count())
	}
}

func TestStandingSortedByFirstSeen(t *testing.T) {
	c := New()

	c.Raise("cloud-store", "third", "msg")
	c.Raise("cloud-store", "second", "msg")
	c.Raise("cloud-store", "first", "msg")

	alarms := c.Standing()
	for i := 1; i < len(alarms); i++ {
		if alarms[i].FirstSeen.Before(alarms[i-1].FirstSeen) {
			t.Errorf("alarm %d (FirstSeen=%v) is before alarm %d (FirstSeen=%v)",
				i, alarms[i].FirstSeen, i-1, alarms[i-1].FirstSeen)
		}
	}
}

// TestCatalogComplete pins the EEMUA 191 invariants of the catalog itself:
// every type carries operator guidance, a priority verdict (or is a marked
// software fault), and a source; type IDs are unique and colon-free (the
// colon is the type/instance separator in composed alarm IDs).
func TestCatalogComplete(t *testing.T) {
	types := Types()
	if len(types) == 0 {
		t.Fatal("catalog is empty")
	}
	seen := make(map[string]bool)
	for _, tt := range types {
		if tt.IDPrefix == "" {
			t.Fatal("catalog entry with empty IDPrefix")
		}
		if seen[tt.IDPrefix] {
			t.Errorf("%s: duplicate catalog entry", tt.IDPrefix)
		}
		seen[tt.IDPrefix] = true
		if strings.Contains(tt.IDPrefix, ":") {
			t.Errorf("%s: type IDs must not contain the instance separator", tt.IDPrefix)
		}
		if tt.Cause == "" {
			t.Errorf("%s: empty Cause — every alarm type documents its condition", tt.IDPrefix)
		}
		if tt.Response == "" {
			t.Errorf("%s: empty Response — every alarm requires an operator action", tt.IDPrefix)
		}
		if tt.Source == "" {
			t.Errorf("%s: empty Source", tt.IDPrefix)
		}
		if tt.SoftwareFault {
			if tt.Priority != 0 {
				t.Errorf("%s: software faults sit outside the priority scale", tt.IDPrefix)
			}
		} else if tt.Priority != Low && tt.Priority != High && tt.Priority != Critical {
			t.Errorf("%s: priority %v is not a cataloged verdict", tt.IDPrefix, tt.Priority)
		}
	}
}

func TestTypeByID(t *testing.T) {
	if _, ok := TypeByID("orchestrator-lock-leak"); !ok {
		t.Error("orchestrator-lock-leak missing from catalog")
	}
	if tt, _ := TypeByID("orchestrator-lock-leak"); !tt.SoftwareFault || !tt.Latching {
		t.Error("lock-leak must be a latching software fault")
	}
	if _, ok := TypeByID("nope"); ok {
		t.Error("TypeByID must report unknown types")
	}
}

func TestPriorityString(t *testing.T) {
	cases := map[Priority]string{Low: "low", High: "high", Critical: "critical", 0: "unspecified"}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("Priority(%d).String() = %q, want %q", p, got, want)
		}
	}
}

func TestConcurrentRaiseClear(t *testing.T) {
	c := New()
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 200; j++ {
				c.Raise("cloud-store", "shared", "msg")
				c.Standing()
				c.Clear("cloud-store", "shared")
				c.Count()
			}
		}()
	}
	for i := 0; i < 4; i++ {
		<-done
	}
}

// findAlarm returns the active alarm with the given composed ID, or nil.
func findAlarm(c *Collector, id string) *Alarm {
	for _, a := range c.Standing() {
		if a.ID == id {
			return a
		}
	}
	return nil
}
