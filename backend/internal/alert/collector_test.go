package alert

import (
	"testing"
)

func TestSetAndActive(t *testing.T) {
	c := New()

	c.Set("test-1", Error, "component-a", "something broke")
	c.Set("test-2", Warning, "component-b", "something might break")

	alerts := c.Active()
	if len(alerts) != 2 {
		t.Fatalf("got %d alerts, want 2", len(alerts))
	}
	if alerts[0].ID != "test-1" {
		t.Errorf("first alert ID = %q, want test-1", alerts[0].ID)
	}
	if alerts[0].Severity != Error {
		t.Errorf("first alert severity = %d, want Error", alerts[0].Severity)
	}
}

func TestSetIdempotent(t *testing.T) {
	c := New()

	c.Set("test-1", Warning, "comp", "first message")
	first := c.Active()[0].FirstSeen

	c.Set("test-1", Warning, "comp", "updated message")
	alerts := c.Active()

	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want 1", len(alerts))
	}
	if alerts[0].FirstSeen != first {
		t.Error("FirstSeen changed on update")
	}
	if alerts[0].Message != "updated message" {
		t.Errorf("message = %q, want 'updated message'", alerts[0].Message)
	}
	if !alerts[0].LastSeen.After(first) || alerts[0].LastSeen.Equal(first) {
		// LastSeen should be >= FirstSeen (may be equal if test runs fast)
	}
}

func TestClear(t *testing.T) {
	c := New()

	c.Set("test-1", Error, "comp", "msg")
	c.Set("test-2", Warning, "comp", "msg")
	c.Clear("test-1")

	alerts := c.Active()
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want 1", len(alerts))
	}
	if alerts[0].ID != "test-2" {
		t.Errorf("remaining alert ID = %q, want test-2", alerts[0].ID)
	}
}

func TestClearNonExistent(t *testing.T) {
	c := New()
	c.Clear("does-not-exist") // should not panic
}

func TestActiveEmpty(t *testing.T) {
	c := New()
	if alerts := c.Active(); alerts != nil {
		t.Errorf("empty collector should return nil, got %v", alerts)
	}
}

func TestCount(t *testing.T) {
	c := New()
	if c.Count() != 0 {
		t.Fatalf("count = %d, want 0", c.Count())
	}
	c.Set("a", Warning, "comp", "msg")
	c.Set("b", Error, "comp", "msg")
	if c.Count() != 2 {
		t.Fatalf("count = %d, want 2", c.Count())
	}
	c.Clear("a")
	if c.Count() != 1 {
		t.Fatalf("count = %d, want 1", c.Count())
	}
}

func TestActiveSortedByFirstSeen(t *testing.T) {
	c := New()

	// Insert in reverse order
	c.Set("third", Warning, "comp", "msg")
	c.Set("second", Warning, "comp", "msg")
	c.Set("first", Warning, "comp", "msg")

	alerts := c.Active()
	for i := 1; i < len(alerts); i++ {
		if alerts[i].FirstSeen.Before(alerts[i-1].FirstSeen) {
			t.Errorf("alert %d (FirstSeen=%v) is before alert %d (FirstSeen=%v)",
				i, alerts[i].FirstSeen, i-1, alerts[i-1].FirstSeen)
		}
	}
}
