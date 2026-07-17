package alert

// Alarm lifecycle journal tests (gastrolog-1z5gg4): ack/shelve state
// survives restart via the per-node journal file. "Restart" is simulated by
// building a NEW collector on the same journal path — the same replay the
// production node runs at boot. Every collector runs on an injected clock;
// no sleeps.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func journalPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "alarm-journal.jsonl")
}

func openCollector(t *testing.T, path string, clock func() time.Time) *Collector {
	t.Helper()
	c := NewWithClock(clock)
	if err := c.OpenJournal(path); err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	t.Cleanup(c.CloseJournal)
	return c
}

// TestJournal_AckSurvivesRestart: ack an alarm, rebuild the collector from
// the journal, re-raise the same standing condition (as its raiser would
// after boot) — the alarm comes back active-ACKED with the operator's
// identity intact.
func TestJournal_AckSurvivesRestart(t *testing.T) {
	path := journalPath(t)
	clock := newSuppressionClock()

	c1 := openCollector(t, path, clock.Now)
	c1.Raise("chunking-underreplicated", "vault-1", "3 segments below minimum")
	if err := c1.Ack("chunking-underreplicated:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	c1.CloseJournal()

	// Restart: fresh collector, same journal. The standing condition is
	// re-detected by its raiser after boot.
	clock.Advance(time.Minute)
	c2 := openCollector(t, path, clock.Now)
	c2.Raise("chunking-underreplicated", "vault-1", "3 segments below minimum")
	a := mustState(t, c2, "chunking-underreplicated:vault-1", StateActiveAcked)
	if a.AckedBy != "alice" {
		t.Fatalf("acked_by after replay = %q, want alice", a.AckedBy)
	}
}

// TestJournal_ShelveSurvivesAndExpires: an unexpired shelve replays as
// shelved; a shelve that lapsed while the node was down replays as nothing
// (active-unacked, mirroring live expiry).
func TestJournal_ShelveSurvivesAndExpires(t *testing.T) {
	path := journalPath(t)
	clock := newSuppressionClock()

	c1 := openCollector(t, path, clock.Now)
	c1.Raise("disk-space-low", "vault-1", "below warn band")
	until, err := c1.Shelve("disk-space-low:vault-1", 2*time.Hour, "alice")
	if err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	c1.CloseJournal()

	// Restart inside the shelve window: still shelved, same expiry.
	clock.Advance(30 * time.Minute)
	c2 := openCollector(t, path, clock.Now)
	c2.Raise("disk-space-low", "vault-1", "below warn band")
	a := mustState(t, c2, "disk-space-low:vault-1", StateShelved)
	if !a.ShelvedUntil.Equal(until) {
		t.Fatalf("replayed shelve expiry = %v, want %v", a.ShelvedUntil, until)
	}
	c2.CloseJournal()

	// Restart after the shelve lapsed: pending state pruned at load; the
	// re-raised condition annunciates active-unacked.
	clock.Advance(3 * time.Hour)
	c3 := openCollector(t, path, clock.Now)
	c3.Raise("disk-space-low", "vault-1", "below warn band")
	a = mustState(t, c3, "disk-space-low:vault-1", StateActiveUnacked)
	if !a.ShelvedUntil.IsZero() {
		t.Fatalf("lapsed shelve left residue: %+v", a)
	}
}

// TestJournal_ResolvePrunes: an alarm that fully releases writes a resolve
// record, so a LATER occurrence after restart comes back untouched —
// yesterday's ack must never mark today's incident as handled.
func TestJournal_ResolvePrunes(t *testing.T) {
	path := journalPath(t)
	clock := newSuppressionClock()

	c1 := openCollector(t, path, clock.Now)
	c1.Raise("disk-space-exhausted", "vault-1", "volume full")
	if err := c1.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// Condition resolves: acked alarm releases and its journal state is
	// pruned by the resolve record.
	c1.Clear("disk-space-exhausted", "vault-1")
	c1.CloseJournal()

	clock.Advance(24 * time.Hour)
	c2 := openCollector(t, path, clock.Now)
	c2.Raise("disk-space-exhausted", "vault-1", "volume full AGAIN")
	a := mustState(t, c2, "disk-space-exhausted:vault-1", StateActiveUnacked)
	if a.AckedBy != "" {
		t.Fatalf("resolved ack resurrected on a new occurrence: %+v", a)
	}
}

// TestJournal_LatchedAckReleaseIsResolved: acking a latched alarm whose
// condition already resolved releases it AND prunes the journal — the ack
// that released must not replay against a future firing of the tripwire.
func TestJournal_LatchedAckReleaseIsResolved(t *testing.T) {
	path := journalPath(t)
	clock := newSuppressionClock()

	c1 := openCollector(t, path, clock.Now)
	delayedRaise(c1, "latched-alarm", "up", 0, 0, true)
	c1.Clear("latched-alarm", "")
	mustState(t, c1, "latched-alarm", StateActiveUnacked)
	if err := c1.Ack("latched-alarm", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	mustGone(t, c1, "latched-alarm")
	c1.CloseJournal()

	c2 := openCollector(t, path, clock.Now)
	delayedRaise(c2, "latched-alarm", "up again", 0, 0, true)
	a := mustState(t, c2, "latched-alarm", StateActiveUnacked)
	if a.AckedBy != "" {
		t.Fatalf("released latched ack replayed onto a new firing: %+v", a)
	}
}

// TestJournal_CompactionAndCorruptLineTolerance: the journal compacts to
// its folded state at open, and a torn/garbage line (crash mid-append) is
// skipped without discarding the intact records around it.
func TestJournal_CompactionAndCorruptLineTolerance(t *testing.T) {
	path := journalPath(t)
	clock := newSuppressionClock()

	c1 := openCollector(t, path, clock.Now)
	c1.Raise("disk-space-exhausted", "vault-1", "full")
	c1.Raise("disk-space-exhausted", "vault-2", "full")
	// vault-1 accumulates history: ack, shelve (resets ack), unshelve, ack.
	if err := c1.Ack("disk-space-exhausted:vault-1", "alice"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if _, err := c1.Shelve("disk-space-exhausted:vault-1", time.Hour, "alice"); err != nil {
		t.Fatalf("Shelve: %v", err)
	}
	if err := c1.Unshelve("disk-space-exhausted:vault-1"); err != nil {
		t.Fatalf("Unshelve: %v", err)
	}
	if err := c1.Ack("disk-space-exhausted:vault-1", "bob"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// vault-2 acks then resolves — pruned entirely.
	if err := c1.Ack("disk-space-exhausted:vault-2", "carol"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	c1.Clear("disk-space-exhausted", "vault-2")
	c1.CloseJournal()

	// Torn final line from a crash mid-append.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatalf("append garbage: %v", err)
	}
	if _, err := f.WriteString(`{"op":"ack","id":"trunc`); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	_ = f.Close()

	c2 := openCollector(t, path, clock.Now)
	c2.Raise("disk-space-exhausted", "vault-1", "full")
	a := mustState(t, c2, "disk-space-exhausted:vault-1", StateActiveAcked)
	if a.AckedBy != "bob" {
		t.Fatalf("folded ack = %q, want bob (last writer wins)", a.AckedBy)
	}
	c2.Raise("disk-space-exhausted", "vault-2", "full again")
	mustState(t, c2, "disk-space-exhausted:vault-2", StateActiveUnacked)

	// Compaction: the reopened file holds exactly the folded state — one
	// ack line for vault-1 — not the full history plus garbage.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read compacted journal: %v", err)
	}
	// c2's own raise/ack appends came after compaction; count only lines
	// present at open by checking the compacted state before c2 appended
	// anything: no shelve/unshelve/garbage survive.
	content := string(data)
	if strings.Contains(content, "unshelve") || strings.Contains(content, "trunc") {
		t.Fatalf("compaction left history/garbage behind:\n%s", content)
	}
	if got := strings.Count(content, "\"op\":\"ack\""); got != 1 {
		t.Fatalf("compacted journal has %d ack lines, want 1:\n%s", got, content)
	}
}

// TestJournal_OpenOnMissingDirFails: an unwritable journal path errors at
// open (the caller degrades loudly rather than losing ops silently).
func TestJournal_OpenOnMissingDirFails(t *testing.T) {
	c := NewWithClock(newSuppressionClock().Now)
	err := c.OpenJournal(filepath.Join(t.TempDir(), "no-such-dir", "j.jsonl"))
	if err == nil {
		t.Fatal("OpenJournal on a missing directory must error")
	}
	if errors.Is(err, os.ErrNotExist) {
		return // expected shape
	}
}
