package alert

// Alarm lifecycle journal (gastrolog-1z5gg4): ack and shelve state survives
// node restart via a small append-only JSON-Lines file under the node home.
// It is deliberately NOT config and NOT Raft — an ack is operator telemetry
// about one node's standing alarm, and routing it through consensus would
// make acking a cluster write.
//
// Format: one JSON object per line, {"op","id","by","at","until"} with op
// one of ack | shelve | unshelve | resolve. Later records for the same
// alarm ID supersede earlier ones when folding:
//
//   - ack      sets acknowledgment (who + when).
//   - shelve   sets the shelve expiry and resets acknowledgment (mirroring
//     the live semantics: shelving supersedes awareness).
//   - unshelve drops the shelve expiry.
//   - resolve  drops everything for the ID — written when the alarm fully
//     releases, so a replay can never resurrect operator actions against a
//     future occurrence. This is the prune: journal entries whose alarm is
//     gone are superseded by their resolve record and dropped at the next
//     compaction.
//
// Replay: at OpenJournal the file is folded into per-ID pending lifecycle
// state (expired shelves prune immediately, against the collector clock),
// the file is compacted to the surviving records, and each pending state is
// applied lazily to the FIRST annunciation of its alarm ID after startup
// (applyPendingLocked) — standing conditions are re-detected by their
// raisers after boot, so the match happens naturally, with no timers. A
// pending entry whose alarm never returns stays in the compacted file until
// its ID next resolves or is superseded; it is one bounded line per alarm
// ID and can never act without the alarm annunciating.
//
// Journal I/O failures are logged and never fail the lifecycle operation:
// losing restart-survival is strictly better than refusing an ack.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"
)

const (
	journalOpAck      = "ack"
	journalOpShelve   = "shelve"
	journalOpUnshelve = "unshelve"
	journalOpResolve  = "resolve"
)

// journalRecord is one line of the lifecycle journal.
type journalRecord struct {
	Op    string    `json:"op"`
	ID    string    `json:"id"`
	By    string    `json:"by,omitempty"`
	At    time.Time `json:"at"`
	Until time.Time `json:"until,omitzero"`
}

// pendingLifecycle is folded journal state for one alarm ID, waiting to be
// re-applied to the alarm's first annunciation after startup.
type pendingLifecycle struct {
	Acked        bool
	AckedBy      string
	AckedAt      time.Time
	ShelvedUntil time.Time
}

// journal is the collector's attached lifecycle journal writer.
type journal struct {
	path string
	f    *os.File
}

// append writes one record and fsyncs. Errors are logged, never returned —
// see the package comment above.
func (j *journal) append(rec journalRecord) {
	line, err := json.Marshal(rec)
	if err != nil {
		slog.Error("alarm journal: marshal record", "error", err, "id", rec.ID)
		return
	}
	line = append(line, '\n')
	if _, err := j.f.Write(line); err != nil {
		slog.Error("alarm journal: append", "error", err, "path", j.path)
		return
	}
	if err := j.f.Sync(); err != nil {
		slog.Error("alarm journal: fsync", "error", err, "path", j.path)
	}
}

// OpenJournal attaches a lifecycle journal at path to the collector: it
// replays the existing file into pending ack/shelve state (applied lazily
// as matching alarms annunciate), compacts the file, and persists every
// subsequent Ack/Shelve/Unshelve and alarm release. Call once at startup,
// before components start raising.
func (c *Collector) OpenJournal(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending, err := loadJournal(path, c.now())
	if err != nil {
		return err
	}

	// Compact: rewrite only the surviving folded state, then reopen for
	// appends. A crash between write and rename leaves the original intact.
	tmp := path + ".tmp"
	var buf []byte
	for id, p := range pending {
		// Shelve before ack: folding replays in order and a shelve resets
		// acknowledgment, so an ack recorded while shelved must come last.
		if !p.ShelvedUntil.IsZero() {
			line, merr := json.Marshal(journalRecord{Op: journalOpShelve, ID: id, At: c.now(), Until: p.ShelvedUntil})
			if merr != nil {
				return fmt.Errorf("alarm journal: marshal compacted shelve: %w", merr)
			}
			buf = append(buf, line...)
			buf = append(buf, '\n')
		}
		if p.Acked {
			line, merr := json.Marshal(journalRecord{Op: journalOpAck, ID: id, By: p.AckedBy, At: p.AckedAt})
			if merr != nil {
				return fmt.Errorf("alarm journal: marshal compacted ack: %w", merr)
			}
			buf = append(buf, line...)
			buf = append(buf, '\n')
		}
	}
	if err := os.WriteFile(tmp, buf, 0o640); err != nil { //nolint:gosec // G306: operator telemetry, not secret
		return fmt.Errorf("alarm journal: write compacted file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("alarm journal: replace with compacted file: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640) //nolint:gosec // G304: path from trusted node home
	if err != nil {
		return fmt.Errorf("alarm journal: open for append: %w", err)
	}
	c.journal = &journal{path: path, f: f}
	c.pending = pending
	if len(pending) > 0 {
		slog.Info("alarm journal replayed — lifecycle state pending re-application",
			"path", path, "alarms", len(pending))
	}
	return nil
}

// loadJournal folds the journal file at path into per-ID pending state.
// Expired shelves prune during the fold (mirroring live expiry, which also
// resets acknowledgment). Unparseable lines are skipped loudly — a torn
// final line from a crash must not discard the intact history before it.
func loadJournal(path string, now time.Time) (map[string]pendingLifecycle, error) {
	pending := make(map[string]pendingLifecycle)
	f, err := os.Open(path) //nolint:gosec // G304: path from trusted node home
	if err != nil {
		if os.IsNotExist(err) {
			return pending, nil
		}
		return nil, fmt.Errorf("alarm journal: open: %w", err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec journalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			slog.Warn("alarm journal: skipping unparseable line", "path", path, "error", err)
			continue
		}
		switch rec.Op {
		case journalOpAck:
			p := pending[rec.ID]
			p.Acked, p.AckedBy, p.AckedAt = true, rec.By, rec.At
			pending[rec.ID] = p
		case journalOpShelve:
			p := pending[rec.ID]
			p.ShelvedUntil = rec.Until
			// Shelving resets acknowledgment (live semantics).
			p.Acked, p.AckedBy, p.AckedAt = false, "", time.Time{}
			pending[rec.ID] = p
		case journalOpUnshelve:
			p := pending[rec.ID]
			p.ShelvedUntil = time.Time{}
			pending[rec.ID] = p
		case journalOpResolve:
			delete(pending, rec.ID)
		default:
			slog.Warn("alarm journal: skipping record with unknown op", "path", path, "op", rec.Op)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("alarm journal: read: %w", err)
	}

	// Prune: expired shelves lapse now (their acks lapse with them, as at
	// live expiry), and IDs left with no state drop out.
	for id, p := range pending {
		if !p.ShelvedUntil.IsZero() && !now.Before(p.ShelvedUntil) {
			delete(pending, id)
		} else if !p.Acked && p.ShelvedUntil.IsZero() {
			delete(pending, id)
		}
	}
	return pending, nil
}

// CloseJournal flushes and detaches the journal, if attached.
func (c *Collector) CloseJournal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.journal != nil {
		_ = c.journal.f.Close()
		c.journal = nil
	}
}
