package timestamp

import (
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

func TestDigest_RFC3339(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string // RFC 3339 representation of expected time
	}{
		{"UTC with Z", "2024-01-15T10:30:45Z some log message", "2024-01-15T10:30:45Z"},
		{"with offset", "2024-01-15T10:30:45+01:00 some log message", "2024-01-15T10:30:45+01:00"},
		{"negative offset", "2024-01-15T10:30:45-05:00 something", "2024-01-15T10:30:45-05:00"},
		{"with fractional seconds", "2024-01-15T10:30:45.123456Z msg", "2024-01-15T10:30:45.123456Z"},
		{"with millis", "2024-01-15T10:30:45.123Z msg", "2024-01-15T10:30:45.123Z"},
		{"frac and offset", "2024-01-15T10:30:45.123+01:00 msg", "2024-01-15T10:30:45.123+01:00"},
		{"mid-line", "level=INFO ts=2024-06-01T12:00:00Z msg=ok", "2024-06-01T12:00:00Z"},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if msg.SourceTS.IsZero() {
				t.Fatal("expected SourceTS to be set")
			}
			want, err := time.Parse(time.RFC3339Nano, tt.want)
			if err != nil {
				t.Fatalf("bad test want: %v", err)
			}
			if !msg.SourceTS.Equal(want) {
				t.Errorf("got %v, want %v", msg.SourceTS, want)
			}
		})
	}
}

func TestDigest_AppleUnified(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string // time.Parse format result
	}{
		{
			"basic with timezone",
			"2024-01-15 10:30:45.123456-0800 localhost syslogd[1]: message",
			"2024-01-15T10:30:45.123456-08:00",
		},
		{
			"positive timezone",
			"2024-01-15 10:30:45.000000+0530 host msg",
			"2024-01-15T10:30:45+05:30",
		},
		{
			"no fractional seconds with tz",
			"2024-01-15 10:30:45-0800 host msg",
			"2024-01-15T10:30:45-08:00",
		},
		{
			"no timezone",
			"2024-01-15 10:30:45 host msg",
			"2024-01-15T10:30:45Z",
		},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if msg.SourceTS.IsZero() {
				t.Fatal("expected SourceTS to be set")
			}
			want, err := time.Parse(time.RFC3339Nano, tt.want)
			if err != nil {
				t.Fatalf("bad test want: %v", err)
			}
			if !msg.SourceTS.Equal(want) {
				t.Errorf("got %v, want %v", msg.SourceTS, want)
			}
		})
	}
}

func TestDigest_SyslogBSD(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		raw       string
		wantMonth time.Month
		wantDay   int
		wantHour  int
	}{
		{"double digit day", "Jan 15 10:30:45 host sshd: msg", time.January, 15, 10},
		{"single digit day", "Feb  5 03:22:11 host kernel: msg", time.February, 5, 3},
		{"december", "Dec 31 23:59:59 host app: msg", time.December, 31, 23},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if msg.SourceTS.IsZero() {
				t.Fatal("expected SourceTS to be set")
			}
			// Year should be current year (or previous, for rollover).
			y := msg.SourceTS.Year()
			if y != now.Year() && y != now.Year()-1 {
				t.Errorf("unexpected year %d", y)
			}
			if msg.SourceTS.Month() != tt.wantMonth {
				t.Errorf("month: got %v, want %v", msg.SourceTS.Month(), tt.wantMonth)
			}
			if msg.SourceTS.Day() != tt.wantDay {
				t.Errorf("day: got %d, want %d", msg.SourceTS.Day(), tt.wantDay)
			}
			if msg.SourceTS.Hour() != tt.wantHour {
				t.Errorf("hour: got %d, want %d", msg.SourceTS.Hour(), tt.wantHour)
			}
		})
	}
}

func TestDigest_CLF(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			"apache access log",
			`192.168.1.5 - - [02/Jan/2006:15:04:05 -0700] "GET /api HTTP/1.1" 200 1234`,
			"2006-01-02T15:04:05-07:00",
		},
		{
			"nginx log",
			`10.0.0.1 - user [15/Mar/2024:08:30:00 +0000] "POST /upload HTTP/1.1" 201 0`,
			"2024-03-15T08:30:00Z",
		},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if msg.SourceTS.IsZero() {
				t.Fatal("expected SourceTS to be set")
			}
			want, err := time.Parse(time.RFC3339, tt.want)
			if err != nil {
				t.Fatalf("bad test want: %v", err)
			}
			if !msg.SourceTS.Equal(want) {
				t.Errorf("got %v, want %v", msg.SourceTS, want)
			}
		})
	}
}

func TestDigest_GoRuby(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			"basic",
			"2024/01/15 10:30:45 [INFO] starting server",
			"2024-01-15T10:30:45Z",
		},
		{
			"mid-line",
			"I, [2024/03/20 14:22:33] INFO -- : message",
			"2024-03-20T14:22:33Z",
		},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if msg.SourceTS.IsZero() {
				t.Fatal("expected SourceTS to be set")
			}
			want, err := time.Parse(time.RFC3339, tt.want)
			if err != nil {
				t.Fatalf("bad test want: %v", err)
			}
			if !msg.SourceTS.Equal(want) {
				t.Errorf("got %v, want %v", msg.SourceTS, want)
			}
		})
	}
}

func TestDigest_Ctime(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantMonth time.Month
		wantDay   int
		wantYear  int // 0 means check current year
	}{
		{"with year", "Fri Feb 13 17:49:50 2026 msg", time.February, 13, 2026},
		{"frac with year", "Fri Feb 13 17:49:50.028 2026 msg", time.February, 13, 2026},
		{"single-digit day", "Thu Jan  2 15:04:05 2026 msg", time.January, 2, 2026},
		{"no year", "Sat Dec 25 12:00:00 merry christmas", time.December, 25, 0},
		{"mid-line", "myapp: Fri Feb 13 17:49:50 2026 something", time.February, 13, 2026},
	}

	now := time.Now()
	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if msg.SourceTS.IsZero() {
				t.Fatal("expected SourceTS to be set")
			}
			if msg.SourceTS.Month() != tt.wantMonth {
				t.Errorf("month: got %v, want %v", msg.SourceTS.Month(), tt.wantMonth)
			}
			if msg.SourceTS.Day() != tt.wantDay {
				t.Errorf("day: got %d, want %d", msg.SourceTS.Day(), tt.wantDay)
			}
			if tt.wantYear != 0 {
				if msg.SourceTS.Year() != tt.wantYear {
					t.Errorf("year: got %d, want %d", msg.SourceTS.Year(), tt.wantYear)
				}
			} else {
				y := msg.SourceTS.Year()
				if y != now.Year() && y != now.Year()-1 {
					t.Errorf("unexpected year %d", y)
				}
			}
		})
	}
}

func TestDigest_SkipsNonZeroSourceTS(t *testing.T) {
	d := New()
	existing := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	msg := &orchestrator.IngestMessage{
		Raw:      []byte("2024-01-15T10:30:45Z some log"),
		SourceTS: existing,
	}
	d.Digest(msg)
	if !msg.SourceTS.Equal(existing) {
		t.Errorf("SourceTS was modified: got %v, want %v", msg.SourceTS, existing)
	}
}

func TestDigest_NoMatch(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"plain text", "starting worker pool"},
		{"random numbers", "abc123 xyz789 1234"},
		{"empty", ""},
		{"partial date", "2024-01"},
		{"invalid date", "2024-99-99T00:00:00Z"},
	}

	d := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &orchestrator.IngestMessage{
				Raw: []byte(tt.raw),
			}
			d.Digest(msg)
			if !msg.SourceTS.IsZero() {
				t.Errorf("expected zero SourceTS, got %v", msg.SourceTS)
			}
		})
	}
}

func TestDigest_EarliestPositionWins(t *testing.T) {
	// Syslog BSD at position 0, RFC 3339 later in message.
	// Syslog starts earlier, so it should win.
	d := New()
	raw := "Jan 15 10:30:45 host app: got event at 2099-06-01T12:00:00Z"
	msg := &orchestrator.IngestMessage{
		Raw: []byte(raw),
	}
	d.Digest(msg)
	if msg.SourceTS.IsZero() {
		t.Fatal("expected SourceTS to be set")
	}
	// Should have parsed the syslog timestamp (position 0), not the RFC 3339 one.
	if msg.SourceTS.Month() != time.January || msg.SourceTS.Day() != 15 {
		t.Errorf("expected Jan 15 from syslog prefix, got %v", msg.SourceTS)
	}
}

func TestDigest_RFC3339BeforeSyslog(t *testing.T) {
	// RFC 3339 at position 0, syslog BSD later — RFC 3339 should win.
	d := New()
	raw := "2024-06-01T12:00:00Z Jan 15 10:30:45 host msg"
	msg := &orchestrator.IngestMessage{
		Raw: []byte(raw),
	}
	d.Digest(msg)
	if msg.SourceTS.IsZero() {
		t.Fatal("expected SourceTS to be set")
	}
	want, _ := time.Parse(time.RFC3339, "2024-06-01T12:00:00Z")
	if !msg.SourceTS.Equal(want) {
		t.Errorf("got %v, want %v", msg.SourceTS, want)
	}
}

func TestExtractTimestamp_AllFormats(t *testing.T) {
	// Verify each format independently via the internal function.
	tests := []struct {
		name    string
		raw     string
		wantSet bool
	}{
		{"rfc3339", "2024-01-15T10:30:45Z", true},
		{"apple", "2024-01-15 10:30:45.123456-0800", true},
		{"syslog", "Jan  5 15:04:02 host msg", true},
		{"clf", "[02/Jan/2006:15:04:05 -0700]", true},
		{"goruby", "2024/01/15 10:30:45", true},
		{"nothing", "hello world", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := extractTimestamp([]byte(tt.raw))
			if tt.wantSet && ts.IsZero() {
				t.Error("expected non-zero timestamp")
			}
			if !tt.wantSet && !ts.IsZero() {
				t.Errorf("expected zero timestamp, got %v", ts)
			}
		})
	}
}
