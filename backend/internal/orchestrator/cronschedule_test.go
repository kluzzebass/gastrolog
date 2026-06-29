package orchestrator

import (
	"testing"
	"time"
)

func TestCronEvery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		interval time.Duration
		want     string
	}{
		{time.Second, "* * * * * *"},
		{5 * time.Second, "*/5 * * * * *"},
		{30 * time.Second, "*/30 * * * * *"},
		{time.Minute, "0 * * * * *"},
		{5 * time.Minute, "0 */5 * * * *"},
		{time.Hour, "0 0 * * * *"},
	}
	for _, tc := range tests {
		if got := CronEvery(tc.interval); got != tc.want {
			t.Errorf("CronEvery(%v) = %q, want %q", tc.interval, got, tc.want)
		}
	}
}

func TestNormalizeCronSchedule(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in, want string
	}{
		{"@every 1s", "* * * * * *"},
		{"@every 5s", "*/5 * * * * *"},
		{"* * * * *", "0 * * * * *"},
		{"0 * * * *", "0 0 * * * *"},
		{"0 3 * * *", "0 0 3 * * *"},
		{"0 3 * * 2,5", "0 0 3 * * 2,5"},
		{"*/30 * * * * *", "*/30 * * * * *"},
		{"once", "once"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := NormalizeCronSchedule(tc.in); got != tc.want {
			t.Errorf("NormalizeCronSchedule(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
