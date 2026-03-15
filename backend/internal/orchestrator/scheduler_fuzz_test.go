package orchestrator

import (
	"testing"

	"github.com/go-co-op/gocron/v2"
)

// FuzzCronJobParse fuzzes gocron's cron expression parser with random strings.
// User-configurable schedules flow through AddJob → gocron.CronJob, so crafted
// cron expressions are untrusted input to a third-party library.
func FuzzCronJobParse(f *testing.F) {
	f.Add("* * * * *")
	f.Add("*/5 * * * *")
	f.Add("0 0 * * *")
	f.Add("0 */2 * * * *")       // 6-field (with seconds)
	f.Add("@every 1h")
	f.Add("@hourly")
	f.Add("@daily")
	f.Add("")
	f.Add("not a cron")
	f.Add("* * * * * * * * * *")  // too many fields
	f.Add("999 999 999 999 999")
	f.Add("-1 -1 -1 -1 -1")
	f.Add("0/0 * * * *")         // zero step
	f.Add("0-100000 * * * *")    // huge range
	f.Add("*/0 * * * *")         // zero interval
	f.Add("\x00\x01\x02\x03\x04")

	f.Fuzz(func(t *testing.T, expr string) {
		s, err := gocron.NewScheduler()
		if err != nil {
			t.Skip("scheduler init failed")
		}
		defer func() { _ = s.Shutdown() }()

		// This should return an error for invalid expressions, never panic.
		_, err = s.NewJob(
			gocron.CronJob(expr, true),
			gocron.NewTask(func() {}),
		)
		_ = err // we only care about panics
	})
}
