package orchestrator

import (
	"fmt"
	"strings"
	"time"
)

// CronEvery returns a 6-field cron expression (with seconds) for a fixed
// interval. Used instead of gocron's "@every" syntax so every scheduled job
// shares one wire/display format.
func CronEvery(interval time.Duration) string {
	if interval <= 0 {
		return "* * * * * *"
	}
	if interval%time.Second != 0 {
		interval = ((interval + time.Second - 1) / time.Second) * time.Second
	}
	sec := int(interval / time.Second)
	if sec < 60 {
		if sec <= 1 {
			return "* * * * * *"
		}
		return fmt.Sprintf("*/%d * * * * *", sec)
	}
	if interval%time.Minute == 0 {
		minutes := int(interval / time.Minute)
		if minutes <= 1 {
			return "0 * * * * *"
		}
		if minutes < 60 {
			return fmt.Sprintf("0 */%d * * * *", minutes)
		}
	}
	if interval%time.Hour == 0 {
		hour := int(interval / time.Hour)
		if hour <= 1 {
			return "0 0 * * * *"
		}
		return fmt.Sprintf("0 0 */%d * * *", hour)
	}
	// Unusual duration — fall back to the nearest second tick.
	if sec <= 1 {
		return "* * * * * *"
	}
	return fmt.Sprintf("*/%d * * * * *", sec)
}

// sweepCadenceOverride, when non-empty, replaces the cron expression of every
// periodic reconcile sweep that goes through sweepCron. It is written from the
// orchestrator package's TestMain ONLY (testprofile_test.go) and is empty in
// every shipped build — production cadence is the constant passed to sweepCron.
//
// The seam exists because the orchestrator's multi-node acceptance tests wait
// on real sweep ticks: at production cadence a single retention-driven
// assertion costs a 60s cron period and a catch-up assertion costs 20s, which
// is what made the package's non-short runtime wall-clock rather than
// compute bound. Compressing the cadence keeps the periodic code path under
// test — the sweeps still run on their own timer, nothing is poked by hand —
// it just stops the suite from sitting out real minutes.
var sweepCadenceOverride string

// sweepCron resolves the cron expression a periodic sweep registers with.
// Returns production unchanged unless the test profile is installed.
func sweepCron(production string) string {
	if sweepCadenceOverride != "" {
		return sweepCadenceOverride
	}
	return production
}

// NormalizeCronSchedule converts legacy "@every" and 5-field cron expressions
// to canonical 6-field cron (second minute hour dom month dow).
func NormalizeCronSchedule(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" || expr == "once" {
		return expr
	}
	if after, ok := strings.CutPrefix(expr, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(after))
		if err != nil {
			return expr
		}
		return CronEvery(d)
	}
	if len(strings.Fields(expr)) == 5 {
		return "0 " + expr
	}
	return expr
}
