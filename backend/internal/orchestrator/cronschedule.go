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
