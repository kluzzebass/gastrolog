package app

import (
	"context"
	"fmt"

	"gastrolog/internal/alert"
	"gastrolog/internal/system"
)

const (
	alarmRateJobName = "alarm-rate-monitor"
	// Every 15 seconds: fast enough that a flood clears promptly after its
	// clean window and a threshold change converges quickly; the window
	// itself is 10 minutes, so the tick cadence never decides an outcome.
	alarmRateSchedule = "*/15 * * * * *"
)

// startAlarmRateMonitorJob registers the alarm system's self-monitoring
// tick (EEMUA 191 rate principle). Each tick converges the flood threshold
// with the stored cluster settings — the discovery-based shape used by the
// disk guard's backlog budget: the setting lives in the Raft-replicated
// config, so a change saved on ANY node is honored on EVERY node's next
// tick with no mutation-path wiring — and then advances the flood state
// machine so alarm-flood clears even when no new alarms arrive.
//
// Alarm activations themselves reach the monitor event-driven via the
// collector's activation hook; this job only supplies the passage of time
// and the threshold.
func startAlarmRateMonitorJob(ctx context.Context, scheduler scheduledJobRegistry, monitor *alert.RateMonitor, cfgStore system.Store) error {
	tick := func() {
		if ss, err := cfgStore.LoadServerSettings(ctx); err == nil {
			monitor.SetThreshold(int(ss.Cluster.AlarmFloodThreshold))
		}
		monitor.Evaluate()
	}
	// Converge the threshold now, before components start raising alarms —
	// a boot burst must be judged against the operator's threshold, not a
	// window of default-vs-configured ambiguity.
	tick()
	if err := scheduler.AddJob(alarmRateJobName, alarmRateSchedule, tick); err != nil {
		return fmt.Errorf("alarm rate monitor job: %w", err)
	}
	scheduler.Describe(alarmRateJobName,
		"Alarm-rate self-monitoring: converges the alarm-flood threshold with cluster settings and advances the flood state machine (raises alarm-flood over threshold, clears it after a full under-threshold 10-minute window)")
	return nil
}
