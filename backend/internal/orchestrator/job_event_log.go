package orchestrator

import (
	"context"
)

// runJobEventLogBridge subscribes to the scheduler's job-event broker
// and emits a structured slog entry for every transition. Lifecycle
// events flow through the slog handler chain — including the self
// ingester's CaptureHandler — so operators can search them like any
// other log line. See gastrolog-5euo for the self-ingester capture
// wiring and gastrolog-5mcqm for the broker.
//
// One-to-one mapping: the broker already coalesces the high-frequency
// progress events upstream, so this consumer just emits one log line
// per delivered JobEvent. Failed events log at Warn so operator
// dashboards filtering on level pick them up; everything else is Info.
func (o *Orchestrator) runJobEventLogBridge(ctx context.Context) {
	if o.scheduler == nil || o.logger == nil {
		return
	}
	sub, cancel := o.scheduler.Events().Subscribe()
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			o.logJobEvent(ev)
		}
	}
}

// logJobEvent maps a single JobEvent to a slog entry. Attribute set is
// deliberately small — the scheduler's broker emits clean transitions,
// not free-form telemetry.
func (o *Orchestrator) logJobEvent(ev JobEvent) {
	attrs := []any{
		"event", ev.Kind.String(),
		"job_id", ev.Job.ID,
		"job_name", ev.Job.Name,
	}
	if ev.Job.Description != "" {
		attrs = append(attrs, "description", ev.Job.Description)
	}
	if ev.Job.Schedule != "" {
		attrs = append(attrs, "schedule", ev.Job.Schedule)
	}
	if p := ev.Job.Progress; p != nil {
		if p.ChunksTotal > 0 {
			attrs = append(attrs, "chunks_total", p.ChunksTotal)
		}
		if p.ChunksDone > 0 {
			attrs = append(attrs, "chunks_done", p.ChunksDone)
		}
		if p.RecordsDone > 0 {
			attrs = append(attrs, "records_done", p.RecordsDone)
		}
		if p.Error != "" {
			attrs = append(attrs, "error", p.Error)
		}
	}
	msg := "job " + ev.Kind.String()
	if ev.Kind == JobEventFailed {
		o.logger.Warn(msg, attrs...)
		return
	}
	o.logger.Info(msg, attrs...)
}
