// Package metrics provides a self-monitoring ingester that emits process-level
// metrics (CPU, memory, goroutines, queue depth) as log records.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/sysmetrics"
)

type ingester struct {
	id       string
	interval time.Duration
	src      StatsSource
	logger   *slog.Logger
}

// Run emits one metrics record per interval until ctx is cancelled.
func (m *ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	m.logger.Info("started", "interval", m.interval)

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		msg := m.collect()
		select {
		case out <- msg:
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *ingester) collect() orchestrator.IngestMessage {
	cpu := sysmetrics.CPUPercent()
	mem := sysmetrics.Memory()
	goroutines := runtime.NumGoroutine()
	queueDepth := m.src.IngestQueueDepth()
	queueCap := m.src.IngestQueueCapacity()

	now := time.Now()

	raw := fmt.Sprintf(
		"cpu_percent=%.1f heap_alloc_bytes=%d heap_inuse_bytes=%d heap_idle_bytes=%d heap_released_bytes=%d stack_inuse_bytes=%d sys_bytes=%d rss_bytes=%d heap_objects=%d num_gc=%d num_goroutine=%d ingest_queue_depth=%d ingest_queue_capacity=%d",
		cpu,
		mem.HeapAlloc,
		mem.HeapInuse,
		mem.HeapIdle,
		mem.HeapReleased,
		mem.StackInuse,
		mem.Sys,
		mem.RSS,
		mem.HeapObjects,
		mem.NumGC,
		goroutines,
		queueDepth,
		queueCap,
	)

	return orchestrator.IngestMessage{
		Attrs: map[string]string{
			"ingester_type": "metrics",
			"ingester_id":   m.id,
			"level":         "info",
		},
		Raw:      []byte(raw),
		SourceTS: now,
		IngestTS: now,
	}
}
