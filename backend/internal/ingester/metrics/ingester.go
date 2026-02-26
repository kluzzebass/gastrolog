// Package metrics provides a self-monitoring ingester that emits process-level
// metrics (CPU, memory, goroutines, queue depth) and per-store stats as log records.
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
	id            string
	interval      time.Duration
	storeInterval time.Duration
	src           StatsSource
	logger        *slog.Logger
}

// Run emits system metrics on interval and store metrics on storeInterval
// until ctx is cancelled.
func (m *ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	m.logger.Info("started", "interval", m.interval, "store_interval", m.storeInterval)

	sysTicker := time.NewTicker(m.interval)
	storeTicker := time.NewTicker(m.storeInterval)
	defer sysTicker.Stop()
	defer storeTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sysTicker.C:
			if !send(ctx, out, m.collectSystem()) {
				return nil
			}
		case <-storeTicker.C:
			for _, msg := range m.collectStores() {
				if !send(ctx, out, msg) {
					return nil
				}
			}
		}
	}
}

// send attempts a context-aware channel send. Returns false if ctx is done.
func send(ctx context.Context, out chan<- orchestrator.IngestMessage, msg orchestrator.IngestMessage) bool {
	select {
	case out <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *ingester) collectSystem() orchestrator.IngestMessage {
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
			"metric_type":   "system",
			"level":         "info",
		},
		Raw:      []byte(raw),
		SourceTS: now,
		IngestTS: now,
	}
}

func (m *ingester) collectStores() []orchestrator.IngestMessage {
	snapshots := m.src.StoreSnapshots()
	if len(snapshots) == 0 {
		return nil
	}

	now := time.Now()
	msgs := make([]orchestrator.IngestMessage, 0, len(snapshots))
	for _, snap := range snapshots {
		raw := fmt.Sprintf(
			"record_count=%d chunk_count=%d sealed_chunks=%d data_bytes=%d enabled=%t",
			snap.RecordCount,
			snap.ChunkCount,
			snap.SealedChunks,
			snap.DataBytes,
			snap.Enabled,
		)
		msgs = append(msgs, orchestrator.IngestMessage{
			Attrs: map[string]string{
				"ingester_type": "metrics",
				"ingester_id":   m.id,
				"metric_type":   "store",
				"store_id":      snap.ID.String(),
				"level":         "info",
			},
			Raw:      []byte(raw),
			SourceTS: now,
			IngestTS: now,
		})
	}
	return msgs
}
