// Package chanwatch provides threshold-crossing alerts for buffered channels.
// A Watcher polls channel utilization at a fixed interval and logs once when
// a channel crosses above the configured threshold, and once when it drops
// back below. This avoids log spam while still catching pressure spikes.
package chanwatch

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/alert"
)

// Probe returns the current length and capacity of a channel.
// Implementations must be safe to call from any goroutine.
type Probe func() (length, capacity int)

// channel is a single monitored channel.
type channel struct {
	name      string
	probe     Probe
	threshold float64 // 0.0–1.0
	pressured bool    // true while above threshold
}

// Watcher monitors one or more channels for pressure.
type Watcher struct {
	logger   *slog.Logger
	interval time.Duration
	alerts   *alert.Collector // optional

	mu       sync.Mutex
	channels []channel
}

// New creates a Watcher that polls at the given interval.
func New(logger *slog.Logger, interval time.Duration) *Watcher {
	return &Watcher{
		logger:   logger,
		interval: interval,
	}
}

// SetAlerts configures an alert collector for pressure notifications.
func (w *Watcher) SetAlerts(a *alert.Collector) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.alerts = a
}

// Watch adds a channel to monitor. Safe to call after Run has started.
// Threshold is a fraction (e.g. 0.9 = 90%).
func (w *Watcher) Watch(name string, probe Probe, threshold float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.channels = append(w.channels, channel{
		name:      name,
		probe:     probe,
		threshold: threshold,
	})
}

// Run polls all channels until ctx is cancelled. Blocks.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll()
		}
	}
}

func (w *Watcher) poll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.channels {
		ch := &w.channels[i]
		length, capacity := ch.probe()
		if capacity == 0 {
			continue
		}
		ratio := float64(length) / float64(capacity)

		if !ch.pressured && ratio >= ch.threshold {
			ch.pressured = true
			w.logger.Warn("channel pressure high",
				"channel", ch.name,
				"length", length,
				"capacity", capacity,
				"utilization", int(ratio*100),
			)
			if w.alerts != nil {
				w.alerts.Set(
					"channel-pressure:"+ch.name,
					alert.Warning, "chanwatch",
					fmt.Sprintf("Channel %q at %d%% capacity (%d/%d)", ch.name, int(ratio*100), length, capacity),
				)
			}
		} else if ch.pressured && ratio < ch.threshold {
			ch.pressured = false
			w.logger.Info("channel pressure resolved",
				"channel", ch.name,
				"length", length,
				"capacity", capacity,
				"utilization", int(ratio*100),
			)
			if w.alerts != nil {
				w.alerts.Clear("channel-pressure:" + ch.name)
			}
		}
	}
}
