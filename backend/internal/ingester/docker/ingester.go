package docker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/querylang"
)

// trackedContainer holds per-container state during ingester operation.
type trackedContainer struct {
	info   containerInfo
	cancel context.CancelFunc
	since  time.Time
}

// ingester tails Docker container logs via the Docker Engine API.
type ingester struct {
	id           string
	client       dockerClient
	filter       *querylang.DNF
	pollInterval time.Duration
	stdout       bool
	stderr       bool
	stateFile    string
	logger       *slog.Logger

	mu         sync.Mutex
	containers map[string]*trackedContainer
	lastTS     map[string]time.Time // container ID -> last seen timestamp
}

// Run implements orchestrator.Ingester.
func (ing *ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	// Load bookmarks.
	st, err := loadState(ing.stateFile)
	if err != nil {
		ing.logger.Warn("failed to load state, starting fresh", "error", err)
		st = state{Containers: make(map[string]containerBookmark)}
	}

	ing.mu.Lock()
	for id, bm := range st.Containers {
		ing.lastTS[id] = bm.LastTimestamp
	}
	ing.mu.Unlock()

	// Retry connecting to Docker daemon.
	if err := ing.waitForDocker(ctx); err != nil {
		return err
	}

	var wg sync.WaitGroup

	// Initial container discovery.
	containers, err := ing.client.ContainerList(ctx)
	if err != nil {
		ing.logger.Warn("initial container list failed", "error", err)
	} else {
		for _, c := range containers {
			ing.startContainer(ctx, c, out, &wg)
		}
	}

	// Launch events listener.
	wg.Go(func() {
		ing.eventLoop(ctx, out, &wg)
	})

	// Launch poll ticker.
	if ing.pollInterval > 0 {
		wg.Go(func() {
			ing.pollLoop(ctx, out, &wg)
		})
	}

	// Wait for shutdown.
	<-ctx.Done()

	// Cancel all per-container contexts.
	ing.mu.Lock()
	for _, tc := range ing.containers {
		tc.cancel()
	}
	ing.mu.Unlock()

	wg.Wait()

	// Save bookmarks.
	ing.mu.Lock()
	for id, ts := range ing.lastTS {
		st.Containers[id] = containerBookmark{LastTimestamp: ts}
	}
	ing.mu.Unlock()

	if err := saveState(ing.stateFile, st); err != nil {
		ing.logger.Warn("failed to save state on shutdown", "error", err)
	}

	return nil
}

// waitForDocker retries connecting to the Docker daemon with backoff.
func (ing *ingester) waitForDocker(ctx context.Context) error {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		_, err := ing.client.ContainerList(ctx)
		if err == nil {
			ing.logger.Info("connected to Docker daemon")
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		ing.logger.Warn("Docker daemon not ready, retrying", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// startContainer begins streaming logs for a container if it matches filters
// and isn't already being tracked.
func (ing *ingester) startContainer(ctx context.Context, info containerInfo, out chan<- orchestrator.IngestMessage, wg *sync.WaitGroup) {
	if !querylang.MatchAttrs(ing.filter, containerAttrs(info)) {
		return
	}

	ing.mu.Lock()
	defer ing.mu.Unlock()

	if _, exists := ing.containers[info.ID]; exists {
		return
	}

	since := time.Now()
	if ts, ok := ing.lastTS[info.ID]; ok {
		// Resume 1ns after last seen to avoid duplicate.
		since = ts.Add(1)
	}

	cctx, cancel := context.WithCancel(ctx)
	tc := &trackedContainer{
		info:   info,
		cancel: cancel,
		since:  since,
	}
	ing.containers[info.ID] = tc

	logger := ing.logger
	wg.Go(func() {
		streamContainer(cctx, ing.client, info, since, ing.stdout, ing.stderr, ing.id, logger, out, ing.updateTimestamp)
		ing.mu.Lock()
		delete(ing.containers, info.ID)
		ing.mu.Unlock()
	})
}

// stopContainer cancels the log stream for a container.
func (ing *ingester) stopContainer(id string) {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	if tc, exists := ing.containers[id]; exists {
		tc.cancel()
		// The goroutine will remove itself from the map when it exits.
	}
}

// updateTimestamp records the last seen timestamp for a container.
func (ing *ingester) updateTimestamp(containerID string, ts time.Time) {
	ing.mu.Lock()
	defer ing.mu.Unlock()
	if existing, ok := ing.lastTS[containerID]; !ok || ts.After(existing) {
		ing.lastTS[containerID] = ts
	}
}

// eventLoop listens for Docker container events and starts/stops streams.
func (ing *ingester) eventLoop(ctx context.Context, out chan<- orchestrator.IngestMessage, wg *sync.WaitGroup) {
	backoff := 1 * time.Second

	for {
		events, errs := ing.client.Events(ctx)
		backoff = 1 * time.Second // Reset on successful connection.

		for {
			select {
			case <-ctx.Done():
				return

			case event, ok := <-events:
				if !ok {
					goto reconnect
				}
				ing.handleEvent(ctx, event, out, wg)

			case err, ok := <-errs:
				if !ok {
					goto reconnect
				}
				if ctx.Err() != nil {
					return
				}
				ing.logger.Warn("events stream error", "error", err)
				goto reconnect
			}
		}

	reconnect:
		if ctx.Err() != nil {
			return
		}
		ing.logger.Warn("events stream ended, reconnecting", "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// handleEvent processes a single Docker container event.
func (ing *ingester) handleEvent(ctx context.Context, event containerEvent, out chan<- orchestrator.IngestMessage, wg *sync.WaitGroup) {
	switch event.Action {
	case "start":
		// Inspect the container to get its full info.
		info, err := ing.client.ContainerInspect(ctx, event.ContainerID)
		if err != nil {
			ing.logger.Warn("failed to inspect container on start event", "container_id", event.ContainerID[:12], "error", err)
			return
		}
		ing.startContainer(ctx, info, out, wg)

	case "stop", "die", "destroy":
		ing.stopContainer(event.ContainerID)
	}
}

// pollLoop periodically discovers containers and starts new ones.
func (ing *ingester) pollLoop(ctx context.Context, out chan<- orchestrator.IngestMessage, wg *sync.WaitGroup) {
	ticker := time.NewTicker(ing.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			containers, err := ing.client.ContainerList(ctx)
			if err != nil {
				ing.logger.Warn("poll container list failed", "error", err)
				continue
			}
			for _, c := range containers {
				ing.startContainer(ctx, c, out, wg)
			}
		}
	}
}
