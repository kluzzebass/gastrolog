package docker

import (
	"context"
	"log/slog"
	"maps"
	"time"

	"gastrolog/internal/orchestrator"
)

// containerAttrs flattens container metadata into a map for querylang filter matching.
// Keys: "name", "image", "label.<key>" for each Docker label.
func containerAttrs(info containerInfo) map[string]string {
	attrs := make(map[string]string, 2+len(info.Labels))
	attrs["name"] = info.Name
	attrs["image"] = info.Image
	for k, v := range info.Labels {
		attrs["label."+k] = v
	}
	return attrs
}

// streamContainer runs a log stream for a single container, emitting messages
// to the output channel. It blocks until the context is cancelled or the stream
// ends. It handles reconnection with backoff on stream errors.
func streamContainer(
	ctx context.Context,
	client dockerClient,
	info containerInfo,
	since time.Time,
	stdout, stderr bool,
	ingesterID string,
	logger *slog.Logger,
	out chan<- orchestrator.IngestMessage,
	onTimestamp func(containerID string, ts time.Time),
) {
	logger = logger.With("container_id", info.ID[:12], "container_name", info.Name)
	logger.Info("starting container log stream")

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		err := streamOnce(ctx, client, info, since, stdout, stderr, ingesterID, logger, out, onTimestamp, &since)
		if ctx.Err() != nil {
			logger.Info("container log stream stopped")
			return
		}
		if err != nil {
			logger.Warn("container log stream error, reconnecting", "error", err, "backoff", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, maxBackoff)
	}
}

// streamOnce opens a single log stream and reads until EOF or error.
func streamOnce(
	ctx context.Context,
	client dockerClient,
	info containerInfo,
	since time.Time,
	stdout, stderr bool,
	ingesterID string,
	logger *slog.Logger,
	out chan<- orchestrator.IngestMessage,
	onTimestamp func(containerID string, ts time.Time),
	lastTS *time.Time,
) error {
	body, isTTY, err := client.ContainerLogs(ctx, info.ID, since, true, stdout, stderr)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	entries := make(chan logEntry, 64)
	streamErr := make(chan error, 1)

	go func() {
		defer close(entries)
		if isTTY || info.IsTTY {
			streamErr <- readRaw(body, entries)
		} else {
			streamErr <- readMultiplexed(body, entries)
		}
	}()

	attrs := map[string]string{
		"ingester_type":  "docker",
		"container_id":   info.ID,
		"container_name": info.Name,
		"image":          info.Image,
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case entry, ok := <-entries:
			if !ok {
				// Stream ended, check for error.
				select {
				case err := <-streamErr:
					return err
				default:
					return nil
				}
			}

			msgAttrs := make(map[string]string, len(attrs)+1)
			maps.Copy(msgAttrs, attrs)
			msgAttrs["stream"] = entry.Stream

			msg := orchestrator.IngestMessage{
				Attrs:      msgAttrs,
				Raw:        entry.Line,
				IngesterID: ingesterID,
			}
			if !entry.Timestamp.IsZero() {
				msg.IngestTS = entry.Timestamp
				*lastTS = entry.Timestamp
				if onTimestamp != nil {
					onTimestamp(info.ID, entry.Timestamp)
				}
			} else {
				msg.IngestTS = time.Now()
			}

			select {
			case out <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
