package docker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	dockerclient "github.com/docker/docker/client"
)

// containerInfo holds metadata about a Docker container.
type containerInfo struct {
	ID     string
	Name   string
	Image  string
	Labels map[string]string
	IsTTY  bool
}

// containerEvent represents a Docker container lifecycle event.
type containerEvent struct {
	Action      string // "start", "stop", "die", "destroy"
	ContainerID string
}

// dockerClient abstracts Docker Engine API interactions.
type dockerClient interface {
	ContainerList(ctx context.Context) ([]containerInfo, error)
	ContainerLogs(ctx context.Context, id string, since time.Time, follow bool, stdout, stderr bool) (io.ReadCloser, bool, error)
	Events(ctx context.Context) (<-chan containerEvent, <-chan error)
	ContainerInspect(ctx context.Context, id string) (containerInfo, error)
	Ping(ctx context.Context) (string, error)
}

// sdkDockerClient implements dockerClient using the official Docker SDK.
type sdkDockerClient struct {
	cli *dockerclient.Client
}

// newSDKDockerClient creates a Docker client using the official SDK.
// It supports unix sockets and TCP connections with optional TLS.
func newSDKDockerClient(host string, useTLS bool, tlsCfg *clientTLSConfig) (*sdkDockerClient, error) {
	opts := []dockerclient.Opt{
		dockerclient.WithHost(host),
		dockerclient.WithAPIVersionNegotiation(),
	}

	// For TCP with TLS, build an http.Client with our custom TLS config
	// so we can use PEM certs from the gastrolog certificate store.
	if useTLS && strings.HasPrefix(host, "tcp://") {
		tc, err := buildTLSConfig(tlsCfg)
		if err != nil {
			return nil, err
		}
		if tc == nil {
			tc = &tls.Config{}
		}
		httpClient := &http.Client{
			Transport: &http.Transport{TLSClientConfig: tc},
		}
		opts = append(opts, dockerclient.WithHTTPClient(httpClient), dockerclient.WithScheme("https"))
	}

	cli, err := dockerclient.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &sdkDockerClient{cli: cli}, nil
}

func (c *sdkDockerClient) ContainerList(ctx context.Context) ([]containerInfo, error) {
	raw, err := c.cli.ContainerList(ctx, container.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("container list: %w", err)
	}

	containers := make([]containerInfo, len(raw))
	for i, r := range raw {
		name := ""
		if len(r.Names) > 0 {
			name = strings.TrimPrefix(r.Names[0], "/")
		}
		containers[i] = containerInfo{
			ID:     r.ID,
			Name:   name,
			Image:  r.Image,
			Labels: r.Labels,
		}
	}
	return containers, nil
}

func (c *sdkDockerClient) ContainerInspect(ctx context.Context, id string) (containerInfo, error) {
	raw, err := c.cli.ContainerInspect(ctx, id)
	if err != nil {
		return containerInfo{}, fmt.Errorf("container inspect: %w", err)
	}

	isTTY := false
	image := ""
	var labels map[string]string
	if raw.Config != nil {
		isTTY = raw.Config.Tty
		image = raw.Config.Image
		labels = raw.Config.Labels
	}

	return containerInfo{
		ID:     raw.ID,
		Name:   strings.TrimPrefix(raw.Name, "/"),
		Image:  image,
		Labels: labels,
		IsTTY:  isTTY,
	}, nil
}

func (c *sdkDockerClient) ContainerLogs(ctx context.Context, id string, since time.Time, follow bool, stdout, stderr bool) (io.ReadCloser, bool, error) {
	// The SDK doesn't expose the Content-Type header that indicates TTY mode,
	// so we inspect the container first to determine the stream format.
	info, err := c.cli.ContainerInspect(ctx, id)
	if err != nil {
		return nil, false, fmt.Errorf("inspect for logs: %w", err)
	}
	isTTY := info.Config != nil && info.Config.Tty

	opts := container.LogsOptions{
		ShowStdout: stdout,
		ShowStderr: stderr,
		Timestamps: true,
		Follow:     follow,
	}
	if !since.IsZero() {
		opts.Since = fmt.Sprintf("%d.%09d", since.Unix(), since.Nanosecond())
	}

	body, err := c.cli.ContainerLogs(ctx, id, opts)
	if err != nil {
		return nil, false, fmt.Errorf("container logs: %w", err)
	}

	return body, isTTY, nil
}

func (c *sdkDockerClient) Events(ctx context.Context) (<-chan containerEvent, <-chan error) {
	eventFilter := filters.NewArgs(
		filters.Arg("type", string(events.ContainerEventType)),
		filters.Arg("event", string(events.ActionStart)),
		filters.Arg("event", string(events.ActionStop)),
		filters.Arg("event", string(events.ActionDie)),
		filters.Arg("event", string(events.ActionDestroy)),
	)

	msgCh, errCh := c.cli.Events(ctx, events.ListOptions{
		Filters: eventFilter,
	})

	out := make(chan containerEvent)
	outErr := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(outErr)

		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				select {
				case out <- containerEvent{
					Action:      string(msg.Action),
					ContainerID: msg.Actor.ID,
				}:
				case <-ctx.Done():
					return
				}

			case err, ok := <-errCh:
				if !ok {
					return
				}
				if ctx.Err() != nil {
					return
				}
				outErr <- fmt.Errorf("events: %w", err)
				return
			}
		}
	}()

	return out, outErr
}

// Ping calls the Docker server version endpoint and returns the version string.
func (c *sdkDockerClient) Ping(ctx context.Context) (string, error) {
	ver, err := c.cli.ServerVersion(ctx)
	if err != nil {
		return "", fmt.Errorf("docker ping: %w", err)
	}
	return ver.Version, nil
}

// clientTLSConfig holds resolved TLS material for connecting to a Docker daemon.
// Values come from the gastrolog certificate store (PEM content or file paths).
type clientTLSConfig struct {
	// CA certificate (PEM content or file path, not both).
	CAPem  string
	CAFile string

	// Client certificate and key (PEM content or file paths).
	CertPem  string
	KeyPem   string
	CertFile string
	KeyFile  string

	Verify bool
}

func (c *clientTLSConfig) empty() bool {
	return c.CAPem == "" && c.CAFile == "" &&
		c.CertPem == "" && c.KeyPem == "" &&
		c.CertFile == "" && c.KeyFile == ""
}

func buildTLSConfig(cfg *clientTLSConfig) (*tls.Config, error) {
	if cfg == nil || cfg.empty() {
		return nil, nil
	}

	tc := &tls.Config{
		InsecureSkipVerify: !cfg.Verify, //nolint:gosec // G402: user-configurable TLS verification for Docker socket connections
	}

	// Load CA certificate.
	if cfg.CAPem != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(cfg.CAPem)) {
			return nil, errors.New("CA certificate contains no valid PEM data")
		}
		tc.RootCAs = pool
	} else if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("CA file contains no valid certificates")
		}
		tc.RootCAs = pool
	}

	// Load client certificate and key.
	if cfg.CertPem != "" && cfg.KeyPem != "" {
		cert, err := tls.X509KeyPair([]byte(cfg.CertPem), []byte(cfg.KeyPem))
		if err != nil {
			return nil, fmt.Errorf("parse client cert/key PEM: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	} else if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}

	return tc, nil
}
