package docker

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/config"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// NewFactory returns an IngesterFactory for Docker container log ingesters.
// The config store is used to resolve certificate names for TLS.
func NewFactory(cfgStore config.Store) orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		cfg, err := parseConfig(id.String(), params, cfgStore, logger)
		if err != nil {
			return nil, err
		}
		return newIngester(cfg)
	}
}

// ingesterConfig holds parsed configuration for a Docker ingester.
type ingesterConfig struct {
	ID           string
	Host         string
	UseTLS       bool
	TLS          *clientTLSConfig
	Filter       containerFilter
	PollInterval time.Duration
	Stdout       bool
	Stderr       bool
	StateFile    string
	Logger       *slog.Logger
}

func parseConfig(id string, params map[string]string, cfgStore config.Store, logger *slog.Logger) (ingesterConfig, error) {
	host := params["host"]
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}

	// TLS: enabled by default for TCP connections.
	useTLS := true
	if v := params["tls"]; v == "false" {
		useTLS = false
	}

	// Resolve certificate names from config store.
	var tlsCfg *clientTLSConfig
	if useTLS {
		var err error
		tlsCfg, err = resolveTLS(id, params, cfgStore)
		if err != nil {
			return ingesterConfig{}, err
		}
	}

	// Filters.
	var f containerFilter
	if labelFilter := params["label_filter"]; labelFilter != "" {
		f.LabelKey, f.LabelValue = parseLabelFilter(labelFilter)
	}
	if nameFilter := params["name_filter"]; nameFilter != "" {
		re, err := regexp.Compile(nameFilter)
		if err != nil {
			return ingesterConfig{}, fmt.Errorf("docker ingester %q: invalid name_filter regex: %w", id, err)
		}
		f.NameRegex = re
	}
	if imageFilter := params["image_filter"]; imageFilter != "" {
		re, err := regexp.Compile(imageFilter)
		if err != nil {
			return ingesterConfig{}, fmt.Errorf("docker ingester %q: invalid image_filter regex: %w", id, err)
		}
		f.ImageRegex = re
	}

	// Poll interval.
	pollInterval := 30 * time.Second
	if v := params["poll_interval"]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return ingesterConfig{}, fmt.Errorf("docker ingester %q: invalid poll_interval %q: %w", id, v, err)
		}
		if d < 0 {
			return ingesterConfig{}, fmt.Errorf("docker ingester %q: poll_interval must be non-negative", id)
		}
		pollInterval = d
	}

	// Stdout/stderr.
	stdout := true
	if v := params["stdout"]; v == "false" {
		stdout = false
	}
	stderr := true
	if v := params["stderr"]; v == "false" {
		stderr = false
	}

	if !stdout && !stderr {
		return ingesterConfig{}, fmt.Errorf("docker ingester %q: at least one of stdout or stderr must be enabled", id)
	}

	// State file.
	var stateFile string
	if stateDir := params["_state_dir"]; stateDir != "" {
		stateFile = filepath.Join(stateDir, "state", "docker", id+".json")
	}

	return ingesterConfig{
		ID:           id,
		Host:         host,
		UseTLS:       useTLS,
		TLS:          tlsCfg,
		Filter:       f,
		PollInterval: pollInterval,
		Stdout:       stdout,
		Stderr:       stderr,
		StateFile:    stateFile,
		Logger:       logging.Default(logger).With("component", "ingester", "type", "docker", "instance", id),
	}, nil
}

// resolveTLS builds a clientTLSConfig by looking up named certificates from the
// config store. Params: tls_ca (CA cert name), tls_cert (client cert name), tls_verify.
func resolveTLS(id string, params map[string]string, cfgStore config.Store) (*clientTLSConfig, error) {
	caName := params["tls_ca"]
	certName := params["tls_cert"]
	if caName == "" && certName == "" {
		return nil, nil
	}

	verify := true
	if v := params["tls_verify"]; v == "false" {
		verify = false
	}

	ctx := context.Background()
	tlsCfg := &clientTLSConfig{Verify: verify}

	// We need to look up certificates by name. ListCertificates returns all
	// certs, and we find the one matching the requested name.
	var certs []config.CertPEM
	if caName != "" || certName != "" {
		var err error
		certs, err = cfgStore.ListCertificates(ctx)
		if err != nil {
			return nil, fmt.Errorf("docker ingester %q: list certificates: %w", id, err)
		}
	}

	if caName != "" {
		pem := findCertByName(certs, caName)
		if pem == nil {
			return nil, fmt.Errorf("docker ingester %q: CA certificate %q not found", id, caName)
		}
		tlsCfg.CAPem = pem.CertPEM
		tlsCfg.CAFile = pem.CertFile
	}

	if certName != "" {
		pem := findCertByName(certs, certName)
		if pem == nil {
			return nil, fmt.Errorf("docker ingester %q: client certificate %q not found", id, certName)
		}
		tlsCfg.CertPem = pem.CertPEM
		tlsCfg.KeyPem = pem.KeyPEM
		tlsCfg.CertFile = pem.CertFile
		tlsCfg.KeyFile = pem.KeyFile
	}

	return tlsCfg, nil
}

// findCertByName returns the first certificate with the given name, or nil if not found.
func findCertByName(certs []config.CertPEM, name string) *config.CertPEM {
	for i := range certs {
		if certs[i].Name == name {
			return &certs[i]
		}
	}
	return nil
}

// TestConnection creates a temporary Docker client from the given params,
// pings the daemon, and lists containers. Returns a human-readable summary.
func TestConnection(ctx context.Context, params map[string]string, cfgStore config.Store) (string, error) {
	host := params["host"]
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}

	useTLS := true
	if v := params["tls"]; v == "false" {
		useTLS = false
	}

	var tlsCfg *clientTLSConfig
	if useTLS {
		var err error
		tlsCfg, err = resolveTLS("_test", params, cfgStore)
		if err != nil {
			return "", fmt.Errorf("resolve TLS: %w", err)
		}
	}

	client, err := newSDKDockerClient(host, useTLS, tlsCfg)
	if err != nil {
		return "", fmt.Errorf("create client: %w", err)
	}

	version, err := client.Ping(ctx)
	if err != nil {
		return "", err
	}

	containers, err := client.ContainerList(ctx)
	if err != nil {
		return "", fmt.Errorf("list containers: %w", err)
	}

	return fmt.Sprintf("Connected — Docker %s, %d containers running", version, len(containers)), nil
}

func newIngester(cfg ingesterConfig) (*ingester, error) {
	client, err := newSDKDockerClient(cfg.Host, cfg.UseTLS, cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("docker ingester %q: %w", cfg.ID, err)
	}
	return newIngesterWithClient(cfg, client), nil
}

// newIngesterWithClient creates an ingester with a provided client (for testing).
func newIngesterWithClient(cfg ingesterConfig, client dockerClient) *ingester {
	return &ingester{
		id:           cfg.ID,
		client:       client,
		filter:       cfg.Filter,
		pollInterval: cfg.PollInterval,
		stdout:       cfg.Stdout,
		stderr:       cfg.Stderr,
		stateFile:    cfg.StateFile,
		logger:       cfg.Logger,
		containers:   make(map[string]*trackedContainer),
		lastTS:       make(map[string]time.Time),
	}
}
