package app

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/system"
)

// Non-interactive cluster bootstrap.
//
// The cluster's bootstrap node generates a join token at startup. An
// attended terminal can read it off stderr, but orchestrators (Docker
// Compose, Kubernetes StatefulSets) have no human reading logs, so this
// file provides two operator-driven token-delivery paths a joiner can
// use without log-scraping:
//
//   1. File-based (default, lowest-friction): the bootstrap node writes
//      the token atomically to a known path; joiners read from the
//      same path via a shared volume / mounted secret.
//   2. Endpoint-based (opt-in): the bootstrap node serves the token
//      from a small HTTP endpoint, gated on a shared secret; joiners
//      poll that URL.
//
// Both paths layer on top of the existing --join-token literal flag,
// which remains the lowest-level escape hatch.

const (
	// bootstrapTokenFileMode keeps the on-disk token readable only by
	// the owner — same precaution as cluster TLS material.
	bootstrapTokenFileMode os.FileMode = 0o600

	// bootstrapTokenInitialBackoff is the starting poll interval when
	// reading from a file or URL. Doubles up to bootstrapTokenMaxBackoff.
	bootstrapTokenInitialBackoff = 1 * time.Second
	bootstrapTokenMaxBackoff     = 30 * time.Second

	// bootstrapTokenDefaultTimeout caps how long a joiner waits for the
	// token to appear before giving up. Long enough to cover real
	// orchestration delays (image pulls, init containers, leader
	// election on the bootstrap node), short enough to not hang a pod
	// indefinitely when something is genuinely misconfigured.
	bootstrapTokenDefaultTimeout = 10 * time.Minute

	// bootstrapTokenSecretHeader is the request header carrying the
	// shared secret on the joiner's GET request. Compared to the
	// stored secret with constant-time equality on the bootstrap side.
	bootstrapTokenSecretHeader = "X-Bootstrap-Token-Secret" //nolint:gosec // G101: header name, not a credential

	// maxBootstrapTokenBytes bounds reads from both the file and HTTP
	// sources. Real join tokens are <200 bytes (hex-secret + ":" +
	// hex-sha256). 1 KiB gives operators room to put a token plus a
	// trailing newline / minor whitespace, while bounding memory and
	// preventing a hostile or misconfigured source from poisoning
	// joiners with arbitrary bytes.
	maxBootstrapTokenBytes = 1024
)

// readTokenFile reads up to maxBootstrapTokenBytes from path and
// returns the trimmed contents. Distinguishes "not present" from
// "unreadable" via os.ErrNotExist passthrough. The bounded read keeps
// a hostile or misconfigured source from forcing the joiner to
// allocate arbitrary memory just to discover the token is malformed.
func readTokenFile(path string) (string, error) {
	// G304: path is operator-supplied via --bootstrap-token-file; the
	// open IS reading the configured location, by design.
	f, err := os.Open(path) //nolint:gosec // G304: path is operator config, intentional
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	var buf [maxBootstrapTokenBytes]byte
	n, err := io.ReadFull(f, buf[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(string(buf[:n])), nil
}

// fetchBootstrapTokenWithRetry GETs the bootstrap token from url with
// exponential backoff until success or ctx cancellation. Sends the
// shared secret in the X-Bootstrap-Token-Secret header.
//
// Distinguishes "endpoint not yet up" (network errors, 5xx, 404) from
// "auth failure" (401/403) — the latter is a config error that won't
// resolve with retries, so we surface it immediately. Anything else
// is treated as transient and retried.
func fetchBootstrapTokenWithRetry(ctx context.Context, url, secret string, logger *slog.Logger) (string, error) {
	if secret == "" {
		return "", errors.New("bootstrap token URL set but secret is empty")
	}
	backoff := bootstrapTokenInitialBackoff
	logger.Info("waiting for bootstrap token endpoint", "url", url)
	client := &http.Client{Timeout: 10 * time.Second}
	for {
		token, fatal, err := fetchTokenOnce(ctx, client, url, secret)
		if err == nil {
			logger.Info("bootstrap token loaded from URL", "url", url)
			return token, nil
		}
		if fatal {
			return "", err
		}
		logger.Debug("bootstrap token fetch transient error, retrying", "url", url, "error", err)
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("bootstrap token URL %q never returned a token: %w", url, ctx.Err())
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > bootstrapTokenMaxBackoff {
			backoff = bootstrapTokenMaxBackoff
		}
	}
}

// fetchTokenOnce performs a single GET. Returns (token, fatal, err):
// fatal=true means "stop retrying, this is a config problem" (e.g.
// 401/403). Anything else is transient.
func fetchTokenOnce(ctx context.Context, client *http.Client, url, secret string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", true, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set(bootstrapTokenSecretHeader, secret)
	// url is operator-supplied via --bootstrap-token-url; the fetch IS
	// targeting the configured endpoint, by design.
	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = resp.Body.Close() }()
	// Bounded read — the response is a tiny ASCII token; cap at
	// maxBootstrapTokenBytes so a hostile or misconfigured server can't
	// force unbounded allocations on the joiner.
	var buf [maxBootstrapTokenBytes]byte
	n, _ := io.ReadFull(resp.Body, buf[:])
	body := buf[:n]
	switch resp.StatusCode {
	case http.StatusOK:
		token := strings.TrimSpace(string(body))
		if token == "" {
			return "", false, errors.New("endpoint returned empty body")
		}
		return token, false, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", true, fmt.Errorf("bootstrap token endpoint rejected secret: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	default:
		return "", false, fmt.Errorf("bootstrap token endpoint returned %d", resp.StatusCode)
	}
}

// makeBootstrapTokenFn returns a closure that mints a join token from the
// cluster's key. The HTTP endpoint at /cluster/bootstrap-token calls this on
// each authorized request, so a joiner fetching one at its own startup always
// gets a token whose window has not started closing yet.
//
// Minting per request is what makes expiry workable. A standing token would
// have to outlive every join that will ever happen, which is the same as not
// expiring; one minted at the moment of asking only has to survive the next
// few seconds.
func makeBootstrapTokenFn(cfgStore system.Store) func() (string, error) {
	return func() (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sys, err := cfgStore.Load(ctx)
		if err != nil {
			return "", fmt.Errorf("load config: %w", err)
		}
		if sys == nil || sys.Runtime.ClusterTLS == nil || sys.Runtime.ClusterTLS.JoinTokenKey == "" {
			return "", errors.New("cluster TLS not initialized")
		}
		key, err := hex.DecodeString(sys.Runtime.ClusterTLS.JoinTokenKey)
		if err != nil {
			return "", fmt.Errorf("decode join token key: %w", err)
		}
		return tlsutil.MintJoinToken(key, []byte(sys.Runtime.ClusterTLS.CACertPEM), tlsutil.DefaultJoinTokenTTL)
	}
}

// resolveJoinTokenFromSources populates cfg.JoinToken when the operator
// supplied a delivery path instead of the literal token. Precedence:
//  1. cfg.JoinToken (literal) — wins if set
//  2. cfg.BootstrapTokenURL  — endpoint-based
//
// There is deliberately no file-drop path. A token written to a shared volume
// at bootstrap is read by joiners that start arbitrarily later, so it would
// have to outlive every join the cluster will ever see — which is the same as
// not expiring. The endpoint mints one per request instead, so a joiner always
// gets a fresh one however long after bootstrap it starts.
//
// Returns nil with cfg unmodified when no source applies (single-node
// or attended bootstrap with the literal flag).
func resolveJoinTokenFromSources(ctx context.Context, cfg *RunConfig, logger *slog.Logger) error {
	if cfg.JoinToken != "" {
		return nil
	}
	if cfg.BootstrapTokenURL == "" {
		return nil
	}
	if cfg.JoinAddr == "" {
		return errors.New("bootstrap-token-url set without --join-addr")
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, bootstrapTokenDefaultTimeout)
	defer cancel()
	token, err := fetchBootstrapTokenWithRetry(timeoutCtx, cfg.BootstrapTokenURL, cfg.BootstrapTokenSecret, logger)
	if err != nil {
		return err
	}
	cfg.JoinToken = token
	return nil
}
