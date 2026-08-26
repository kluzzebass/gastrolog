package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"gastrolog/internal/auth"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// Initial admin user provisioning.
//
// Container orchestrators (Docker Compose, Kubernetes) need a way to
// inject the initial admin credentials at boot without an interactive
// step. The first-access UI screen is preserved as the fallback for
// human-driven deployments; this code path only activates when the
// operator opts in by configuring InitialAdminFile or InitialAdminUser/
// InitialAdminPassword.
//
// Precedence:
//   1. InitialAdminFile (mounted secret / configmap volume)
//   2. InitialAdminUser + InitialAdminPassword (env vars)
//   3. Neither set → no-op; first-access UI handles it.
//
// Idempotency: provisioning is "create if missing." Once any user
// exists in the cluster, both sources become no-ops on every
// subsequent restart, so a Secret left in place doesn't overwrite
// the operator's password changes.

const (
	// initialAdminCredsMaxBytes bounds the size of the credentials file
	// to keep memory bounded against a hostile or misconfigured source.
	// Real credentials are tens of bytes; 4 KiB is generous enough for
	// JSON formatting plus operator comments while still bounded.
	initialAdminCredsMaxBytes = 4096
)

// adminUsernameRe matches the same pattern enforced by AuthServer.Register
// — kept in sync so the file/env path produces creds that would also
// pass through the interactive UI flow. Duplicated here rather than
// imported to avoid a server→app dependency.
var adminUsernameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{3,64}$`)

// initialAdminCreds is the file format for InitialAdminFile.
//
// Two encodings are accepted:
//   - JSON object: {"username": "admin", "password": "..."}
//   - Single line: "username:password"
//
// The JSON form is friendlier to K8s Secrets (which can be projected
// as JSON) and operator scripting. The colon form is convenient for
// quick `echo` setups. Whichever form the file uses, the result is
// the same struct.
type initialAdminCreds struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// provisionInitialAdmin creates an initial admin user from the file
// or env source if (a) provisioning is configured and (b) no users
// exist yet in the cluster. Joiners (JoinAddr non-empty) skip
// provisioning entirely — only the bootstrap node creates users.
//
// Errors from provisioning fail the bootstrap explicitly: a
// misconfigured Secret (empty file, malformed JSON, password too
// short) should surface immediately rather than silently dropping
// the operator into the first-access flow.
func provisionInitialAdmin(ctx context.Context, cfgStore system.Store, cfg RunConfig, logger *slog.Logger) error {
	if cfg.JoinAddr != "" {
		return nil // joiners never provision
	}
	if cfg.InitialAdminFile == "" && cfg.InitialAdminUser == "" && cfg.InitialAdminPassword == "" {
		return nil // not configured
	}

	count, err := cfgStore.CountUsers(ctx)
	if err != nil {
		return fmt.Errorf("count users: %w", err)
	}
	if count > 0 {
		logger.Info("initial admin: skipping (users already exist)")
		return nil
	}

	creds, source, err := loadInitialAdminCreds(cfg)
	if err != nil {
		return fmt.Errorf("load initial admin credentials: %w", err)
	}

	if err := validateInitialAdmin(creds); err != nil {
		return fmt.Errorf("validate initial admin: %w", err)
	}

	hash, err := auth.HashPassword(creds.Password)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	now := time.Now().UTC()
	user := system.User{
		ID:           glid.New(),
		Username:     creds.Username,
		PasswordHash: hash,
		Role:         "admin",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := cfgStore.CreateUser(ctx, user); err != nil {
		return fmt.Errorf("create initial admin user: %w", err)
	}

	logger.Info("initial admin user provisioned", "username", creds.Username, "source", source)
	return nil
}

// loadInitialAdminCreds resolves credentials from the configured
// source. Returns the creds plus a short label ("file" or "env") for
// the success log line so operators can confirm which path was used.
func loadInitialAdminCreds(cfg RunConfig) (initialAdminCreds, string, error) {
	if cfg.InitialAdminFile != "" {
		creds, err := readInitialAdminFile(cfg.InitialAdminFile)
		if err != nil {
			return initialAdminCreds{}, "", err
		}
		return creds, "file", nil
	}
	if cfg.InitialAdminUser == "" || cfg.InitialAdminPassword == "" {
		return initialAdminCreds{}, "", errors.New("InitialAdminUser and InitialAdminPassword must both be set when not using InitialAdminFile")
	}
	return initialAdminCreds{
		Username: cfg.InitialAdminUser,
		Password: cfg.InitialAdminPassword,
	}, "env", nil
}

// readInitialAdminFile reads the credentials file with a bounded
// read and parses it as JSON or "user:password" depending on the
// first byte. Whitespace around the credentials (newlines, leading
// whitespace) is trimmed.
func readInitialAdminFile(path string) (initialAdminCreds, error) {
	// G304: path is operator-supplied via --initial-admin-file; the
	// open IS reading the configured location, by design.
	f, err := os.Open(path) //nolint:gosec // G304: path is operator config, intentional
	if err != nil {
		return initialAdminCreds{}, err
	}
	defer func() { _ = f.Close() }()

	var buf [initialAdminCredsMaxBytes]byte
	n, err := io.ReadFull(f, buf[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return initialAdminCreds{}, err
	}
	body := strings.TrimSpace(string(buf[:n]))
	if body == "" {
		return initialAdminCreds{}, errors.New("file is empty")
	}

	if strings.HasPrefix(body, "{") {
		var creds initialAdminCreds
		if err := json.Unmarshal([]byte(body), &creds); err != nil {
			return initialAdminCreds{}, fmt.Errorf("parse JSON: %w", err)
		}
		return creds, nil
	}
	// "user:password" format. Split on first colon — passwords can
	// contain colons, but usernames cannot per adminUsernameRe.
	user, pass, ok := strings.Cut(body, ":")
	if !ok {
		return initialAdminCreds{}, errors.New(`expected "username:password" or JSON {"username": ..., "password": ...}`)
	}
	return initialAdminCreds{
		Username: user,
		Password: pass,
	}, nil
}

// validateInitialAdmin enforces the same username + password shape
// rules the interactive Register handler enforces. Keeping the rules
// in lockstep means an operator who provisions via env or file can't
// land creds that would have been rejected from the UI.
func validateInitialAdmin(creds initialAdminCreds) error {
	if !adminUsernameRe.MatchString(creds.Username) {
		return errors.New("username must be 3-64 characters, alphanumeric, underscores, or hyphens")
	}
	// Use the auth package's policy validation so password rules stay
	// in lockstep with whatever the operator has configured for the
	// cluster. At bootstrap there's no policy config yet, so we use
	// a conservative built-in default: minimum 8 characters.
	if len(creds.Password) < 8 {
		return errors.New("password must be at least 8 characters")
	}
	return nil
}
