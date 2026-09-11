package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gastrolog/internal/orchestrator"
)

func cspDirectives(t *testing.T, csp string) map[string]string {
	t.Helper()
	if csp == "" {
		t.Fatal("no Content-Security-Policy header")
	}
	directives := map[string]string{}
	for _, d := range strings.Split(csp, ";") {
		name, value, _ := strings.Cut(strings.TrimSpace(d), " ")
		directives[name] = value
	}
	return directives
}

func serveThroughSecurityHeaders(t *testing.T) http.Header {
	t.Helper()
	h := securityHeadersMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Result().Header
}

func TestSecurityHeadersSetsBaseline(t *testing.T) {
	got := serveThroughSecurityHeaders(t)
	for name, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "strict-origin-when-cross-origin",
	} {
		if got.Get(name) != want {
			t.Errorf("%s = %q, want %q", name, got.Get(name), want)
		}
	}
}

// A CSP that allows inline script would not stop the injection vectors it is
// here for: an onerror= attribute on log-derived markup, or a javascript: URL.
func TestContentSecurityPolicyForbidsInlineScript(t *testing.T) {
	csp := serveThroughSecurityHeaders(t).Get("Content-Security-Policy")
	directives := cspDirectives(t, csp)

	script, ok := directives["script-src"]
	if !ok {
		t.Fatalf("no script-src directive in %q", csp)
	}
	if strings.Contains(script, "unsafe-inline") || strings.Contains(script, "unsafe-eval") {
		t.Errorf("script-src = %q, must allow neither unsafe-inline nor unsafe-eval", script)
	}
	if directives["default-src"] != "'self'" {
		t.Errorf("default-src = %q, want 'self'", directives["default-src"])
	}
	if directives["object-src"] != "'none'" {
		t.Errorf("object-src = %q, want 'none'", directives["object-src"])
	}
	if directives["base-uri"] != "'self'" {
		t.Errorf("base-uri = %q, want 'self'", directives["base-uri"])
	}
}

// The binary serves its own typefaces, so the policy names no origin but
// this one. An allowance added back here would be a third party in the
// runtime of a UI that no longer needs one, and the fonts would still load,
// so nothing else would notice.
func TestContentSecurityPolicyNamesNoExternalOrigin(t *testing.T) {
	directives := cspDirectives(t, serveThroughSecurityHeaders(t).Get("Content-Security-Policy"))

	for name, value := range directives {
		if strings.Contains(value, "//") {
			t.Errorf("%s = %q names an external origin; the UI is served entirely by this binary",
				name, value)
		}
	}
	if got := directives["font-src"]; got != "'self'" {
		t.Errorf("font-src = %q, want 'self' — the typefaces ship with the binary", got)
	}
	// A face small enough for the bundler to inline would need data: here,
	// which is why the build is configured to keep fonts as files.
	if strings.Contains(directives["font-src"], "data:") {
		t.Error("font-src allows data:, so some face is being inlined rather than served as a file")
	}
}

// The policy is worthless if the middleware chain stops covering the path that
// serves the app. Status is not asserted: the embedded dist is a build
// artifact, so "/" is unregistered in a source checkout and 404s. The header
// is set before the mux runs either way, which is the ordering being pinned.
func TestSPAPathServesThroughSecurityHeaders(t *testing.T) {
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(orch, nil, orchestrator.Factories{}, nil, Config{})

	rec := httptest.NewRecorder()
	srv.wrapMiddleware(srv.buildMux()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := rec.Result().Header.Get("Content-Security-Policy"); got != contentSecurityPolicy {
		t.Errorf("Content-Security-Policy on the SPA path = %q, want %q", got, contentSecurityPolicy)
	}
}
