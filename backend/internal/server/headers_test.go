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

// The frontend stylesheet @imports the Observatory typefaces from Google
// Fonts, which then serves the woff2 files from a second origin. A policy that
// omits either one does not fail loudly — the UI just renders in system fonts.
func TestContentSecurityPolicyAllowsTheWebfontOrigins(t *testing.T) {
	directives := cspDirectives(t, serveThroughSecurityHeaders(t).Get("Content-Security-Policy"))

	if !strings.Contains(directives["style-src"], fontStylesheetOrigin) {
		t.Errorf("style-src = %q, must allow %s or the font stylesheet is blocked",
			directives["style-src"], fontStylesheetOrigin)
	}
	if !strings.Contains(directives["font-src"], fontFileOrigin) {
		t.Errorf("font-src = %q, must allow %s or the woff2 files are blocked",
			directives["font-src"], fontFileOrigin)
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
