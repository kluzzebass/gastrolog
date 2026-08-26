package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
	if csp == "" {
		t.Fatal("no Content-Security-Policy header")
	}

	directives := map[string]string{}
	for _, d := range strings.Split(csp, ";") {
		name, value, _ := strings.Cut(strings.TrimSpace(d), " ")
		directives[name] = value
	}

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
