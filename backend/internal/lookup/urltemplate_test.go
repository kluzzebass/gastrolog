package lookup

import (
	"errors"
	"net/url"
	"testing"
)

func TestParseURLTemplateRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"placeholder in host", "http://{tenant}.geo.internal/lookup"},
		{"placeholder is the host", "http://{value}/lookup"},
		{"placeholder in port", "http://api.example.com:{port}/lookup"},
		{"placeholder in userinfo", "http://{user}:pw@api.example.com/lookup"},
		{"no scheme", "api.example.com/{value}"},
		{"file scheme", "file:///etc/{value}"},
		{"no host", "http:///{value}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateURLTemplate(tt.raw); err == nil {
				t.Fatalf("ValidateURLTemplate(%q) = nil, want an error", tt.raw)
			}
		})
	}
}

func TestParseURLTemplateAccepts(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"http://api.example.com/users/{value}",
		"https://api.example.com:8443/users/{value}/info",
		"https://user:pw@api.example.com/users/{value}",
		"http://api.example.com/weather?lat={lat}&lon={lon}",
		"https://api.example.com/lookup",
	} {
		if err := ValidateURLTemplate(raw); err != nil {
			t.Errorf("ValidateURLTemplate(%q) = %v, want nil", raw, err)
		}
	}
}

// TestExpandCannotChangeAuthority covers the values a log record can carry into
// a substitution: none of them may move the request off the configured host.
func TestExpandCannotChangeAuthority(t *testing.T) {
	t.Parallel()

	const host = "api.example.com"
	tests := []struct {
		name     string
		template string
		values   map[string]string
	}{
		{"userinfo hijack", "http://" + host + "/users/{value}", map[string]string{"value": "trusted@evil.com"}},
		{"port hijack", "http://" + host + "/users/{value}", map[string]string{"value": "x:8080"}},
		{"absolute url", "http://" + host + "/users/{value}", map[string]string{"value": "http://evil/"}},
		{"protocol relative", "http://" + host + "/users/{value}", map[string]string{"value": "//evil/"}},
		{"path traversal", "http://" + host + "/users/{value}", map[string]string{"value": "../../evil"}},
		{"query injection", "http://" + host + "/users?u={value}", map[string]string{"value": "a&admin=true#x"}},
		{"fragment escape", "http://" + host + "/users/{value}", map[string]string{"value": "a#@evil.com/"}},
		{"newline", "http://" + host + "/users/{value}", map[string]string{"value": "a\r\nHost: evil"}},
		{"empty", "http://" + host + "/users/{value}", map[string]string{"value": ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpl, err := parseURLTemplate(tt.template)
			if err != nil {
				t.Fatalf("parseURLTemplate: %v", err)
			}
			raw, err := tmpl.expand(tt.values)
			if err != nil {
				if errors.Is(err, errTemplateAuthority) {
					return // refused outright, which is also a safe outcome
				}
				t.Fatalf("expand: %v", err)
			}
			u, err := url.Parse(raw)
			if err != nil {
				t.Fatalf("parse %q: %v", raw, err)
			}
			if u.Scheme != "http" || u.Host != host || u.User != nil {
				t.Fatalf("expand(%v) = %q, which requests %s://%s (userinfo %v)", tt.values, raw, u.Scheme, u.Host, u.User)
			}
		})
	}
}

func TestExpandSubstitutesValue(t *testing.T) {
	t.Parallel()

	tmpl, err := parseURLTemplate("http://api.example.com/users/{value}?fmt=json&q={value}")
	if err != nil {
		t.Fatalf("parseURLTemplate: %v", err)
	}
	raw, err := tmpl.expand(map[string]string{"value": "a b/c"})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	if got := u.EscapedPath(); got != "/users/a%20b%2Fc" {
		t.Errorf("path = %q, want the value escaped into a single segment", got)
	}
	if got := u.Query().Get("q"); got != "a b/c" {
		t.Errorf("query q = %q, want the value round-tripped", got)
	}
}
