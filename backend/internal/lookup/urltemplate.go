package lookup

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"gastrolog/internal/safefetch"
)

// errTemplateAuthority reports a substitution that would have changed where the
// request goes rather than what it asks for.
var errTemplateAuthority = errors.New("substituted value changes the URL authority")

// urlTemplate is a lookup URL split at its authority. Placeholders may appear
// only after the authority, so scheme, userinfo, host and port are fixed when
// the lookup is configured and no substituted value can move the request to
// another destination. Every expansion is re-parsed and compared against the
// template's authority, because escaping alone is not proof.
type urlTemplate struct {
	authority string // "scheme://[userinfo@]host[:port]", placeholder-free
	path      string // template text from the path up to the query or fragment
	tail      string // template text from the first "?" or "#" onward, "" if neither
	base      *url.URL
}

// ValidateURLTemplate reports whether raw is usable as a lookup URL template.
func ValidateURLTemplate(raw string) error {
	_, err := parseURLTemplate(raw)
	return err
}

func parseURLTemplate(raw string) (*urlTemplate, error) {
	if raw == "" {
		return nil, errors.New("url template is required")
	}
	base, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse url template: %w", err)
	}
	if err := safefetch.CheckURL(base); err != nil {
		return nil, err
	}
	if strings.ContainsAny(base.Host, "{}") || (base.User != nil && strings.ContainsAny(base.User.String(), "{}")) {
		return nil, fmt.Errorf("url template %q: placeholders are only allowed after the host", raw)
	}

	authority, rest := splitAuthority(raw)
	path, tail := rest, ""
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		path, tail = rest[:i], rest[i:]
	}
	return &urlTemplate{authority: authority, path: path, tail: tail, base: base}, nil
}

// splitAuthority returns the "scheme://authority" prefix of raw and everything
// that follows it.
func splitAuthority(raw string) (authority, rest string) {
	i := strings.Index(raw, "://")
	if i < 0 {
		return raw, ""
	}
	j := strings.IndexAny(raw[i+3:], "/?#")
	if j < 0 {
		return raw, ""
	}
	return raw[:i+3+j], raw[i+3+j:]
}

// expand substitutes values into the template's placeholders and returns the
// request URL. Path placeholders are path-escaped and query placeholders
// query-escaped, and the result is rejected unless it parses back to the
// template's own scheme, userinfo and host.
func (t *urlTemplate) expand(values map[string]string) (string, error) {
	path := t.path
	tail := t.tail
	for k, v := range values {
		path = strings.ReplaceAll(path, "{"+k+"}", url.PathEscape(v))
		tail = strings.ReplaceAll(tail, "{"+k+"}", url.QueryEscape(v))
	}

	raw := t.authority + path + tail
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errTemplateAuthority, err)
	}
	if u.Opaque != "" || u.Scheme != t.base.Scheme || u.Host != t.base.Host || userinfo(u) != userinfo(t.base) {
		return "", errTemplateAuthority
	}
	return raw, nil
}

func userinfo(u *url.URL) string {
	if u.User == nil {
		return ""
	}
	return u.User.String()
}
