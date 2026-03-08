package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/theory/jsonpath"
)

const (
	defaultHTTPTimeout  = 5 * time.Second
	defaultHTTPCacheTTL = 5 * time.Minute
	defaultHTTPCacheMax = 10_000
)

// HTTPConfig configures an HTTP API lookup table.
type HTTPConfig struct {
	URLTemplate   string            // e.g. "http://api/users/{value}"
	Headers       map[string]string // optional auth headers etc.
	ResponsePaths []string          // JSONPath expressions to extract target objects; results are merged
	Parameters    []string          // ordered parameter names for {name} placeholders; empty = legacy {value} mode
	Timeout       time.Duration     // 0 = default 5s
	CacheTTL      time.Duration     // 0 = default 5min
	CacheSize     int               // 0 = default 10000
}

// httpEntry is a cached HTTP lookup result.
type httpEntry struct {
	result  map[string]string
	expires time.Time
}

// httpPath pairs a parsed JSONPath with its raw expression string.
type httpPath struct {
	raw    string
	parsed *jsonpath.Path
}

// HTTP is a lookup table that enriches records by calling an external HTTP endpoint.
// It makes GET requests to a URL template, requires a JSON response (application/json),
// optionally navigates into the response via a JSONPath expression, and flattens
// top-level scalar fields into the result map.
type HTTP struct {
	urlTemplate   string
	responsePaths []httpPath // parsed JSONPath expressions; nil/empty = use root object
	parameters    []string   // ordered parameter names; empty = legacy {value} mode
	client        *http.Client
	headers      map[string]string
	cacheTTL     time.Duration
	cacheSize    int

	mu       sync.Mutex
	cache    map[string]httpEntry
	suffixes []string // discovered from first successful response
}

// NewHTTP creates an HTTP API lookup table.
func NewHTTP(cfg HTTPConfig) *HTTP {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	cacheTTL := cfg.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = defaultHTTPCacheTTL
	}
	cacheSize := cfg.CacheSize
	if cacheSize <= 0 {
		cacheSize = defaultHTTPCacheMax
	}

	var paths []httpPath
	for _, p := range cfg.ResponsePaths {
		if parsed, err := jsonpath.Parse(p); err == nil {
			paths = append(paths, httpPath{raw: p, parsed: parsed})
		}
	}

	return &HTTP{
		urlTemplate:   cfg.URLTemplate,
		responsePaths: paths,
		parameters:    cfg.Parameters,
		client:        &http.Client{Timeout: timeout},
		headers:      cfg.Headers,
		cacheTTL:     cacheTTL,
		cacheSize:    cacheSize,
		cache:        make(map[string]httpEntry),
	}
}

// Suffixes returns the output suffixes discovered from the first successful response.
// Returns nil before any successful lookup.
func (h *HTTP) Suffixes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.suffixes
}

// Parameters returns the ordered parameter names, or nil for legacy {value} mode.
func (h *HTTP) Parameters() []string {
	return h.parameters
}

// LookupValues performs a single lookup with multiple named input values.
// Values are substituted as {key} placeholders in the URL template.
func (h *HTTP) LookupValues(ctx context.Context, values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	// Build cache key from parameter values in order.
	var b strings.Builder
	for _, p := range h.parameters {
		if b.Len() > 0 {
			b.WriteByte(0)
		}
		b.WriteString(values[p])
	}
	cacheKey := b.String()

	// Check cache.
	h.mu.Lock()
	if entry, ok := h.cache[cacheKey]; ok {
		if time.Now().Before(entry.expires) {
			h.mu.Unlock()
			return entry.result
		}
	}
	h.mu.Unlock()

	// Build URL with all parameter substitutions.
	reqURL := h.urlTemplate
	for k, v := range values {
		reqURL = strings.ReplaceAll(reqURL, "{"+k+"}", url.PathEscape(v))
	}
	result := h.doFetch(ctx, reqURL)

	// Cache the result.
	h.mu.Lock()
	if len(h.cache) >= h.cacheSize {
		clear(h.cache)
	}
	h.cache[cacheKey] = httpEntry{result: result, expires: time.Now().Add(h.cacheTTL)}
	if result != nil && h.suffixes == nil {
		keys := make([]string, 0, len(result))
		for k := range result {
			keys = append(keys, k)
		}
		h.suffixes = keys
	}
	h.mu.Unlock()

	return result
}

// Lookup calls the HTTP endpoint with the given value and returns the response
// fields as a string map. Returns nil on error, timeout, or empty response.
func (h *HTTP) Lookup(ctx context.Context, value string) map[string]string {
	if value == "" {
		return nil
	}

	// Check cache.
	h.mu.Lock()
	if entry, ok := h.cache[value]; ok {
		if time.Now().Before(entry.expires) {
			h.mu.Unlock()
			return entry.result // nil for cached negative
		}
	}
	h.mu.Unlock()

	result := h.fetch(ctx, value)

	// Cache the result (including nil for negative caching).
	h.mu.Lock()
	if len(h.cache) >= h.cacheSize {
		clear(h.cache)
	}
	h.cache[value] = httpEntry{result: result, expires: time.Now().Add(h.cacheTTL)}

	// Discover suffixes from first successful response.
	if result != nil && h.suffixes == nil {
		keys := make([]string, 0, len(result))
		for k := range result {
			keys = append(keys, k)
		}
		h.suffixes = keys
	}
	h.mu.Unlock()

	return result
}

// TestFetch makes a single HTTP request, bypassing the empty-value guard and cache.
// Values are substituted as {key} placeholders in the URL template.
func (h *HTTP) TestFetch(ctx context.Context, values map[string]string) map[string]string {
	reqURL := h.urlTemplate
	for k, v := range values {
		reqURL = strings.ReplaceAll(reqURL, "{"+k+"}", url.PathEscape(v))
	}
	return h.doFetch(ctx, reqURL)
}

// fetch makes the HTTP GET request substituting {value} in the URL template.
func (h *HTTP) fetch(ctx context.Context, value string) map[string]string {
	reqURL := strings.ReplaceAll(h.urlTemplate, "{value}", url.PathEscape(value))
	return h.doFetch(ctx, reqURL)
}

// doFetch makes the HTTP GET request and parses the JSON response.
func (h *HTTP) doFetch(ctx context.Context, reqURL string) map[string]string {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil
	}
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req) //nolint:gosec // URL is from admin-configured template
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	// Enforce JSON content type.
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		return nil
	}
	mediaType, _, _ := mime.ParseMediaType(ct)
	if mediaType != "application/json" {
		return nil
	}

	var raw any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}

	// No paths configured — flatten the root object directly.
	if len(h.responsePaths) == 0 {
		obj, ok := raw.(map[string]any)
		if !ok || len(obj) == 0 {
			return nil
		}
		return flattenScalars(obj)
	}

	// Evaluate each JSONPath and merge results.
	merged := make(map[string]string)
	for _, hp := range h.responsePaths {
		nodes := hp.parsed.Select(raw)
		for _, node := range nodes {
			mergeNode(merged, hp.raw, node)
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

// mergeNode adds a JSONPath result node into the merged map.
// Handles objects (flatten scalars), arrays (flatten each element), and scalars (use path segment as key).
func mergeNode(merged map[string]string, raw string, node any) {
	switch v := node.(type) {
	case map[string]any:
		maps.Copy(merged, flattenScalars(v))
	case []any:
		seg := pathLastSegment(raw)
		for i, elem := range v {
			switch ev := elem.(type) {
			case map[string]any:
				for k, val := range flattenScalars(ev) {
					merged[fmt.Sprintf("%s.%d.%s", seg, i, k)] = val
				}
			case string:
				merged[fmt.Sprintf("%s.%d", seg, i)] = ev
			case float64:
				merged[fmt.Sprintf("%s.%d", seg, i)] = fmt.Sprintf("%g", ev)
			case bool:
				if ev {
					merged[fmt.Sprintf("%s.%d", seg, i)] = "true"
				} else {
					merged[fmt.Sprintf("%s.%d", seg, i)] = "false"
				}
			}
		}
	case string:
		merged[pathLastSegment(raw)] = v
	case float64:
		merged[pathLastSegment(raw)] = fmt.Sprintf("%g", v)
	case bool:
		if v {
			merged[pathLastSegment(raw)] = "true"
		} else {
			merged[pathLastSegment(raw)] = "false"
		}
	}
}

// pathLastSegment extracts the last segment from a JSONPath expression.
// e.g. "$.headers.host" → "host", "$.results[0].name" → "name", "$.x" → "x".
func pathLastSegment(raw string) string {
	// Strip trailing array indices like [0].
	s := raw
	for len(s) > 0 && s[len(s)-1] == ']' {
		if idx := strings.LastIndexByte(s, '['); idx >= 0 {
			s = s[:idx]
		} else {
			break
		}
	}
	if dot := strings.LastIndexByte(s, '.'); dot >= 0 && dot+1 < len(s) {
		return s[dot+1:]
	}
	return raw
}

// flattenScalars extracts top-level values from a JSON object as strings.
// Scalars are converted directly; nested objects and arrays are JSON-encoded.
func flattenScalars(obj map[string]any) map[string]string {
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		switch tv := v.(type) {
		case string:
			out[k] = tv
		case float64:
			out[k] = fmt.Sprintf("%g", tv)
		case bool:
			if tv {
				out[k] = "true"
			} else {
				out[k] = "false"
			}
		case nil:
			// skip nulls
		default:
			if b, err := json.Marshal(tv); err == nil {
				out[k] = string(b)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
