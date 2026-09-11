package lookup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/itchyny/gojq"

	"gastrolog/internal/logging"
	"gastrolog/internal/logging/comp"
	"gastrolog/internal/safefetch"
)

const (
	defaultHTTPTimeout  = 5 * time.Second
	defaultHTTPCacheTTL = 5 * time.Minute
	defaultHTTPCacheMax = 10_000

	// maxConcurrentFetches bounds the requests one lookup table has in flight
	// at once, so concurrent queries cannot multiply into a burst at the
	// configured endpoint.
	maxConcurrentFetches = 8

	// maxJQResults caps what one response-path expression may emit. A jq
	// program can generate without bound from a small input, and every
	// value it emits is merged into a map held in memory.
	maxJQResults = 1024

	// maxHTTPTimeout caps how long one lookup may hold a request open. A
	// per-record fetch that waits longer than this pins a goroutine and the
	// query behind it for no useful enrichment.
	maxHTTPTimeout = 30 * time.Second

	// maxResponseBytes caps what one lookup response may cost in memory. A
	// lookup answer is a small JSON object; anything larger is a misconfigured
	// or hostile endpoint.
	maxResponseBytes = 4 << 20
)

// errResponseTooLarge reports an endpoint answering with more than a lookup
// result could reasonably be.
var errResponseTooLarge = errors.New("lookup response exceeds the size limit")

// HTTPConfig configures an HTTP API lookup table.
type HTTPConfig struct {
	URLTemplate   string            // e.g. "http://api/users/{value}"
	Headers       map[string]string // optional auth headers etc.
	ResponsePaths []string          // JSONPath expressions to extract target objects; results are merged
	Parameters    []string          // ordered parameter names for {name} placeholders; empty = legacy {value} mode
	Timeout       time.Duration     // 0 = default 5s
	CacheTTL      time.Duration     // 0 = default 5min
	CacheSize     int               // 0 = default 10000

	// AllowPrivateDestinations lets this table reach loopback, private and
	// unique-local addresses, which the destination policy denies by default.
	// Link-local space stays out of reach either way.
	AllowPrivateDestinations bool

	Name   string       // registry name, for log lines
	Logger *slog.Logger // nil discards
}

// httpEntry is a cached HTTP lookup result.
type httpEntry struct {
	result  map[string]string
	expires time.Time
}

// httpPath pairs a pre-compiled jq program with its raw expression string.
type httpPath struct {
	raw    string
	parsed *gojq.Code
}

// jsonPathFilter matches JSONPath filter expressions like [?(@.field == 'value')] or [?(@.field == true)].
// Captures the full content between [?( and )].
var jsonPathFilter = regexp.MustCompile(`\[\?\(([^)]+)\)\]`)

// jsonPathToJQ converts a JSONPath expression to an equivalent jq expression.
// Supports: $ root, dot-notation, bracket notation, array indices, and filter expressions.
func jsonPathToJQ(jp string) string {
	if jp == "" {
		return "."
	}

	// Strip leading $.
	s := strings.TrimPrefix(jp, "$")
	if s == "" {
		return "."
	}

	// Convert bracket key access $['key'] or $["key"] to .["key"].
	s = strings.ReplaceAll(s, "['", `["`)
	s = strings.ReplaceAll(s, "']", `"]`)

	// Convert filter expressions [?(expr)] to [] | select(expr).
	s = jsonPathFilter.ReplaceAllStringFunc(s, func(match string) string {
		inner := jsonPathFilter.FindStringSubmatch(match)
		if len(inner) < 2 {
			return match
		}
		expr := inner[1]

		// Convert @.field references to .field.
		expr = strings.ReplaceAll(expr, "@.", ".")

		// Convert single-quoted strings to double-quoted.
		expr = strings.ReplaceAll(expr, "'", `"`)

		// Convert && to "and", || to "or".
		expr = strings.ReplaceAll(expr, "&&", "and")
		expr = strings.ReplaceAll(expr, "||", "or")

		return "[] | select(" + expr + ")"
	})

	// Ensure it starts with a dot.
	if !strings.HasPrefix(s, ".") && !strings.HasPrefix(s, "[") {
		s = "." + s
	}
	// Bracket notation at start needs a dot prefix: ["key"] -> .["key"]
	if strings.HasPrefix(s, "[") {
		s = "." + s
	}

	return s
}

// CompileJQ parses a JSONPath expression by converting it to jq syntax, then compiles it.
func CompileJQ(expr string) (*gojq.Code, error) {
	jqExpr := jsonPathToJQ(expr)
	parsed, err := gojq.Parse(jqExpr)
	if err != nil {
		return nil, fmt.Errorf("parse %q (from %q): %w", jqExpr, expr, err)
	}
	code, err := gojq.Compile(parsed)
	if err != nil {
		return nil, fmt.Errorf("compile %q (from %q): %w", jqExpr, expr, err)
	}
	return code, nil
}

// jqSelect runs a compiled jq program against input and collects its
// non-error results, bounded in both directions a jq program can run away:
// the context cuts a program that will not finish, and maxJQResults caps a
// program that emits without end. An expression can reach the engine from a
// lookup configuration, so neither bound is theoretical.
func jqSelect(ctx context.Context, code *gojq.Code, input any) []any {
	return jqSelectN(ctx, code, input, maxJQResults)
}

// jqSelectN runs a compiled jq program and collects up to maxResults values.
// maxResults <= 0 means unlimited.
func jqSelectN(ctx context.Context, code *gojq.Code, input any, maxResults int) []any {
	var results []any
	iter := code.RunWithContext(ctx, input)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if _, isErr := v.(error); isErr {
			break
		}
		results = append(results, v)
		if maxResults > 0 && len(results) >= maxResults {
			break
		}
	}
	return results
}

// HTTP is a lookup table that enriches records by calling an external HTTP endpoint.
// It makes GET requests to a URL template, requires a JSON response (application/json),
// optionally navigates into the response via a JSONPath expression, and flattens
// top-level scalar fields into the result map.
type HTTP struct {
	urlTemplate   *urlTemplate
	responsePaths []httpPath // parsed JSONPath expressions; nil/empty = use root object
	parameters    []string   // ordered parameter names; empty = legacy {value} mode
	client        *http.Client
	headers       map[string]string
	cacheTTL      time.Duration
	cacheSize     int
	inFlight      chan struct{} // capacity bounds concurrent outbound requests
	name          string
	logger        *slog.Logger

	mu       sync.Mutex
	cache    map[string]httpEntry
	suffixes []string // discovered from first successful response
}

// NewHTTP creates an HTTP API lookup table. It fails when the URL template
// cannot be fetched safely — an unsupported scheme, or a placeholder in the
// host, which would let a record value pick the destination.
func NewHTTP(cfg HTTPConfig) (*HTTP, error) {
	tmpl, err := parseURLTemplate(cfg.URLTemplate)
	if err != nil {
		return nil, err
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	if timeout > maxHTTPTimeout {
		timeout = maxHTTPTimeout
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
		code, err := CompileJQ(p)
		if err != nil {
			continue
		}
		paths = append(paths, httpPath{raw: p, parsed: code})
	}

	params := cfg.Parameters
	if len(params) == 0 {
		params = []string{"value"}
	}

	policy := safefetch.Policy{AllowPrivate: cfg.AllowPrivateDestinations}
	return &HTTP{
		urlTemplate:   tmpl,
		responsePaths: paths,
		parameters:    params,
		client:        safefetch.Client(policy, timeout),
		headers:       cfg.Headers,
		cacheTTL:      cacheTTL,
		cacheSize:     cacheSize,
		inFlight:      make(chan struct{}, maxConcurrentFetches),
		name:          cfg.Name,
		logger: comp.Root("lookup").Desc(
			"Lookup tables that enrich records at query time — HTTP, file-backed, MMDB and static.",
		).Apply(logging.Default(cfg.Logger)),
		cache: make(map[string]httpEntry),
	}, nil
}

// Suffixes returns the output suffixes discovered from the first successful response.
// Returns nil before any successful lookup.
func (h *HTTP) Suffixes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.suffixes
}

// Parameters returns the ordered parameter names (at least ["value"]).
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

	reqURL, err := h.urlTemplate.expand(values)
	if err != nil {
		h.logger.Debug("lookup url could not be built", "table", h.name, "error", err)
		return nil
	}

	// A cache miss is what costs an outbound request, so it is what the
	// per-query budget pays for.
	allowed, firstRefusal := spendOutbound(ctx)
	if !allowed {
		if firstRefusal {
			limit, _ := outboundLimit(ctx)
			h.logger.Warn("outbound lookup budget exhausted; remaining records go unenriched",
				"table", h.name, "limit", limit)
		}
		return nil
	}

	result, err := h.fetch(ctx, reqURL)
	if err != nil {
		h.logger.Debug("lookup fetch failed", "table", h.name, "error", err)
	}

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

// TestFetch makes a single HTTP request, bypassing the empty-value guard and cache.
// Values are substituted as {key} placeholders in the URL template.
func (h *HTTP) TestFetch(ctx context.Context, values map[string]string) (map[string]string, error) {
	reqURL, err := h.urlTemplate.expand(values)
	if err != nil {
		return nil, err
	}
	return h.fetch(ctx, reqURL)
}

// fetch makes the HTTP GET request and parses the JSON response. An empty
// result and a nil error means the endpoint answered with nothing usable.
func (h *HTTP) fetch(ctx context.Context, reqURL string) (map[string]string, error) {
	select {
	case h.inFlight <- struct{}{}:
		defer func() { <-h.inFlight }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lookup endpoint returned HTTP %d", resp.StatusCode)
	}

	// Enforce JSON content type.
	ct := resp.Header.Get("Content-Type")
	mediaType, _, _ := mime.ParseMediaType(ct)
	if mediaType != "application/json" {
		return nil, fmt.Errorf("lookup endpoint returned content type %q, want application/json", ct)
	}

	// Read through a limit rather than streaming into the decoder: the reply is
	// a small object, and a caller-chosen URL must not be able to pin memory.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxResponseBytes {
		return nil, errResponseTooLarge
	}

	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	// No paths configured — flatten the root object directly.
	if len(h.responsePaths) == 0 {
		obj, ok := raw.(map[string]any)
		if !ok || len(obj) == 0 {
			return nil, nil
		}
		return flattenScalars(obj), nil
	}

	// Evaluate each jq expression and merge results.
	merged := make(map[string]string)
	for _, hp := range h.responsePaths {
		nodes := jqSelect(ctx, hp.parsed, raw)
		for _, node := range nodes {
			mergeNode(merged, hp.raw, node)
		}
	}
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
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
