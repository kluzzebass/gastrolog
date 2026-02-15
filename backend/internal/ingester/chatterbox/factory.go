package chatterbox

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

const (
	defaultMinInterval  = 100 * time.Millisecond
	defaultMaxInterval  = 1 * time.Second
	defaultHostCount    = 10
	defaultServiceCount = 5
)

// Format names for configuration.
const (
	FormatPlain       = "plain"
	FormatKV          = "kv"
	FormatJSON        = "json"
	FormatAccess      = "access"
	FormatSyslog      = "syslog"
	FormatWeird       = "weird"
	FormatMultirecord = "multirecord"
)

// allFormats lists all supported format names in default order.
var allFormats = []string{FormatPlain, FormatKV, FormatJSON, FormatAccess, FormatSyslog, FormatWeird, FormatMultirecord}

// NewIngester creates a new chatterbox ingester from configuration parameters.
//
// Supported parameters:
//   - "minInterval": minimum delay between messages (default: "100ms")
//   - "maxInterval": maximum delay between messages (default: "1s")
//   - "formats": comma-separated list of enabled formats (default: all)
//     Valid formats: plain, kv, json, access, syslog, weird, multirecord
//   - "formatWeights": comma-separated format=weight pairs (default: equal weights)
//     Example: "plain=30,json=20,kv=25,access=10,syslog=10,weird=5"
//   - "hostCount": number of distinct hosts to generate (default: 10)
//   - "serviceCount": number of distinct services to generate (default: 5)
//
// Intervals use Go duration format: "100us", "1.5ms", "2s", etc.
//
// If logger is nil, logging is disabled.
//
// Returns an error if parameters are invalid (e.g., unparseable duration,
// min > max, negative values, unknown format names).
func NewIngester(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
	minInterval := defaultMinInterval
	maxInterval := defaultMaxInterval
	hostCount := defaultHostCount
	serviceCount := defaultServiceCount

	if v, ok := params["minInterval"]; ok {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid minInterval %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("minInterval must be non-negative, got %v", parsed)
		}
		minInterval = parsed
	}

	if v, ok := params["maxInterval"]; ok {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid maxInterval %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("maxInterval must be non-negative, got %v", parsed)
		}
		maxInterval = parsed
	}

	if minInterval > maxInterval {
		return nil, fmt.Errorf("minInterval (%v) must not exceed maxInterval (%v)", minInterval, maxInterval)
	}

	if v, ok := params["hostCount"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid hostCount %q: %w", v, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("hostCount must be positive, got %d", n)
		}
		hostCount = n
	}

	if v, ok := params["serviceCount"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid serviceCount %q: %w", v, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("serviceCount must be positive, got %d", n)
		}
		serviceCount = n
	}

	// Parse format configuration.
	enabledFormats, err := parseFormats(params["formats"])
	if err != nil {
		return nil, err
	}

	weights, err := parseWeights(params["formatWeights"], enabledFormats)
	if err != nil {
		return nil, err
	}

	// Create attribute pools.
	pools := NewAttributePools(hostCount, serviceCount)

	// Create format instances.
	formats, cumulativeWeights, totalWeight := buildFormats(enabledFormats, weights, pools)

	// Scope logger with component identity.
	scopedLogger := logging.Default(logger).With(
		"component", "ingester",
		"type", "chatterbox",
	)

	return &Ingester{
		id:          id.String(),
		minInterval: minInterval,
		maxInterval: maxInterval,
		rng:         rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		formats:     formats,
		weights:     cumulativeWeights,
		totalWeight: totalWeight,
		logger:      scopedLogger,
	}, nil
}

// parseFormats parses the formats parameter into a list of format names.
// If empty, returns all formats.
func parseFormats(formatsParam string) ([]string, error) {
	if formatsParam == "" {
		return allFormats, nil
	}

	parts := strings.Split(formatsParam, ",")
	var formats []string
	seen := make(map[string]bool)

	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name == "" {
			continue
		}
		if !isValidFormat(name) {
			return nil, fmt.Errorf("unknown format %q", name)
		}
		if seen[name] {
			continue // Skip duplicates.
		}
		seen[name] = true
		formats = append(formats, name)
	}

	if len(formats) == 0 {
		return nil, fmt.Errorf("no valid formats specified")
	}

	return formats, nil
}

// isValidFormat checks if a format name is valid.
func isValidFormat(name string) bool {
	return slices.Contains(allFormats, name)
}

// parseWeights parses the formatWeights parameter into a weight map.
// If empty, returns equal weights for all enabled formats.
func parseWeights(weightsParam string, enabledFormats []string) (map[string]int, error) {
	weights := make(map[string]int)

	if weightsParam == "" {
		// Equal weights for all enabled formats.
		for _, f := range enabledFormats {
			weights[f] = 1
		}
		return weights, nil
	}

	parts := strings.SplitSeq(weightsParam, ",")
	for p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid weight format %q, expected name=weight", p)
		}

		name := strings.TrimSpace(kv[0])
		weightStr := strings.TrimSpace(kv[1])

		if !isValidFormat(name) {
			return nil, fmt.Errorf("unknown format %q in weights", name)
		}

		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			return nil, fmt.Errorf("invalid weight for %q: %w", name, err)
		}
		if weight <= 0 {
			return nil, fmt.Errorf("weight for %q must be positive, got %d", name, weight)
		}

		weights[name] = weight
	}

	// Ensure all enabled formats have a weight.
	for _, f := range enabledFormats {
		if _, ok := weights[f]; !ok {
			weights[f] = 1 // Default weight for unspecified formats.
		}
	}

	return weights, nil
}

// buildFormats creates format instances and builds the cumulative weight table.
func buildFormats(enabledFormats []string, weights map[string]int, pools *AttributePools) ([]MultiRecordFormat, []int, int) {
	formats := make([]MultiRecordFormat, 0, len(enabledFormats))
	cumulativeWeights := make([]int, 0, len(enabledFormats))
	totalWeight := 0

	for _, name := range enabledFormats {
		var format MultiRecordFormat
		switch name {
		case FormatPlain:
			format = &singleFormatAdapter{f: NewPlainTextFormat(pools)}
		case FormatKV:
			format = &singleFormatAdapter{f: NewKeyValueFormat(pools)}
		case FormatJSON:
			format = &singleFormatAdapter{f: NewJSONFormat(pools)}
		case FormatAccess:
			format = &singleFormatAdapter{f: NewAccessLogFormat(pools)}
		case FormatSyslog:
			format = &singleFormatAdapter{f: NewSyslogFormat(pools)}
		case FormatWeird:
			format = &singleFormatAdapter{f: NewWeirdFormat(pools)}
		case FormatMultirecord:
			format = NewMultirecordFormat(pools)
		}

		formats = append(formats, format)
		totalWeight += weights[name]
		cumulativeWeights = append(cumulativeWeights, totalWeight)
	}

	return formats, cumulativeWeights, totalWeight
}
