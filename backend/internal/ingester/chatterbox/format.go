package chatterbox

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// LogFormat generates synthetic log messages with associated source attributes.
type LogFormat interface {
	// Generate returns a raw log message, source attributes, and the source timestamp.
	// sourceTS is the timestamp embedded in the log message (zero if the format has none).
	Generate(rng *rand.Rand) (raw []byte, attrs map[string]string, sourceTS time.Time)
}

// MultiRecordFormat generates multiple log records at once (e.g. stack dumps,
// command help output) that have accidentally found their way into logs.
// Each line or logical unit becomes its own record.
type MultiRecordFormat interface {
	// GenerateMulti returns multiple records. Each record is a separate line/unit.
	GenerateMulti(rng *rand.Rand) []recordDraft
}

// recordDraft is a single record before the ingester adds IngestTS and base attrs.
type recordDraft struct {
	Raw      []byte
	Attrs    map[string]string
	SourceTS time.Time
}

// singleFormatAdapter wraps a LogFormat to produce one record via GenerateMulti.
type singleFormatAdapter struct {
	f LogFormat
}

func (a *singleFormatAdapter) GenerateMulti(rng *rand.Rand) []recordDraft {
	raw, attrs, sourceTS := a.f.Generate(rng)
	return []recordDraft{{Raw: raw, Attrs: attrs, SourceTS: sourceTS}}
}

// AttributePools holds pre-generated pools of attribute values.
// These are shared across format implementations to ensure consistent
// cardinality and enable source grouping.
type AttributePools struct {
	Hosts    []string
	Services []string
	Envs     []string
	VHosts   []string
}

// NewAttributePools creates attribute pools with the specified sizes.
func NewAttributePools(hostCount, serviceCount int) *AttributePools {
	hosts := make([]string, hostCount)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("host-%d", i+1)
	}

	services := make([]string, serviceCount)
	serviceNames := []string{"api", "web", "backend", "worker", "gateway", "auth", "cache", "db-proxy", "scheduler", "metrics"}
	for i := range services {
		services[i] = serviceNames[i%len(serviceNames)]
	}

	envs := []string{"prod", "staging", "dev", "test"}

	vhosts := []string{
		"example.com",
		"api.example.com",
		"admin.example.com",
		"cdn.example.com",
		"static.example.com",
	}

	return &AttributePools{
		Hosts:    hosts,
		Services: services,
		Envs:     envs,
		VHosts:   vhosts,
	}
}

// pick returns a random element from the slice.
func pick[T any](rng *rand.Rand, s []T) T {
	return s[rng.IntN(len(s))]
}
