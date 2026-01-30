package chatterbox

import (
	"fmt"
	"math/rand/v2"
)

// LogFormat generates synthetic log messages with associated source attributes.
type LogFormat interface {
	// Generate returns a raw log message and source attributes.
	// The attrs map should contain stable attribute dimensions that
	// can be used to form meaningful SourceIDs.
	Generate(rng *rand.Rand) (raw []byte, attrs map[string]string)
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
