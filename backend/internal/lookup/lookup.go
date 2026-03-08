// Package lookup provides field enrichment tables for pipeline queries.
package lookup

import (
	"context"
	"sort"
)

// LookupTable enriches records by looking up field values in an external source.
// Implementations must be safe for concurrent use.
type LookupTable interface {
	// Lookup returns key→value pairs for the given input value.
	// Returns nil on miss (enrichment misses are normal, not errors).
	Lookup(ctx context.Context, value string) map[string]string

	// Suffixes returns the list of output keys this table produces
	// (e.g. ["hostname"] for RDNS).
	Suffixes() []string
}

// ParameterizedLookup is optionally implemented by tables that accept
// multiple named input values (e.g. HTTP APIs with several URL placeholders).
// The pipeline maps query fields positionally to Parameters() and makes a
// single LookupValues call instead of per-field Lookup calls.
type ParameterizedLookup interface {
	LookupTable
	// Parameters returns the ordered parameter names for this table.
	Parameters() []string
	// LookupValues performs a single lookup with multiple named inputs.
	LookupValues(ctx context.Context, values map[string]string) map[string]string
}

// Resolver looks up a table by name. Returns nil if unknown.
type Resolver func(name string) LookupTable

// Registry is a static map of table name → LookupTable.
// Built at startup, read-only after — no mutex needed.
type Registry map[string]LookupTable

// Resolve returns the table for the given name, or nil if not found.
func (r Registry) Resolve(name string) LookupTable {
	return r[name]
}

// Names returns the sorted table name keys.
func (r Registry) Names() []string {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
