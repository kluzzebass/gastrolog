// Package lookup provides field enrichment tables for pipeline queries.
package lookup

import (
	"context"
	"sort"
)

// LookupTable enriches records by looking up field values in an external source.
// Implementations must be safe for concurrent use.
//
// Every table exposes ordered Parameters (at least one). Single-input tables
// (RDNS, GeoIP, ASN) return ["value"]; multi-input tables (HTTP, JSON file)
// return their configured parameter names. The pipeline uses len(Parameters())
// to decide between per-field iteration and positional multi-field mapping.
type LookupTable interface {
	// LookupValues performs a lookup with named inputs.
	// Returns nil on miss (enrichment misses are normal, not errors).
	LookupValues(ctx context.Context, values map[string]string) map[string]string

	// Parameters returns the ordered parameter names for this table.
	// Single-input tables return ["value"].
	Parameters() []string

	// Suffixes returns the list of output keys this table produces
	// (e.g. ["hostname"] for RDNS).
	Suffixes() []string
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
