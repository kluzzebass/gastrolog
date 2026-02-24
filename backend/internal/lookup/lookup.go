// Package lookup provides field enrichment tables for pipeline queries.
// A LookupTable maps a string value to a set of suffix→value pairs that
// get added to the record as <field>_<suffix>.
package lookup

import "context"

// LookupTable enriches a single field value with additional fields.
// Implementations must be safe for concurrent use.
type LookupTable interface {
	// Lookup returns suffix→value pairs for the given input value.
	// Returns nil on miss (enrichment misses are normal, not errors).
	Lookup(ctx context.Context, value string) map[string]string

	// Suffixes returns the list of output suffixes this table produces
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
