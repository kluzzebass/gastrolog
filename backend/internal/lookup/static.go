package lookup

import (
	"context"
	"maps"
	"sort"
)

// StaticRow holds the column values for a single static lookup row.
type StaticRow struct {
	Values map[string]string
}

// Static is an in-memory lookup table populated from operator-defined rows.
// No file I/O, no mmap, no watcher — data lives entirely in memory and is
// replicated via the Raft-backed system config.
// Safe for concurrent use (immutable after construction).
type Static struct {
	name         string
	keyColumn    string
	valueColumns []string // output column names (sorted)
	index        map[string]map[string]string
}

var _ LookupTable = (*Static)(nil)

// NewStatic creates an in-memory static lookup table from the given rows.
// The key column value in each row is used as the index key; the remaining
// value columns are returned on lookup. First occurrence wins for duplicate keys.
func NewStatic(name string, keyColumn string, valueColumns []string, rows []StaticRow) *Static {
	// Determine output columns: explicit list, or discover from rows (excluding key column).
	valCols := valueColumns
	if len(valCols) == 0 {
		seen := make(map[string]struct{})
		for _, r := range rows {
			for k := range r.Values {
				if k != keyColumn {
					seen[k] = struct{}{}
				}
			}
		}
		valCols = make([]string, 0, len(seen))
		for k := range seen {
			valCols = append(valCols, k)
		}
		sort.Strings(valCols)
	}

	// Build the key → values index.
	index := make(map[string]map[string]string, len(rows))
	valSet := make(map[string]struct{}, len(valCols))
	for _, c := range valCols {
		valSet[c] = struct{}{}
	}

	for _, r := range rows {
		key := r.Values[keyColumn]
		if key == "" {
			continue
		}
		if _, exists := index[key]; exists {
			continue // first occurrence wins
		}
		vals := make(map[string]string, len(valCols))
		for k, v := range r.Values {
			if k == keyColumn {
				continue
			}
			if _, ok := valSet[k]; ok {
				vals[k] = v
			}
		}
		index[key] = vals
	}

	return &Static{
		name:         name,
		keyColumn:    keyColumn,
		valueColumns: valCols,
		index:        index,
	}
}

// Parameters returns the single input parameter name.
func (s *Static) Parameters() []string { return []string{"value"} }

// Suffixes returns the output column names.
func (s *Static) Suffixes() []string { return s.valueColumns }

// LookupValues looks up a key in the static table.
func (s *Static) LookupValues(_ context.Context, values map[string]string) map[string]string {
	key := values["value"]
	if key == "" {
		return nil
	}
	row, ok := s.index[key]
	if !ok {
		return nil
	}
	// Return a copy to prevent caller mutation.
	out := make(map[string]string, len(row))
	maps.Copy(out, row)
	return out
}
