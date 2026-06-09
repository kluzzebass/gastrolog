package routing

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"gastrolog/internal/glid"
	"gastrolog/internal/querylang"
	"gastrolog/internal/record"
)

// MatchKind identifies how a route's match expression evaluates.
type MatchKind int

const (
	MatchNone MatchKind = iota
	MatchAll
	MatchExpr
)

// Route is a compiled routing rule: expression → vault IDs. Routing knows
// vaults only — no node placement or replication factor.
type Route struct {
	ID       glid.GLID
	Name     string
	Priority int32
	Kind     MatchKind
	Expr     string
	DNF      *querylang.DNF
	VaultIDs []glid.GLID
}

// SourceKind identifies record origin for synthetic routing attributes.
type SourceKind string

const (
	SourceIngest    SourceKind = "ingest"
	SourceRetention SourceKind = "retention"
	SourceUnknown   SourceKind = ""
)

// SourceContext carries routing-time fields overlaid as `_source`, `_ingester`,
// `_vault`, and `_reason` during rule evaluation only.
type SourceContext struct {
	Kind       SourceKind
	IngesterID glid.GLID
	VaultID    glid.GLID
	Reason     string
}

const (
	synthAttrSource   = "_source"
	synthAttrIngester = "_ingester"
	synthAttrVault    = "_vault"
	synthAttrReason   = "_reason"
)

// Table evaluates routes in priority order; first match wins.
type Table struct {
	routes []*Route
}

// NewTable copies and sorts routes by (priority asc, name asc).
func NewTable(routes []*Route) *Table {
	cp := make([]*Route, len(routes))
	copy(cp, routes)
	slices.SortFunc(cp, func(a, b *Route) int {
		if c := cmp.Compare(a.Priority, b.Priority); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return &Table{routes: cp}
}

// CompileRoute parses expression and binds vault destinations.
func CompileRoute(id glid.GLID, name string, priority int32, expression string, vaultIDs []glid.GLID) (*Route, error) {
	r := &Route{
		ID:       id,
		Name:     name,
		Priority: priority,
		Expr:     expression,
		VaultIDs: vaultIDs,
	}
	expr := strings.TrimSpace(expression)

	switch expr {
	case "":
		r.Kind = MatchNone
		return r, nil
	case "*":
		r.Kind = MatchAll
		return r, nil
	}

	parsed, err := querylang.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}
	if err := querylang.ValidateAttrFilter(parsed); err != nil {
		return nil, err
	}
	dnf := querylang.ToDNF(parsed)
	r.Kind = MatchExpr
	r.DNF = &dnf
	return r, nil
}

// Match returns vault IDs for the first matching route, or nil when no route
// matches (intentional drop — operators add a catch-all at lowest priority).
func (t *Table) Match(attrs record.Attributes, src SourceContext) []glid.GLID {
	_, vaults := t.MatchRoute(attrs, src)
	return vaults
}

// MatchRoute returns the first matching route and a clone of its destination
// vault IDs, or (nil, nil) when no route matches. Exposed so the routing manager
// can attribute matched-record counters to the specific route that fired.
func (t *Table) MatchRoute(attrs record.Attributes, src SourceContext) (*Route, []glid.GLID) {
	if t == nil {
		return nil, nil
	}
	enriched := enrichAttrs(attrs, src)
	for _, r := range t.routes {
		if routeMatches(r, enriched) {
			return r, slices.Clone(r.VaultIDs)
		}
	}
	return nil, nil
}

func enrichAttrs(attrs record.Attributes, src SourceContext) record.Attributes {
	enriched := make(record.Attributes, len(attrs)+4)
	maps.Copy(enriched, attrs)
	if src.Kind != SourceUnknown {
		enriched[synthAttrSource] = string(src.Kind)
	}
	if src.IngesterID != glid.Nil {
		enriched[synthAttrIngester] = src.IngesterID.String()
	}
	if src.VaultID != glid.Nil {
		enriched[synthAttrVault] = src.VaultID.String()
	}
	if src.Reason != "" {
		enriched[synthAttrReason] = src.Reason
	}
	return enriched
}

func routeMatches(r *Route, attrs record.Attributes) bool {
	switch r.Kind {
	case MatchNone:
		return false
	case MatchAll:
		return true
	case MatchExpr:
		return querylang.MatchAttrs(r.DNF, attrs)
	}
	return false
}

// IngestSource builds SourceContext for records arriving from the ingest→digest
// path. The ingester ID is taken from EventID.
func IngestSource(rec *record.Record) SourceContext {
	if rec == nil {
		return SourceContext{Kind: SourceIngest}
	}
	return SourceContext{
		Kind:       SourceIngest,
		IngesterID: rec.EventID.IngesterID,
	}
}

// RetentionSource builds SourceContext for records ejected from a vault during a
// retention event (disposition=route). The source vault ID and optional reason
// (`age`, `size`, `count`) overlay as `_vault` and `_reason` during matching.
func RetentionSource(vaultID glid.GLID, reason string) SourceContext {
	return SourceContext{
		Kind:    SourceRetention,
		VaultID: vaultID,
		Reason:  reason,
	}
}
