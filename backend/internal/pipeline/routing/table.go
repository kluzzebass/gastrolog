package routing

import (
	"cmp"
	"fmt"
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
	view := overlayAttrs(attrs, src)
	for _, r := range t.routes {
		if routeMatches(r, &view) {
			return r, slices.Clone(r.VaultIDs)
		}
	}
	return nil, nil
}

// srcOverlay is a zero-copy querylang.AttrSource layering the synthetic
// source attributes over a record's own attrs for route matching. Replaces
// a per-record make+maps.Copy of the whole attributes map — ~10GB of
// garbage per soak run at pour rates (gastrolog-11y2iv). Set synthetic
// keys win over record attrs of the same name, exactly as the map
// overwrite did; All skips shadowed record attrs so no key yields twice.
type srcOverlay struct {
	attrs record.Attributes
	// Precomputed synthetic values; empty string means absent.
	source, ingester, vault, reason string
}

func overlayAttrs(attrs record.Attributes, src SourceContext) srcOverlay {
	o := srcOverlay{attrs: attrs}
	if src.Kind != SourceUnknown {
		o.source = string(src.Kind)
	}
	if src.IngesterID != glid.Nil {
		o.ingester = src.IngesterID.String()
	}
	if src.VaultID != glid.Nil {
		o.vault = src.VaultID.String()
	}
	if src.Reason != "" {
		o.reason = src.Reason
	}
	return o
}

func (o *srcOverlay) synth(key string) (string, bool) {
	switch key {
	case synthAttrSource:
		return o.source, o.source != ""
	case synthAttrIngester:
		return o.ingester, o.ingester != ""
	case synthAttrVault:
		return o.vault, o.vault != ""
	case synthAttrReason:
		return o.reason, o.reason != ""
	}
	return "", false
}

func (o *srcOverlay) Get(key string) (string, bool) {
	if v, ok := o.synth(key); ok {
		return v, true
	}
	// An unset synthetic falls through to the record attr of the same
	// name, matching the map version (which only overwrote set values).
	v, ok := o.attrs[key]
	return v, ok
}

func (o *srcOverlay) All(yield func(key, value string) bool) {
	if o.source != "" && !yield(synthAttrSource, o.source) {
		return
	}
	if o.ingester != "" && !yield(synthAttrIngester, o.ingester) {
		return
	}
	if o.vault != "" && !yield(synthAttrVault, o.vault) {
		return
	}
	if o.reason != "" && !yield(synthAttrReason, o.reason) {
		return
	}
	for k, v := range o.attrs {
		if _, shadowed := o.synth(k); shadowed {
			continue
		}
		if !yield(k, v) {
			return
		}
	}
}

func routeMatches(r *Route, attrs *srcOverlay) bool {
	switch r.Kind {
	case MatchNone:
		return false
	case MatchAll:
		return true
	case MatchExpr:
		return querylang.MatchAttrSource(r.DNF, attrs)
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
