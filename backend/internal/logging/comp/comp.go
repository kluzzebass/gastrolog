// Package comp defines the hierarchical component-path identifier used as
// the "component" slog attribute throughout GastroLog.
//
// Paths are constructed via Root (top-level) and Sub (children) — there
// is no string-to-Path constructor by design, so the only component
// values that flow through the logging system are ones some package
// actually built. That's what makes hierarchical filtering reliable:
// when an operator sets "orchestrator.*=debug", every descendant path
// inherits, and nothing accidentally bypasses the hierarchy by using a
// flat string detached from any parent.
//
// The pattern mirrors echo's *Group:
//
//	// at wiring time — parent assigns the namespace
//	ingesterGroup := comp.Root("ingester")
//	relp.NewFactory(logger, ingesterGroup)   // factory uses .Sub("relp")
//	mqtt.NewFactory(logger, ingesterGroup)   // factory uses .Sub("mqtt")
//
//	// inside the factory
//	func NewFactory(logger *slog.Logger, group comp.Path) *Factory {
//	    return &Factory{
//	        logger: group.Sub("relp").Apply(logger),
//	    }
//	}
//
// There is no central manifest. The set of known paths is whatever Root
// and Sub have built — discovered via All() at runtime, the same way
// echo's Routes() enumerates whatever Add calls have happened.
package comp

import (
	"log/slog"
	"slices"
	"strings"
	"sync"
)

// Path is a hierarchical component identifier. The zero value is invalid;
// construct via Root or .Sub. Each construction auto-registers the path
// in the package-level registry, so All() returns every path some code
// has built — no separate manifest to maintain.
type Path struct {
	s string
}

// Root creates a top-level component path.
//
// name must be non-empty and must not contain "." (the path separator) or
// "*" (the glob wildcard). Violating any of those panics — this is a
// development-time mistake, not a runtime condition to handle.
func Root(name string) Path {
	mustValidSegment(name)
	p := Path{s: name}
	register(p)
	return p
}

// Sub extends a path with a child segment. name follows the same rules as
// Root.
func (p Path) Sub(name string) Path {
	mustValidSegment(name)
	if p.s == "" {
		panic("comp: Sub called on zero Path; use Root to create a top-level path")
	}
	child := Path{s: p.s + "." + name}
	register(child)
	return child
}

// String returns the dotted form (e.g. "orchestrator.replication").
func (p Path) String() string {
	return p.s
}

// Apply returns logger.With("component", p.String()). This is the only
// supported way to set the component attribute — never write
// .With("component", "foo") directly.
func (p Path) Apply(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return nil
	}
	return logger.With("component", p.s)
}

// All returns every Path that has been constructed via Root or Sub,
// sorted by dotted form. The returned slice is a defensive copy.
//
// Used by the ListLogComponents RPC for CLI tab-completion and UI
// dropdown discovery. The set grows as packages run their init() vars
// and as wiring code builds the path tree; in a fully-initialised
// binary it represents every component path the binary could ever emit.
func All() []Path {
	registryMu.Lock()
	defer registryMu.Unlock()
	out := make([]Path, len(registry))
	copy(out, registry)
	slices.SortFunc(out, func(a, b Path) int { return strings.Compare(a.s, b.s) })
	return out
}

var (
	registryMu sync.Mutex
	registry   []Path
	registered = map[string]bool{}
)

func register(p Path) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if registered[p.s] {
		return
	}
	registered[p.s] = true
	registry = append(registry, p)
}

func mustValidSegment(name string) {
	if name == "" {
		panic("comp: path segment must not be empty")
	}
	if strings.ContainsAny(name, ".*") {
		panic("comp: path segment must not contain '.' or '*': " + name)
	}
}

// resetRegistryForTest clears the registry. Intended for tests in this
// package that exercise auto-registration; production code should never
// call it.
func resetRegistryForTest() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = nil
	registered = map[string]bool{}
}
