package orchestrator

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
)

// IngesterDesired describes one ingester that should run on this node.
//
// Build is invoked lazily — only when the orchestrator must (re)construct the
// ingester because its ID is new or its metadata changed. An unchanged ingester
// keeps its already-running instance and never flaps its alive state. Build is
// responsible for constructing the ingester AND restoring any replicated
// checkpoint before returning.
type IngesterDesired struct {
	ID      glid.GLID
	Name    string
	Type    string
	Passive bool
	// Params is the config-store parameter snapshot. A change triggers rebuild
	// even when Name, Type, and Passive are unchanged.
	Params map[string]string
	Build  func() (Ingester, error)
}

// ReconcileIngesters drives the running ingester set toward desired. It is the
// single authoritative, declarative entry point for ingester lifecycle:
// callers compute the full set of ingesters that should run on this node and
// pass it here.
//
//   - Ingesters absent from desired are stopped and dropped.
//   - New or metadata-changed ingesters are built (and checkpoint-restored) via
//     Build, then started.
//   - Unchanged ingesters keep their running instance untouched, so their alive
//     state does not flap across reconciles.
//
// Build errors for individual ingesters are collected and returned without
// aborting the reconcile; the remaining ingesters still converge.
func (o *Orchestrator) ReconcileIngesters(desired []IngesterDesired) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reconcileIngestersLocked(desired)
}

func (o *Orchestrator) reconcileIngestersLocked(desired []IngesterDesired) error {
	want := make(map[glid.GLID]IngesterDesired, len(desired))
	for _, d := range desired {
		if d.ID.IsZero() {
			return errors.New("ingester desired entry missing ID")
		}
		if d.Build == nil {
			return fmt.Errorf("ingester %s (%s): nil Build", d.Name, d.ID)
		}
		want[d.ID] = d
	}

	// Drop ingesters no longer desired. Stats are retained so historical
	// counters survive a reassignment/restart of the same ingester ID.
	for id := range o.ingesters {
		if _, ok := want[id]; !ok {
			delete(o.ingesters, id)
			delete(o.ingesterMeta, id)
			delete(o.ingesterAdapters, id)
		}
	}

	// Add or rebuild changed ingesters.
	var buildErr error
	for id, d := range want {
		meta := ingesterInfo{
			Name:    d.Name,
			Type:    d.Type,
			Passive: d.Passive,
			Params:  cloneIngesterParams(d.Params),
		}
		if _, had := o.ingesters[id]; had {
			old := o.ingesterMeta[id]
			if old.equal(meta) {
				continue // unchanged — keep the running instance and cached adapter
			}
			// A rebuild stops and restarts a running ingester, so the
			// reason must be operator-visible: the 3mnjlo boot-window
			// flap (started at boot, stop/started on the first sweep
			// tick) was undiagnosable because nothing recorded WHAT the
			// reconcile considered changed. Field names and param KEYS
			// only — params may hold credentials, never log values.
			o.logger.Info("ingester config changed; rebuilding",
				"id", id, "name", meta.Name, "changed", old.diff(meta))
		}
		ing, err := d.Build()
		if err != nil {
			buildErr = errors.Join(buildErr, fmt.Errorf("build ingester %s (%s): %w", d.Name, id, err))
			continue
		}
		o.setIngesterLocked(id, meta, ing)
	}

	if err := o.pushIngestersToSupervisorLocked(); err != nil {
		return errors.Join(buildErr, err)
	}
	return buildErr
}

func cloneIngesterParams(p map[string]string) map[string]string {
	if len(p) == 0 {
		return nil
	}
	return maps.Clone(p)
}

func (i ingesterInfo) equal(other ingesterInfo) bool {
	return i.Name == other.Name &&
		i.Type == other.Type &&
		i.Passive == other.Passive &&
		maps.Equal(i.Params, other.Params)
}

// diff names the fields that differ between two ingesterInfo values, for
// the rebuild log line. Param VALUES are never included (they may hold
// credentials); changed params are reported by key, e.g.
// "params[rate,token]".
func (i ingesterInfo) diff(other ingesterInfo) string {
	var parts []string
	if i.Name != other.Name {
		parts = append(parts, "name")
	}
	if i.Type != other.Type {
		parts = append(parts, "type")
	}
	if i.Passive != other.Passive {
		parts = append(parts, "passive")
	}
	if !maps.Equal(i.Params, other.Params) {
		keys := make(map[string]bool)
		for k, v := range i.Params {
			if ov, ok := other.Params[k]; !ok || ov != v {
				keys[k] = true
			}
		}
		for k, v := range other.Params {
			if iv, ok := i.Params[k]; !ok || iv != v {
				keys[k] = true
			}
		}
		parts = append(parts, "params["+strings.Join(slices.Sorted(maps.Keys(keys)), ",")+"]")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

// setIngesterLocked installs (or replaces) an ingester in the desired set and
// (re)builds its stable pipeline adapter. Caller holds o.mu.
func (o *Orchestrator) setIngesterLocked(id glid.GLID, meta ingesterInfo, ing Ingester) {
	o.ingesters[id] = ing
	o.ingesterMeta[id] = meta
	if o.ingesterStats[id] == nil {
		o.ingesterStats[id] = &IngesterStats{}
	}
	o.ingesterAdapters[id] = o.newIngesterAdapter(id, ing, o.ingesterStats[id])
}

// pushIngestersToSupervisorLocked snapshots the current desired ingester set as
// ingestion specs (reusing cached adapters so unchanged ingesters keep a stable
// identity in the ingestion manager's no-flap reconcile) and hands it to the
// supervisor. Safe before or after Start: the ingestion manager starts
// newly-added ingesters immediately when running, otherwise at Start.
// Caller holds o.mu.
func (o *Orchestrator) pushIngestersToSupervisorLocked() error {
	if o.pipeline == nil {
		return nil
	}
	specs := make([]ingestion.IngesterSpec, 0, len(o.ingesters))
	for id, ing := range o.ingesters {
		adapter := o.ingesterAdapters[id]
		if adapter == nil {
			if o.ingesterStats[id] == nil {
				o.ingesterStats[id] = &IngesterStats{}
			}
			adapter = o.newIngesterAdapter(id, ing, o.ingesterStats[id])
			o.ingesterAdapters[id] = adapter
		}
		meta := o.ingesterMeta[id]
		specs = append(specs, ingestion.IngesterSpec{
			ID:       id,
			Ingester: adapter,
			Passive:  meta.Passive,
			Name:     meta.Name,
			Type:     meta.Type,
		})
	}
	return o.pipeline.ReconcileIngesters(specs)
}
