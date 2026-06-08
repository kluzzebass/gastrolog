package orchestrator

import (
	"errors"
	"fmt"

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
	Build   func() (Ingester, error)
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
		meta := ingesterInfo{Name: d.Name, Type: d.Type, Passive: d.Passive}
		if _, had := o.ingesters[id]; had && o.ingesterMeta[id] == meta {
			continue // unchanged — keep the running instance and cached adapter
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

// setIngesterLocked installs (or replaces) an ingester in the desired set and
// (re)builds its stable V3 adapter. Caller holds o.mu.
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
	if o.v3 == nil {
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
	return o.v3.ReconcileIngesters(specs)
}
