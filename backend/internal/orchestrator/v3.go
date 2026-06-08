package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator/v3pipeline"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/system"
)

// v3OriginSpec builds the supervisor VaultSpec for registering a destination
// vault as an Origin (segmentation + distribution) on this node. While Rubicon
// B keeps segments local, the publisher is a no-op and the vault is never a
// local Home, so completed segments accumulate under root without being pulled.
func v3OriginSpec(vaultID glid.GLID, root string) v3pipeline.VaultSpec {
	return v3pipeline.VaultSpec{
		VaultID:    vaultID,
		Origin:     true,
		OriginRoot: root,
		Publisher:  noopPublisher{},
	}
}

// originRoot returns the V3 segmentation root for a vault: the directory that
// holds the vault's working/ and completed/ segment areas on this node.
//
// TODO(gastrolog-jiwlf): segment storage location is currently a single base
// dir under the node home. Make it configurable per node/storage class.
func (o *Orchestrator) originRoot(vaultID glid.GLID) string {
	base := o.segmentsDir
	if base == "" {
		base = "segments"
	}
	return filepath.Join(base, vaultID.String())
}

// noopPublisher is the distribution publisher used while Rubicon B keeps
// completed segments local: it accepts and drops publish metadata. Slice C
// swaps in the VaultCtlPublisher so completed segments are announced to the
// vault-ctl log and pulled by home nodes.
type noopPublisher struct{}

var _ distribution.Publisher = noopPublisher{}

func (noopPublisher) Publish(context.Context, distribution.Metadata) error { return nil }

// buildRoutingTable compiles a V3 routing table from the cluster-wide route
// config. V3 routing is vault-only: each enabled route maps its match
// expression to its destination vault IDs, independent of node placement —
// every node is an Origin for every destination vault, so records are written
// to a local durable segment wherever they are ingested.
func buildRoutingTable(sys *system.System) (*routing.Table, error) {
	if sys == nil {
		return routing.NewTable(nil), nil
	}
	var routes []*routing.Route
	for _, rc := range sys.Config.Routes {
		if !rc.Enabled {
			continue
		}
		r, err := routing.CompileRoute(rc.ID, rc.Name, rc.Priority, rc.MatchExpression(), rc.Destinations)
		if err != nil {
			return nil, fmt.Errorf("compile route %s: %w", rc.Name, err)
		}
		routes = append(routes, r)
	}
	return routing.NewTable(routes), nil
}

// destinationVaults returns the set of vault IDs that any enabled route targets
// — the vaults this node must Origin-register so matched records have a local
// segmentation queue to land in.
func destinationVaults(sys *system.System) map[glid.GLID]struct{} {
	dests := make(map[glid.GLID]struct{})
	if sys == nil {
		return dests
	}
	for _, rc := range sys.Config.Routes {
		if !rc.Enabled {
			continue
		}
		for _, vid := range rc.Destinations {
			dests[vid] = struct{}{}
		}
	}
	return dests
}

// reloadV3FromConfig republishes the V3 routing table and reconciles the set of
// Origin-registered vaults to match the destinations of all enabled routes.
// Must be called with o.mu held (it mutates o.v3Origins).
//
// Every route-target vault is registered as an Origin on this node so a matched
// record is always durably written to a local segment, regardless of where the
// vault is homed — cross-node collection pulls those segments in a later slice.
func (o *Orchestrator) reloadV3FromConfig(sys *system.System) error {
	if o.v3 == nil {
		return nil
	}

	table, err := buildRoutingTable(sys)
	if err != nil {
		return err
	}
	o.v3.SetRoutingTable(table)

	desired := destinationVaults(sys)

	// Unregister vaults that are no longer any route's destination.
	for vid := range o.v3Origins {
		if _, ok := desired[vid]; !ok {
			o.v3.UnregisterVault(vid)
			delete(o.v3Origins, vid)
		}
	}

	// Register newly targeted destination vaults as Origins.
	for vid := range desired {
		if _, ok := o.v3Origins[vid]; ok {
			continue
		}
		if err := o.v3.RegisterVault(v3OriginSpec(vid, o.originRoot(vid))); err != nil {
			return fmt.Errorf("register origin vault %s: %w", vid, err)
		}
		o.v3Origins[vid] = struct{}{}
	}

	return nil
}
