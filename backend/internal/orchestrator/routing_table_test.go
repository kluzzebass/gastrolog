package orchestrator

import (
	"slices"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/system"
)

// The routing table carries each route's distribution and splits its
// destinations: an enabled vault accepts records, a disabled one receives
// nothing and is listed so its drops are counted.
func TestBuildRoutingTableCarriesDistributionAndDisabledDestinations(t *testing.T) {
	t.Parallel()
	on, off, unknown := glid.New(), glid.New(), glid.New()
	routeID := glid.New()
	sys := &system.System{Config: system.Config{
		Vaults: []system.VaultConfig{
			{ID: on, Name: "on", Type: system.VaultTypeFile, Enabled: true},
			{ID: off, Name: "off", Type: system.VaultTypeFile, Enabled: false},
		},
		Routes: []system.RouteConfig{{
			ID: routeID, Name: "spread", Enabled: true,
			Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
			Destinations: []glid.GLID{off, on, unknown},
			Distribution: system.DistributionFailover,
		}},
	}}
	table, err := buildRoutingTable(sys)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := table.MatchRoute(nil, routing.IngestSource(nil))
	if r == nil {
		t.Fatal("the catch-all route did not match")
	}
	if r.Distribution != routing.DistributeFailover {
		t.Fatalf("distribution = %q, want failover", r.Distribution)
	}
	if !slices.Equal(r.VaultIDs, []glid.GLID{on, unknown}) {
		t.Fatalf("accepting destinations = %v, want the enabled one and the one not in config, in order", r.VaultIDs)
	}
	if !slices.Equal(r.DisabledVaultIDs, []glid.GLID{off}) {
		t.Fatalf("disabled destinations = %v, want the disabled vault", r.DisabledVaultIDs)
	}

	sys.Config.Routes[0].Distribution = "random"
	if _, err := buildRoutingTable(sys); err == nil {
		t.Fatal("an unknown distribution compiled")
	}
}
