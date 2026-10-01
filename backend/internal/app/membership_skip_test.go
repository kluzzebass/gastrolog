package app

// A node that restarts into a configuration already listing it where it says
// it is has nothing to ask the cluster for. Asking anyway commits a change
// that changes nothing and, worse, makes every restart depend on another node
// being up and answering — which is exactly what a rolling restart is not.

import (
	"testing"

	"gastrolog/internal/cluster"
)

func TestConfigurationLists(t *testing.T) {
	t.Parallel()

	const self = "node-2"
	const advertise = "gastrolog-2.headless:4566"

	configured := []cluster.RaftServer{
		{ID: "node-1", Address: "gastrolog-1.headless:4566", Suffrage: "voter"},
		{ID: self, Address: advertise, Suffrage: "voter"},
	}

	cases := []struct {
		name      string
		servers   []cluster.RaftServer
		nodeID    string
		advertise string
		want      bool
		why       string
	}{
		{"a restart into the same configuration", configured, self, advertise, true,
			"the cluster already has this node where it says it is"},
		{"a node the configuration has never heard of", configured, "node-9", "gastrolog-9.headless:4566", false,
			"a fresh joiner has to ask"},
		{"a node listed at its old address", configured, self, "10.0.0.7:4566", false,
			"peers would dial the address in the configuration, not this one"},
		{"an empty configuration", nil, self, advertise, false,
			"nothing has been replayed, so nothing is known"},
		{"a nonvoter already present", []cluster.RaftServer{{ID: self, Address: advertise, Suffrage: "nonvoter"}}, self, advertise, true,
			"a learner is in the configuration; the promoter upgrades it, not a join"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := configurationLists(tc.servers, tc.nodeID, tc.advertise); got != tc.want {
				t.Fatalf("got %v, but %s", got, tc.why)
			}
		})
	}
}

// Without a cluster server there is no configuration to consult, and
// answering "already there" would skip a join that has to happen.
func TestAlreadyInConfigurationWithoutACluster(t *testing.T) {
	t.Parallel()

	if alreadyInConfiguration(nil, "node-2", "gastrolog-2.headless:4566") {
		t.Fatal("claimed membership with no cluster to have been configured by")
	}
}
