package app

import (
	"context"
	"log/slog"

	"gastrolog/internal/cluster"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

// startProtocolLevelRaiser runs the raiser that moves the cluster's active
// protocol level up once every cluster-ctl member reports a binary at the
// new level. It acts on the cluster-ctl leader only, on node-record changes
// (from the dispatcher) and on leadership changes, plus once now for any
// node-record change applied before the dispatcher was wired.
func startProtocolLevelRaiser(ctx context.Context, cfgStore system.Store, clusterSrv *cluster.Server, disp *configDispatcher, logger *slog.Logger) {
	raiser := raftlevel.NewRaiser(cfgStore, clusterSrv.IsLeader, func() ([]string, error) {
		return clusterMemberIDs(clusterSrv)
	}, logger)
	leaderCh := make(chan hraft.Observation, 4)
	clusterSrv.RegisterLeaderObserver(leaderCh)
	disp.setProtocolLevelWake(raiser.Wake)
	go raiser.Run(ctx, leaderCh)
	raiser.Wake()
}

// clusterMemberIDs lists every server in the cluster-ctl configuration,
// voters and learners alike.
func clusterMemberIDs(clusterSrv *cluster.Server) ([]string, error) {
	servers, err := clusterSrv.Servers()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(servers))
	for _, s := range servers {
		ids = append(ids, s.ID)
	}
	return ids, nil
}
