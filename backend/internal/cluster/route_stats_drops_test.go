package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Per-vault route drops sum across peers like matched counts, so a disabled
// destination's drops on every node reach the cluster-wide route stats.
func TestAggregateRouteStatsSumsDropsAcrossPeers(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(30*time.Second, 0)
	vault := []byte("vault-0123456789")
	for i, sender := range []string{"node-a", "node-b"} {
		ps.HandleBroadcast(&gastrologv1.BroadcastMessage{
			SenderId:  []byte(sender),
			Timestamp: timestamppb.Now(),
			Payload: &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &gastrologv1.NodeStats{
				RouteVaultStats: []*gastrologv1.VaultRouteStats{{
					VaultId: vault, RecordsMatched: int64(10 * (i + 1)), RecordsDropped: int64(3 * (i + 1)),
				}},
			}},
		})
	}
	_, _, _, _, vaults, _ := ps.AggregateRouteStats()
	if len(vaults) != 1 {
		t.Fatalf("vault stats = %v, want one vault", vaults)
	}
	if got := vaults[0]; got.RecordsMatched != 30 || got.RecordsDropped != 9 {
		t.Fatalf("aggregated matched=%d dropped=%d, want 30 and 9", got.RecordsMatched, got.RecordsDropped)
	}
}
