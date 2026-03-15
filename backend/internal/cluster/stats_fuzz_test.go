package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FuzzNodeStatsDeserialize fuzzes proto deserialization of NodeStats messages.
// In a cluster, NodeStats bytes arrive over the wire from peers and are
// unmarshalled before being stored in PeerState. Malformed bytes must not
// cause a panic.
func FuzzNodeStatsDeserialize(f *testing.F) {
	// Seed with a valid NodeStats message.
	valid := &gastrologv1.NodeStats{
		NodeName:    "node-1",
		Version:     "1.0.0",
		CpuPercent:  42.5,
		MemoryRss:   1024 * 1024 * 512,
		Goroutines:  100,
		RaftState:   "Leader",
		ApiAddress:  ":4564",
		Vaults: []*gastrologv1.VaultStats{
			{Id: "abc", Name: "default", RecordCount: 1000, DataBytes: 999},
		},
		Ingesters: []*gastrologv1.IngesterNodeStats{
			{Id: "ing-1", Name: "syslog", MessagesIngested: 500, Running: true},
		},
	}
	validBytes, _ := proto.Marshal(valid)
	f.Add(validBytes)

	// Seed with an empty message.
	emptyBytes, _ := proto.Marshal(&gastrologv1.NodeStats{})
	f.Add(emptyBytes)

	// Seed with garbage.
	f.Add([]byte{0xff, 0xfe, 0x00, 0x01, 0x80})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var ns gastrologv1.NodeStats
		if err := proto.Unmarshal(data, &ns); err != nil {
			return // invalid proto is fine, just must not panic
		}

		// Exercise the PeerState path that processes incoming NodeStats.
		ps := NewPeerState(30 * time.Second)
		msg := &gastrologv1.BroadcastMessage{
			SenderId:  "fuzz-sender",
			Timestamp: timestamppb.Now(),
			Payload:   &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &ns},
		}
		ps.HandleBroadcast(msg)

		// Exercise read paths that iterate over the deserialized data.
		_ = ps.Get("fuzz-sender")
		ps.FindVaultStats("any-vault")
		ps.FindIngesterStats("any-ingester")
		ps.AggregateRouteStats()
	})
}
