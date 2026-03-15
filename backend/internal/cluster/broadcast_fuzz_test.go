package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FuzzBroadcastMessageDeserialize fuzzes the BroadcastMessage deserialization
// and dispatch path. Gossip broadcasts arrive as serialized proto bytes from
// peers. The broadcast handler and all subscribers must not panic on any input.
func FuzzBroadcastMessageDeserialize(f *testing.F) {
	// Seed: valid BroadcastMessage with NodeStats payload.
	statsMsg := &gastrologv1.BroadcastMessage{
		SenderId:  "node-1",
		Timestamp: timestamppb.Now(),
		Payload: &gastrologv1.BroadcastMessage_NodeStats{
			NodeStats: &gastrologv1.NodeStats{
				NodeName:   "test-node",
				CpuPercent: 55.0,
				Goroutines: 42,
			},
		},
	}
	statsBytes, _ := proto.Marshal(statsMsg)
	f.Add(statsBytes)

	// Seed: valid BroadcastMessage with NodeJobs payload.
	jobsMsg := &gastrologv1.BroadcastMessage{
		SenderId:  "node-2",
		Timestamp: timestamppb.Now(),
		Payload: &gastrologv1.BroadcastMessage_NodeJobs{
			NodeJobs: &gastrologv1.NodeJobs{
				Jobs: []*gastrologv1.Job{
					{Id: "job-1"},
				},
			},
		},
	}
	jobsBytes, _ := proto.Marshal(jobsMsg)
	f.Add(jobsBytes)

	// Seed: empty message.
	emptyBytes, _ := proto.Marshal(&gastrologv1.BroadcastMessage{})
	f.Add(emptyBytes)

	// Seed: no payload (nil oneof).
	noPaylod := &gastrologv1.BroadcastMessage{
		SenderId:  "node-3",
		Timestamp: timestamppb.Now(),
	}
	noPayloadBytes, _ := proto.Marshal(noPaylod)
	f.Add(noPayloadBytes)

	f.Add([]byte{0xde, 0xad, 0xbe, 0xef})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg gastrologv1.BroadcastMessage
		if err := proto.Unmarshal(data, &msg); err != nil {
			return
		}

		// Exercise the subscriber dispatch path.
		var reg subscriberRegistry
		called := false
		reg.subscribe(func(m *gastrologv1.BroadcastMessage) {
			called = true
			// Access fields that subscribers typically read.
			_ = m.GetSenderId()
			_ = m.GetTimestamp()
			_ = m.GetNodeStats()
			_ = m.GetNodeJobs()
		})
		reg.dispatch(&msg)

		if !called {
			t.Fatal("subscriber was not called")
		}

		// Also exercise PeerState.HandleBroadcast which is the primary
		// subscriber in production.
		ps := NewPeerState(30 * time.Second)
		ps.HandleBroadcast(&msg)
	})
}
