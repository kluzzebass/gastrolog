package cluster

import (
	"context"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/proto"
)

// FuzzEnrollRequestDeserialize fuzzes the Enroll RPC request deserialization
// and validation path. A malicious or buggy joining node could send arbitrary
// bytes as an EnrollRequest. The handler must not panic regardless of input.
func FuzzEnrollRequestDeserialize(f *testing.F) {
	// Seed with a valid request.
	valid := &gastrologv1.EnrollRequest{
		TokenSecret: "secret-token-abc123",
		NodeId:      []byte("node-joining-1"),
		NodeAddr:    "10.0.0.5:4565",
	}
	validBytes, _ := proto.Marshal(valid)
	f.Add(validBytes)

	// Seed with empty fields.
	emptyBytes, _ := proto.Marshal(&gastrologv1.EnrollRequest{})
	f.Add(emptyBytes)

	// Seed with oversized values.
	big := &gastrologv1.EnrollRequest{
		TokenSecret: string(make([]byte, 4096)),
		NodeId:      make([]byte, 1024),
		NodeAddr:    string(make([]byte, 1024)),
	}
	bigBytes, _ := proto.Marshal(big)
	f.Add(bigBytes)

	f.Add([]byte{0xff, 0x00, 0x80, 0x7f})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var req gastrologv1.EnrollRequest
		if err := proto.Unmarshal(data, &req); err != nil {
			return // invalid proto wire format, not a panic
		}

		// Exercise the enroll handler path with a no-op handler.
		// This tests that the request struct is safe to inspect after
		// deserialization from arbitrary bytes.
		s := &Server{}
		s.SetEnrollHandler(func(_ context.Context, r *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error) {
			// Access all fields to catch any deserialization issues.
			_ = r.GetTokenSecret()
			_ = r.GetNodeId()
			_ = r.GetNodeAddr()
			return &gastrologv1.EnrollResponse{}, nil
		})

		_, _ = s.enroll(context.Background(), &req)
	})
}
