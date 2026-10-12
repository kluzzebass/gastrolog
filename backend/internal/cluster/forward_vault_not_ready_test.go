package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var errVaultMetadataNotReady = fmt.Errorf("%w: vault v metadata not ready", ErrVaultNotReady)

func notReadyForSearch(inIterator bool) func(*Server) {
	return func(s *Server) { s.SetSearchExecutor(searchExecutorFailing(errVaultMetadataNotReady, inIterator)) }
}

func notReadyForContext(s *Server) {
	s.SetContextExecutor(func(context.Context, glid.GLID, chunk.ChunkID, uint64, int, int) ([]chunk.Record, chunk.Record, []chunk.Record, error) {
		return nil, chunk.Record{}, nil, errVaultMetadataNotReady
	})
}

func forwardGetContextRequest() *gastrologv1.ForwardGetContextRequest {
	return &gastrologv1.ForwardGetContextRequest{
		VaultId: glid.New().ToProto(),
		ChunkId: glid.GLID(chunk.NewChunkID()).ToProto(),
	}
}

// A peer that answers that a vault is not ready is heard as exactly that on
// the calling node: an Unavailable that matches ErrVaultNotReady, over the
// connection that carried it, which stays in the pool.
func TestForwardedVaultNotReadyMatchesSentinel(t *testing.T) {
	type forwardCall struct {
		name  string
		setup func(*Server)
		run   func(*SearchForwarder) error
	}
	var calls []forwardCall
	for _, call := range searchCalls {
		for _, inIterator := range []bool{false, true} {
			calls = append(calls, forwardCall{
				name:  fmt.Sprintf("%s/in iterator %v", call.name, inIterator),
				setup: notReadyForSearch(inIterator),
				run: func(sf *SearchForwarder) error {
					return call.run(sf, "owner", forwardSearchRequest())
				},
			})
		}
	}
	calls = append(calls, forwardCall{
		name:  "GetContext",
		setup: notReadyForContext,
		run: func(sf *SearchForwarder) error {
			_, err := sf.GetContext(context.Background(), "owner", forwardGetContextRequest())
			return err
		},
	})

	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			sf, conns := callerOf(t, map[string]*Server{"owner": startPeer(t, "owner", call.setup)})

			err := call.run(sf)
			if !errors.Is(err, ErrVaultNotReady) {
				t.Fatalf("error %v does not match ErrVaultNotReady", err)
			}
			if got := status.Code(err); got != codes.Unavailable {
				t.Fatalf("code = %v, want Unavailable (err: %v)", got, err)
			}
			before := serviceConnIDsTo(conns, "owner")
			if len(before) != 1 {
				t.Fatalf("service connections after the answer = %v, want the one that carried it", before)
			}
			if err := call.run(sf); !errors.Is(err, ErrVaultNotReady) {
				t.Fatalf("second call: %v does not match ErrVaultNotReady", err)
			}
			if after := serviceConnIDsTo(conns, "owner"); !slices.Equal(after, before) {
				t.Fatalf("service connections after the next call = %v, want %v reused", after, before)
			}
		})
	}
}

// Nothing but a peer's not-ready answer matches ErrVaultNotReady: not a query
// failure, not a peer with no executor wired (also Unavailable, about no vault
// in particular), and not a transport that died under the call.
func TestOnlyANotReadyAnswerMatchesSentinel(t *testing.T) {
	for _, call := range searchCalls {
		t.Run(call.name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			broken := startBlockingPeer(t, "broken", entered)
			sf, _ := callerOf(t, map[string]*Server{
				"broken":    broken,
				"failing":   startPeer(t, "failing", func(s *Server) { s.SetSearchExecutor(searchExecutorFailing(errors.New("disk on fire"), false)) }),
				"exhausted": startPeer(t, "exhausted", failingWithMemoryOverflow),
				"bare":      startPeer(t, "bare", func(*Server) {}),
			})

			for node, code := range map[string]codes.Code{"failing": codes.Internal, "exhausted": codes.ResourceExhausted, "bare": codes.Unavailable} {
				err := call.run(sf, node, forwardSearchRequest())
				if status.Code(err) != code {
					t.Fatalf("%s: code = %v, want %v (err: %v)", node, status.Code(err), code, err)
				}
				if errors.Is(err, ErrVaultNotReady) {
					t.Errorf("%s: %v matches ErrVaultNotReady", node, err)
				}
			}

			done := make(chan error, 1)
			go func() { done <- call.run(sf, "broken", forwardSearchRequest()) }()
			<-entered
			broken.grpcSrv.Stop()
			err := <-done
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("broken: code = %v, want Unavailable (err: %v)", status.Code(err), err)
			}
			if errors.Is(err, ErrVaultNotReady) {
				t.Errorf("broken: transport failure %v matches ErrVaultNotReady", err)
			}
		})
	}
}

// The not-ready reason is honoured only on a status the peer itself returned,
// with the code and domain the handler sets.
func TestVaultNotReadyStatusShape(t *testing.T) {
	answered := metadata.Pairs(peerStatusTrailerKey, "1")
	typed := vaultNotReadyStatus("search", errVaultMetadataNotReady)

	if !errors.Is(markPeerStatus(typed, answered), ErrVaultNotReady) {
		t.Error("a peer's not-ready answer does not match ErrVaultNotReady")
	}
	if errors.Is(markPeerStatus(typed, nil), ErrVaultNotReady) {
		t.Error("a not-ready status no peer answered with matches ErrVaultNotReady")
	}

	withReason := func(code codes.Code, domain string) error {
		st, err := status.New(code, "x").WithDetails(&errdetails.ErrorInfo{Reason: vaultNotReadyReason, Domain: domain})
		if err != nil {
			t.Fatal(err)
		}
		return markPeerStatus(st.Err(), answered)
	}
	if errors.Is(withReason(codes.Internal, statusDomain), ErrVaultNotReady) {
		t.Error("an Internal status carrying the reason matches ErrVaultNotReady")
	}
	if errors.Is(withReason(codes.Unavailable, "elsewhere"), ErrVaultNotReady) {
		t.Error("a reason from another domain matches ErrVaultNotReady")
	}
	if errors.Is(markPeerStatus(status.Error(codes.Unavailable, errVaultMetadataNotReady.Error()), answered), ErrVaultNotReady) {
		t.Error("an Unavailable whose message reads not ready, without the reason, matches ErrVaultNotReady")
	}
}
