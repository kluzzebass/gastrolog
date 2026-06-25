package collection

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryableCollectErr(t *testing.T) {
	t.Parallel()

	retryable := []error{
		fmt.Errorf("no remote holder for segment abc"),
		fmt.Errorf("segment xyz not in vault-ctl registry"),
		status.Error(codes.NotFound, "serve segment vault/seg: segment not found"),
		fmt.Errorf("pull from node-a: %w", status.Error(codes.NotFound, "serve segment v/s: segment not found")),
		os.ErrNotExist,
		errors.Join(
			fmt.Errorf("pull from n1: %w", status.Error(codes.NotFound, "missing")),
			fmt.Errorf("no remote holder for segment s"),
		),
	}
	for _, err := range retryable {
		if !retryableCollectErr(err) {
			t.Errorf("expected retryable: %v", err)
		}
	}

	nonRetryable := []error{
		fmt.Errorf("vault-ctl FSM required"),
		fmt.Errorf("disk full"),
		status.Error(codes.Internal, "boom"),
		errors.Join(
			fmt.Errorf("no remote holder for segment s"),
			fmt.Errorf("permission denied"),
		),
	}
	for _, err := range nonRetryable {
		if retryableCollectErr(err) {
			t.Errorf("expected non-retryable: %v", err)
		}
	}
}
