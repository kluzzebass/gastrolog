package cluster

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const invalidateGracePeriod = 5 * time.Second

// shouldInvalidate reports whether err indicates the underlying gRPC
// connection itself is broken and should be discarded.
func shouldInvalidate(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return status.Code(err) == codes.Unavailable
}
