package collection

import (
	"errors"
	"os"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryableCollectErr reports whether a collect pass failure is an expected
// catch-up race at high ingest: the registry still lists a segment but no peer
// has bytes yet (no holder acks), or a holder already purged head/ after seal.
// The next vault-ctl publish or leadership wake retries; logging at Warn drowns signal.
func retryableCollectErr(err error) bool {
	if err == nil {
		return false
	}
	for _, sub := range unpackJoinErrors(err) {
		if !retryableCollectSuberr(sub) {
			return false
		}
	}
	return true
}

func unpackJoinErrors(err error) []error {
	type unwrapper interface {
		Unwrap() []error
	}
	if u, ok := err.(unwrapper); ok {
		if subs := u.Unwrap(); len(subs) > 0 {
			return subs
		}
	}
	return []error{err}
}

func retryableCollectSuberr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return true
	}
	msg := err.Error()
	if strings.Contains(msg, "no remote holder for segment") {
		return true
	}
	if strings.Contains(msg, "not in vault-ctl registry") {
		return true
	}
	if strings.Contains(msg, "serve segment") && strings.Contains(msg, "segment not found") {
		return true
	}
	if u, ok := err.(interface{ Unwrap() error }); ok {
		if inner := u.Unwrap(); inner != nil {
			return retryableCollectSuberr(inner)
		}
	}
	return false
}
