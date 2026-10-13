package cluster

import (
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrVaultNotReady is returned when a vault's replicated metadata has not
// applied far enough on the node serving it for reads or writes. A peer's
// not-ready answer still matches it after the hop, so a caller treats a vault
// that is not ready on another node exactly as one that is not ready on its
// own.
var ErrVaultNotReady = errors.New("vault not ready")

// vaultNotReadyReason marks a status as a not-ready answer. The status code
// alone cannot: a peer whose executor is not wired yet also answers
// Unavailable, and that says nothing about any one vault.
const (
	vaultNotReadyReason = "VAULT_NOT_READY"
	statusDomain        = "gastrolog"
)

// vaultNotReadyStatus is the status a handler returns for err, a not-ready
// vault: Unavailable, typed by its ErrorInfo reason.
func vaultNotReadyStatus(what string, err error) error {
	st := status.New(codes.Unavailable, fmt.Sprintf("%s: %v", what, err))
	typed, detailErr := st.WithDetails(&errdetails.ErrorInfo{Reason: vaultNotReadyReason, Domain: statusDomain})
	if detailErr != nil {
		return st.Err()
	}
	return typed.Err()
}

// isVaultNotReadyStatus reports whether st is a not-ready answer.
func isVaultNotReadyStatus(st *status.Status) bool {
	if st.Code() != codes.Unavailable {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == vaultNotReadyReason && info.GetDomain() == statusDomain {
			return true
		}
	}
	return false
}
