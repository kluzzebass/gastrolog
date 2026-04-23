package cluster

import "errors"

// ErrForwardTargetNotReady is wrapped by the app-layer cluster record appender
// when a peer's orchestrator.Append returns orchestrator.ErrVaultNotReady
// (placement churn, no local tier instances yet, or tier FSM not caught up).
// ForwardRecords handlers test for this with errors.Is and log at debug instead
// of warn so a mis-targeted or not-yet-ready peer does not flood logs.
var ErrForwardTargetNotReady = errors.New("forward target vault not ready")
