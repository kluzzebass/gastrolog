// Package panicguard confines a panic to the goroutine that raised it.
//
// An unrecovered panic in any goroutine ends the whole process. A node serves
// every vault placed on it and leads Raft groups for others, so without a
// guard a panic in one connection handler or one detached query goroutine
// costs all of them. Guarding at the goroutine boundary costs one connection
// or one request instead.
//
// Guard only a goroutine that owns everything it was mutating when it
// panicked. A panic that leaves shared state half-written must escape: a
// process that dies is recoverable, a cluster that keeps serving from
// corrupt state is not.
package panicguard

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// Recover recovers a panic in the calling goroutine and logs it with the
// panicking goroutine's stack. Defer it as the outermost statement of a
// goroutine whose only correct response to a panic is to stop.
//
// what names the goroutine class ("RELP connection", "search histogram");
// attrs carry the context needed to find the input that caused it.
func Recover(logger *slog.Logger, what string, attrs ...any) {
	if v := recover(); v != nil {
		Log(logger, what, v, attrs...)
	}
}

// Call runs fn and returns its error, converting a panic into an error so
// the caller's existing failure path handles it. Use it where a stopped
// goroutine must be reported rather than silently absorbed — a listener
// loop whose exit the ingester manager retries, for instance.
func Call(logger *slog.Logger, what string, fn func() error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			Log(logger, what, v)
			err = fmt.Errorf("%s panicked: %v", what, v)
		}
	}()
	return fn()
}

// Log records an already-recovered panic value. Call it from inside the
// deferred function that recovered, while the panicking frames are still on
// the stack, so the logged stack points at the panic and not at the guard.
//
// The stack is part of the record on purpose: a panic that is swallowed
// without one is harder to fix than the crash it replaced.
func Log(logger *slog.Logger, what string, v any, attrs ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	all := make([]any, 0, len(attrs)+6)
	all = append(all, "goroutine", what, "panic", fmt.Sprint(v))
	all = append(all, attrs...)
	all = append(all, "stack", string(debug.Stack()))
	logger.Error("recovered panic", all...)
}
