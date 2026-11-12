package raftlevel

import (
	"log/slog"
	"os"
)

// Unapplicable is what an FSM calls when it meets a committed entry it cannot
// apply: an entry it cannot decode, a command type it does not know, or an
// active level above its binary's. The FSM neither applies the entry nor
// advances past it.
type Unapplicable func(group string, index uint64, reason error)

// FailStop returns the production Unapplicable: it logs the entry to logger
// and stops the process. hashicorp/raft counts every entry handed to
// FSM.Apply as applied, whatever Apply returns, so returning an error would
// carry the node on past the entry with state its peers do not have; an Apply
// that never returns would hang Raft shutdown instead. The node stays down
// with this reason until it runs a binary that can apply the entry.
//
// A nil logger writes to standard error: the reason must never be lost.
func FailStop(logger *slog.Logger) Unapplicable {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return func(group string, index uint64, reason error) {
		logger.Error("stopping: this binary cannot apply a committed raft entry",
			"group", group, "index", index, "binary_protocol_level", Binary, "reason", reason)
		os.Exit(1) //nolint:revive // the only way not to carry on past an entry raft considers applied
	}
}
