package logging

import (
	"log/slog"

	"github.com/hashicorp/go-hclog"
)

// NewRaftGroupSlog returns a slog.Logger scoped to a Raft group. The group id
// is a structured attribute only — never embedded in the message string.
func NewRaftGroupSlog(logger *slog.Logger, groupID string) *slog.Logger {
	return logger.With("group", groupID)
}

// NewRaftGroupHclog adapts slog for hashicorp/raft on a named group. The group
// id is a structured attribute only — never embedded in the message string.
func NewRaftGroupHclog(logger *slog.Logger, groupID string) hclog.Logger {
	return NewHclogAdapter(NewRaftGroupSlog(logger, groupID))
}
