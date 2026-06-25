package logging

import (
	"log/slog"

	"github.com/hashicorp/go-hclog"
)

// NewRaftGroupHclog adapts slog for hashicorp/raft on a named group. The group
// id is a structured attribute only — never embedded in the message string.
func NewRaftGroupHclog(logger *slog.Logger, groupID string) hclog.Logger {
	return NewHclogAdapter(logger.With("group", groupID))
}
