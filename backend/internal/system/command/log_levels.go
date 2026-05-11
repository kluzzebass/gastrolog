package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/system"
)

// NewPutLogLevels builds a SystemCommand carrying a LogLevelConfig
// replacement (gastrolog-3flfp). One Raft commit atomically replaces the
// whole rule set across every node.
func NewPutLogLevels(cfg system.LogLevelConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutLogLevels{
			PutLogLevels: &gastrologv1.PutLogLevelsCommand{
				Config: convert.LogLevelConfigToProto(cfg),
			},
		},
	}
}

// ExtractPutLogLevels converts a PutLogLevelsCommand back to the Go type.
// Nil-safe — both nil cmd and nil cmd.Config produce the zero-value
// LogLevelConfig (effectively "reset to defaults").
func ExtractPutLogLevels(cmd *gastrologv1.PutLogLevelsCommand) (system.LogLevelConfig, error) {
	if cmd == nil {
		return system.LogLevelConfig{}, nil
	}
	return convert.LogLevelConfigFromProto(cmd.GetConfig()), nil
}
