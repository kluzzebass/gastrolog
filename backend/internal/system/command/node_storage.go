package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/convert"
)

func setNodeStorageConfigCmd(cfg system.NodeStorageConfig) *gastrologv1.SetNodeStorageConfigCommand {
	return &gastrologv1.SetNodeStorageConfigCommand{
		NodeStorage: convert.NodeStorageConfigToProto(cfg),
	}
}

// NewSetNodeStorageConfig creates a ConfigCommand for SetNodeStorageConfig.
func NewSetNodeStorageConfig(cfg system.NodeStorageConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetNodeStorageConfig{
			SetNodeStorageConfig: setNodeStorageConfigCmd(cfg),
		},
	}
}

// ExtractSetNodeStorageConfig converts a SetNodeStorageConfigCommand back.
func ExtractSetNodeStorageConfig(cmd *gastrologv1.SetNodeStorageConfigCommand) (system.NodeStorageConfig, error) {
	return convert.NodeStorageConfigFromProto(cmd.GetNodeStorage()), nil
}
