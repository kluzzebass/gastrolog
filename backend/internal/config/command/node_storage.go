package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/convert"
)

func setNodeStorageConfigCmd(cfg config.NodeStorageConfig) *gastrologv1.SetNodeStorageConfigCommand {
	return &gastrologv1.SetNodeStorageConfigCommand{
		NodeStorage: convert.NodeStorageConfigToProto(cfg),
	}
}

// NewSetNodeStorageConfig creates a ConfigCommand for SetNodeStorageConfig.
func NewSetNodeStorageConfig(cfg config.NodeStorageConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_SetNodeStorageConfig{
			SetNodeStorageConfig: setNodeStorageConfigCmd(cfg),
		},
	}
}

// ExtractSetNodeStorageConfig converts a SetNodeStorageConfigCommand back.
func ExtractSetNodeStorageConfig(cmd *gastrologv1.SetNodeStorageConfigCommand) (config.NodeStorageConfig, error) {
	return convert.NodeStorageConfigFromProto(cmd.GetNodeStorage()), nil
}
