package command

import (
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

func setNodeStorageConfigCmd(cfg config.NodeStorageConfig) *gastrologv1.SetNodeStorageConfigCommand {
	areas := make([]*gastrologv1.StorageAreaCommand, len(cfg.Areas))
	for i, a := range cfg.Areas {
		areas[i] = &gastrologv1.StorageAreaCommand{
			Id:                a.ID.String(),
			StorageClass:      a.StorageClass,
			Label:             a.Label,
			Path:              a.Path,
			MemoryBudgetBytes: a.MemoryBudgetBytes,
		}
	}
	return &gastrologv1.SetNodeStorageConfigCommand{
		NodeId: cfg.NodeID,
		Areas:  areas,
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

// ExtractSetNodeStorageConfig converts a SetNodeStorageConfigCommand back to a NodeStorageConfig.
func ExtractSetNodeStorageConfig(cmd *gastrologv1.SetNodeStorageConfigCommand) (config.NodeStorageConfig, error) {
	var areas []config.StorageArea
	for _, a := range cmd.GetAreas() {
		id, err := uuid.Parse(a.GetId())
		if err != nil {
			return config.NodeStorageConfig{}, fmt.Errorf("parse storage area id: %w", err)
		}
		areas = append(areas, config.StorageArea{
			ID:                id,
			StorageClass:      a.GetStorageClass(),
			Label:             a.GetLabel(),
			Path:              a.GetPath(),
			MemoryBudgetBytes: a.GetMemoryBudgetBytes(),
		})
	}
	return config.NodeStorageConfig{
		NodeID: cmd.GetNodeId(),
		Areas:  areas,
	}, nil
}
