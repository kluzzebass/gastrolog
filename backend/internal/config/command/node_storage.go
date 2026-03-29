package command

import (
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

func setNodeStorageConfigCmd(cfg config.NodeStorageConfig) *gastrologv1.SetNodeStorageConfigCommand {
	storages := make([]*gastrologv1.FileStorageCommand, len(cfg.FileStorages))
	for i, a := range cfg.FileStorages {
		storages[i] = &gastrologv1.FileStorageCommand{
			Id:                a.ID.String(),
			StorageClass:      a.StorageClass,
			Name:              a.Name,
			Path:              a.Path,
			MemoryBudgetBytes: a.MemoryBudgetBytes,
		}
	}
	return &gastrologv1.SetNodeStorageConfigCommand{
		NodeId: cfg.NodeID,
		FileStorages:  storages,
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
	var storages []config.FileStorage
	for _, a := range cmd.GetFileStorages() {
		id, err := uuid.Parse(a.GetId())
		if err != nil {
			return config.NodeStorageConfig{}, fmt.Errorf("parse file storage id: %w", err)
		}
		storages = append(storages, config.FileStorage{
			ID:                id,
			StorageClass:      a.GetStorageClass(),
			Name:              a.GetName(),
			Path:              a.GetPath(),
			MemoryBudgetBytes: a.GetMemoryBudgetBytes(),
		})
	}
	return config.NodeStorageConfig{
		NodeID: cmd.GetNodeId(),
		FileStorages:  storages,
	}, nil
}
