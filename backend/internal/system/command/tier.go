package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func putTierCmd(tier system.TierConfig) *gastrologv1.PutTierCommand {
	// Snapshots/commands send TierConfig without placements — placements
	// are stored separately in Runtime. Pass nil for the proto conversion.
	return &gastrologv1.PutTierCommand{
		Tier: convert.TierConfigToProto(tier, nil),
	}
}

// NewPutTier creates a SystemCommand for PutTier.
func NewPutTier(tier system.TierConfig) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutTier{PutTier: putTierCmd(tier)},
	}
}

// NewDeleteTier creates a SystemCommand for DeleteTier.
func NewDeleteTier(id glid.GLID, drain bool) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteTier{
			DeleteTier: &gastrologv1.DeleteTierCommand{Id: id.ToProto(), Drain: drain},
		},
	}
}

// ExtractPutTier converts a PutTierCommand back to a TierConfig.
func ExtractPutTier(cmd *gastrologv1.PutTierCommand) (system.TierConfig, error) {
	return convert.TierConfigFromProto(cmd.GetTier())
}

// ExtractPutTierPlacements extracts placements from a PutTierCommand.
func ExtractPutTierPlacements(cmd *gastrologv1.PutTierCommand) []system.VaultPlacement {
	return convert.TierPlacementsFromProto(cmd.GetTier())
}

// ExtractDeleteTier extracts the UUID from a DeleteTierCommand.
func ExtractDeleteTier(cmd *gastrologv1.DeleteTierCommand) (glid.GLID, error) {
	return glid.FromBytes(cmd.GetId()), nil
}

// NewSetVaultPlacements creates a SystemCommand for SetVaultPlacements.
func NewSetVaultPlacements(instID glid.GLID, placements []system.VaultPlacement) *gastrologv1.SystemCommand {
	pbPlacements := make([]*gastrologv1.VaultPlacement, len(placements))
	for i, p := range placements {
		pbPlacements[i] = &gastrologv1.VaultPlacement{
			StorageId: []byte(p.StorageID),
			Leader:    p.Leader,
		}
	}
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetVaultPlacements{
			SetVaultPlacements: &gastrologv1.SetVaultPlacementsCommand{
				TierId:     instID.ToProto(),
				Placements: pbPlacements,
			},
		},
	}
}

// NewSetSetupWizardDismissed creates a SystemCommand for SetSetupWizardDismissed.
func NewSetSetupWizardDismissed(dismissed bool) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetSetupWizardDismissed{
			SetSetupWizardDismissed: &gastrologv1.SetSetupWizardDismissedCommand{Dismissed: dismissed},
		},
	}
}

// ExtractSetVaultPlacements converts a SetVaultPlacementsCommand back.
func ExtractSetVaultPlacements(cmd *gastrologv1.SetVaultPlacementsCommand) (glid.GLID, []system.VaultPlacement, error) {
	instID := glid.FromBytes(cmd.GetTierId())
	placements := make([]system.VaultPlacement, len(cmd.GetPlacements()))
	for i, p := range cmd.GetPlacements() {
		placements[i] = system.VaultPlacement{
			StorageID: string(p.GetStorageId()),
			Leader:    p.GetLeader(),
		}
	}
	return instID, placements, nil
}

// NewSetIngesterAlive creates a SystemCommand for SetIngesterAlive.
func NewSetIngesterAlive(ingesterID glid.GLID, nodeID string, alive bool) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetIngesterAlive{
			SetIngesterAlive: &gastrologv1.SetIngesterAliveCommand{
				IngesterId: ingesterID.ToProto(),
				NodeId:     nodeID,
				Alive:      alive,
			},
		},
	}
}

// NewSetIngesterCheckpoint creates a SystemCommand for SetIngesterCheckpoint.
func NewSetIngesterCheckpoint(ingesterID glid.GLID, data []byte) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetIngesterCheckpoint{
			SetIngesterCheckpoint: &gastrologv1.SetIngesterCheckpointCommand{
				IngesterId: ingesterID.ToProto(),
				Data:       data,
			},
		},
	}
}

// NewSetIngesterAssignment creates a SystemCommand for SetIngesterAssignment.
func NewSetIngesterAssignment(ingesterID glid.GLID, nodeID string) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetIngesterAssignment{
			SetIngesterAssignment: &gastrologv1.SetIngesterAssignmentCommand{
				IngesterId: ingesterID.ToProto(),
				NodeId:     nodeID,
			},
		},
	}
}
