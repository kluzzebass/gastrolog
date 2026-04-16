package command

import (
	"gastrolog/internal/glid"
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
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
func ExtractPutTierPlacements(cmd *gastrologv1.PutTierCommand) []system.TierPlacement {
	return convert.TierPlacementsFromProto(cmd.GetTier())
}

// ExtractDeleteTier extracts the UUID from a DeleteTierCommand.
func ExtractDeleteTier(cmd *gastrologv1.DeleteTierCommand) (glid.GLID, error) {
	return glid.FromBytes(cmd.GetId()), nil
}

// NewSetTierPlacements creates a SystemCommand for SetTierPlacements.
func NewSetTierPlacements(tierID glid.GLID, placements []system.TierPlacement) *gastrologv1.SystemCommand {
	pbPlacements := make([]*gastrologv1.TierPlacement, len(placements))
	for i, p := range placements {
		pbPlacements[i] = &gastrologv1.TierPlacement{
			StorageId: []byte(p.StorageID),
			Leader:    p.Leader,
		}
	}
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_SetTierPlacements{
			SetTierPlacements: &gastrologv1.SetTierPlacementsCommand{
				TierId:     tierID.ToProto(),
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

// ExtractSetTierPlacements converts a SetTierPlacementsCommand back.
func ExtractSetTierPlacements(cmd *gastrologv1.SetTierPlacementsCommand) (glid.GLID, []system.TierPlacement, error) {
	tierID := glid.FromBytes(cmd.GetTierId())
	placements := make([]system.TierPlacement, len(cmd.GetPlacements()))
	for i, p := range cmd.GetPlacements() {
		placements[i] = system.TierPlacement{
			StorageID: string(p.GetStorageId()),
			Leader:    p.GetLeader(),
		}
	}
	return tierID, placements, nil
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
