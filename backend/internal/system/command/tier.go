package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/convert"
	"gastrolog/internal/system"

	"github.com/google/uuid"
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
func NewDeleteTier(id uuid.UUID, drain bool) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteTier{
			DeleteTier: &gastrologv1.DeleteTierCommand{Id: id.String(), Drain: drain},
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
func ExtractDeleteTier(cmd *gastrologv1.DeleteTierCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}
