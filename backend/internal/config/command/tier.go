package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
	"gastrolog/internal/convert"

	"github.com/google/uuid"
)

func putTierCmd(tier config.TierConfig) *gastrologv1.PutTierCommand {
	return &gastrologv1.PutTierCommand{
		Tier: convert.TierConfigToProto(tier),
	}
}

// NewPutTier creates a ConfigCommand for PutTier.
func NewPutTier(tier config.TierConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutTier{PutTier: putTierCmd(tier)},
	}
}

// NewDeleteTier creates a ConfigCommand for DeleteTier.
func NewDeleteTier(id uuid.UUID, drain bool) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteTier{
			DeleteTier: &gastrologv1.DeleteTierCommand{Id: id.String(), Drain: drain},
		},
	}
}

// ExtractPutTier converts a PutTierCommand back to a TierConfig.
func ExtractPutTier(cmd *gastrologv1.PutTierCommand) (config.TierConfig, error) {
	return convert.TierConfigFromProto(cmd.GetTier())
}

// ExtractDeleteTier extracts the UUID from a DeleteTierCommand.
func ExtractDeleteTier(cmd *gastrologv1.DeleteTierCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}
