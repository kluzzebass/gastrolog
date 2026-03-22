package command

import (
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

func putTierCmd(tier config.TierConfig) *gastrologv1.PutTierCommand {
	rules := make([]*gastrologv1.VaultRetentionRule, len(tier.RetentionRules))
	for i, r := range tier.RetentionRules {
		ejectIDs := make([]string, len(r.EjectRouteIDs))
		for j, eid := range r.EjectRouteIDs {
			ejectIDs[j] = eid.String()
		}
		rules[i] = &gastrologv1.VaultRetentionRule{
			RetentionPolicyId: r.RetentionPolicyID.String(),
			Action:            string(r.Action),
			EjectRouteIds:     ejectIDs,
		}
	}
	return &gastrologv1.PutTierCommand{
		Id:                tier.ID.String(),
		Name:              tier.Name,
		Type:              string(tier.Type),
		RotationPolicyId:  uuidPtrToString(tier.RotationPolicyID),
		RetentionRules:    rules,
		MemoryBudgetBytes: tier.MemoryBudgetBytes,
		StorageClass:      tier.StorageClass,
		CloudServiceId:    uuidPtrToString(tier.CloudServiceID),
		ActiveChunkClass:  tier.ActiveChunkClass,
		CacheClass:        tier.CacheClass,
	}
}

// NewPutTier creates a ConfigCommand for PutTier.
func NewPutTier(tier config.TierConfig) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutTier{PutTier: putTierCmd(tier)},
	}
}

// NewDeleteTier creates a ConfigCommand for DeleteTier.
func NewDeleteTier(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteTier{
			DeleteTier: &gastrologv1.DeleteTierCommand{Id: id.String()},
		},
	}
}

// ExtractPutTier converts a PutTierCommand back to a TierConfig.
func ExtractPutTier(cmd *gastrologv1.PutTierCommand) (config.TierConfig, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.TierConfig{}, fmt.Errorf("parse tier id: %w", err)
	}
	rotationPolicyID, err := parseOptionalUUID(cmd.GetRotationPolicyId())
	if err != nil {
		return config.TierConfig{}, fmt.Errorf("parse tier rotation policy id: %w", err)
	}
	cloudServiceID, err := parseOptionalUUID(cmd.GetCloudServiceId())
	if err != nil {
		return config.TierConfig{}, fmt.Errorf("parse tier cloud service id: %w", err)
	}

	var rules []config.RetentionRule
	for _, r := range cmd.GetRetentionRules() {
		rpID, err := uuid.Parse(r.GetRetentionPolicyId())
		if err != nil {
			return config.TierConfig{}, fmt.Errorf("parse tier retention rule policy id: %w", err)
		}
		var ejectRouteIDs []uuid.UUID
		for _, eid := range r.GetEjectRouteIds() {
			eidParsed, err := uuid.Parse(eid)
			if err != nil {
				return config.TierConfig{}, fmt.Errorf("parse tier eject route id: %w", err)
			}
			ejectRouteIDs = append(ejectRouteIDs, eidParsed)
		}
		rules = append(rules, config.RetentionRule{
			RetentionPolicyID: rpID,
			Action:            config.RetentionAction(r.GetAction()),
			EjectRouteIDs:     ejectRouteIDs,
		})
	}

	return config.TierConfig{
		ID:                id,
		Name:              cmd.GetName(),
		Type:              config.TierType(cmd.GetType()),
		RotationPolicyID:  rotationPolicyID,
		RetentionRules:    rules,
		MemoryBudgetBytes: cmd.GetMemoryBudgetBytes(),
		StorageClass:      cmd.GetStorageClass(),
		CloudServiceID:    cloudServiceID,
		ActiveChunkClass:  cmd.GetActiveChunkClass(),
		CacheClass:        cmd.GetCacheClass(),
	}, nil
}

// ExtractDeleteTier extracts the UUID from a DeleteTierCommand.
func ExtractDeleteTier(cmd *gastrologv1.DeleteTierCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}
