package command

import (
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

func putCloudServiceCmd(cs config.CloudService) *gastrologv1.PutCloudServiceCommand {
	transitions := make([]*gastrologv1.CloudServiceTransition, len(cs.Transitions))
	for i, t := range cs.Transitions {
		transitions[i] = &gastrologv1.CloudServiceTransition{
			AfterDays:    t.AfterDays,
			StorageClass: t.StorageClass,
		}
	}
	return &gastrologv1.PutCloudServiceCommand{
		Id:                cs.ID.String(),
		Name:              cs.Name,
		Provider:          cs.Provider,
		Bucket:            cs.Bucket,
		Region:            cs.Region,
		Endpoint:          cs.Endpoint,
		AccessKey:         cs.AccessKey,
		SecretKey:         cs.SecretKey,
		Container:         cs.Container,
		ConnectionString:  cs.ConnectionString,
		CredentialsJson:   cs.CredentialsJSON,
		StorageClass:      cs.StorageClass,
		ArchivalMode:      cs.ArchivalMode,
		Transitions:       transitions,
		RestoreTier:       cs.RestoreTier,
		RestoreDays:       cs.RestoreDays,
		SuspectGraceDays:  cs.SuspectGraceDays,
		ReconcileSchedule: cs.ReconcileSchedule,
	}
}

// NewPutCloudService creates a ConfigCommand for PutCloudService.
func NewPutCloudService(cs config.CloudService) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_PutCloudService{PutCloudService: putCloudServiceCmd(cs)},
	}
}

// NewDeleteCloudService creates a ConfigCommand for DeleteCloudService.
func NewDeleteCloudService(id uuid.UUID) *gastrologv1.ConfigCommand {
	return &gastrologv1.ConfigCommand{
		Command: &gastrologv1.ConfigCommand_DeleteCloudService{
			DeleteCloudService: &gastrologv1.DeleteCloudServiceCommand{Id: id.String()},
		},
	}
}

// ExtractPutCloudService converts a PutCloudServiceCommand back to a CloudService.
func ExtractPutCloudService(cmd *gastrologv1.PutCloudServiceCommand) (config.CloudService, error) {
	id, err := uuid.Parse(cmd.GetId())
	if err != nil {
		return config.CloudService{}, fmt.Errorf("parse cloud service id: %w", err)
	}
	transitions := make([]config.CloudStorageTransition, len(cmd.GetTransitions()))
	for i, t := range cmd.GetTransitions() {
		transitions[i] = config.CloudStorageTransition{
			AfterDays:    t.GetAfterDays(),
			StorageClass: t.GetStorageClass(),
		}
	}
	return config.CloudService{
		ID:                id,
		Name:              cmd.GetName(),
		Provider:          cmd.GetProvider(),
		Bucket:            cmd.GetBucket(),
		Region:            cmd.GetRegion(),
		Endpoint:          cmd.GetEndpoint(),
		AccessKey:         cmd.GetAccessKey(),
		SecretKey:         cmd.GetSecretKey(),
		Container:         cmd.GetContainer(),
		ConnectionString:  cmd.GetConnectionString(),
		CredentialsJSON:   cmd.GetCredentialsJson(),
		StorageClass:      cmd.GetStorageClass(),
		ArchivalMode:      cmd.GetArchivalMode(),
		Transitions:       transitions,
		RestoreTier:       cmd.GetRestoreTier(),
		RestoreDays:       cmd.GetRestoreDays(),
		SuspectGraceDays:  cmd.GetSuspectGraceDays(),
		ReconcileSchedule: cmd.GetReconcileSchedule(),
	}, nil
}

// ExtractDeleteCloudService extracts the UUID from a DeleteCloudServiceCommand.
func ExtractDeleteCloudService(cmd *gastrologv1.DeleteCloudServiceCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}
