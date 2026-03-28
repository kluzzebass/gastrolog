package command

import (
	"fmt"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"

	"github.com/google/uuid"
)

func putCloudServiceCmd(cs config.CloudService) *gastrologv1.PutCloudServiceCommand {
	return &gastrologv1.PutCloudServiceCommand{
		Id:               cs.ID.String(),
		Name:             cs.Name,
		Provider:         cs.Provider,
		Bucket:           cs.Bucket,
		Region:           cs.Region,
		Endpoint:         cs.Endpoint,
		AccessKey:        cs.AccessKey,
		SecretKey:        cs.SecretKey,
		Container:        cs.Container,
		ConnectionString: cs.ConnectionString,
		CredentialsJson:  cs.CredentialsJSON,
		StorageClass:     cs.StorageClass,
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
	return config.CloudService{
		ID:               id,
		Name:             cmd.GetName(),
		Provider:         cmd.GetProvider(),
		Bucket:           cmd.GetBucket(),
		Region:           cmd.GetRegion(),
		Endpoint:         cmd.GetEndpoint(),
		AccessKey:        cmd.GetAccessKey(),
		SecretKey:        cmd.GetSecretKey(),
		Container:        cmd.GetContainer(),
		ConnectionString: cmd.GetConnectionString(),
		CredentialsJSON:  cmd.GetCredentialsJson(),
		StorageClass:     cmd.GetStorageClass(),
	}, nil
}

// ExtractDeleteCloudService extracts the UUID from a DeleteCloudServiceCommand.
func ExtractDeleteCloudService(cmd *gastrologv1.DeleteCloudServiceCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}
