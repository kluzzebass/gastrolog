package command

import (
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/convert"

	"github.com/google/uuid"
)

func putCloudServiceCmd(cs system.CloudService) *gastrologv1.PutCloudServiceCommand {
	return &gastrologv1.PutCloudServiceCommand{
		CloudService: convert.CloudServiceToProto(cs),
	}
}

// NewPutCloudService creates a ConfigCommand for PutCloudService.
func NewPutCloudService(cs system.CloudService) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_PutCloudService{PutCloudService: putCloudServiceCmd(cs)},
	}
}

// NewDeleteCloudService creates a ConfigCommand for DeleteCloudService.
func NewDeleteCloudService(id uuid.UUID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteCloudService{
			DeleteCloudService: &gastrologv1.DeleteCloudServiceCommand{Id: id.String()},
		},
	}
}

// ExtractPutCloudService converts a PutCloudServiceCommand back to a CloudService.
func ExtractPutCloudService(cmd *gastrologv1.PutCloudServiceCommand) (system.CloudService, error) {
	return convert.CloudServiceFromProto(cmd.GetCloudService()), nil
}

// ExtractDeleteCloudService extracts the UUID from a DeleteCloudServiceCommand.
func ExtractDeleteCloudService(cmd *gastrologv1.DeleteCloudServiceCommand) (uuid.UUID, error) {
	return uuid.Parse(cmd.GetId())
}
