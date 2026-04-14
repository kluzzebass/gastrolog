package command

import (
	"gastrolog/internal/glid"
	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/system"
	"gastrolog/internal/convert"

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
func NewDeleteCloudService(id glid.GLID) *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{
		Command: &gastrologv1.SystemCommand_DeleteCloudService{
			DeleteCloudService: &gastrologv1.DeleteCloudServiceCommand{Id: id.ToProto()},
		},
	}
}

// ExtractPutCloudService converts a PutCloudServiceCommand back to a CloudService.
func ExtractPutCloudService(cmd *gastrologv1.PutCloudServiceCommand) (system.CloudService, error) {
	return convert.CloudServiceFromProto(cmd.GetCloudService()), nil
}

// ExtractDeleteCloudService extracts the UUID from a DeleteCloudServiceCommand.
func ExtractDeleteCloudService(cmd *gastrologv1.DeleteCloudServiceCommand) (glid.GLID, error) {
	return glid.FromBytes(cmd.GetId()), nil
}
