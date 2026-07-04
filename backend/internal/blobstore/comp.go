package blobstore

import "gastrolog/internal/logging/comp"

// compBlobstore is the logging component root for cloud blob stores.
var compBlobstore = comp.Root("blobstore").Desc(
	"Cloud blob stores — provider clients backing cloud-backed chunk blobs.")

// compS3 tags the AWS S3 / S3-compatible client, including AWS SDK log
// output rerouted through the structured logger.
var compS3 = compBlobstore.Sub("s3").Desc(
	"AWS S3 / S3-compatible client — SDK diagnostics route here instead of raw stderr.")
