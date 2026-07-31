package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"gastrolog/internal/logging"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithylogging "github.com/aws/smithy-go/logging"
)

// S3Config holds configuration for an S3-compatible blob store.
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string // Optional: for MinIO or other S3-compatible stores.
	AccessKey string //nolint:gosec // config field, not a hardcoded secret
	SecretKey string //nolint:gosec // config field, not a hardcoded secret
	// Logger receives AWS SDK diagnostics via the blobstore.s3 component.
	// Nil discards them (tests); production wiring threads the node logger
	// so SDK output never bypasses the structured logging system.
	Logger *slog.Logger
}

// S3Store implements Store using AWS S3 or S3-compatible services.
type S3Store struct {
	client *s3.Client
	bucket string
	// customEndpoint is true for MinIO and other S3-compatible stores reached
	// via BaseEndpoint. Plain-HTTP mocks require spooling unseekable upload
	// bodies so PutObject can set Content-Length.
	customEndpoint bool
}

// NewS3(cfg) creates a new S3Store.
func NewS3(ctx context.Context, cfg S3Config) (*S3Store, error) {
	log := compS3.Apply(logging.Default(cfg.Logger))
	opts := []func(*config.LoadOptions) error{
		// Route SDK log output (retries, checksum notices, transport
		// diagnostics) through the structured logger. Without this the SDK
		// writes raw stdlib "SDK WARN ..." lines to stderr, bypassing every
		// logging pattern in the codebase.
		config.WithLogger(sdkLogger{log: log}),
	}
	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		applyS3ClientOptions(cfg, o)
	})
	return &S3Store{
		client:         client,
		bucket:         cfg.Bucket,
		customEndpoint: cfg.Endpoint != "",
	}, nil
}

// applyS3ClientOptions configures the AWS S3 client for Gastrolog blob uploads.
// RequestChecksumCalculationWhenRequired disables the SDK default of always
// computing a trailing CRC on PutObject. Trailing checksums need a seekable
// body or TLS; our upload path streams GLCB through zstd + io.Pipe over plain
// HTTP to MinIO and similar mocks.
// ResponseChecksumValidationWhenRequired is the download-side mirror: MinIO
// and other S3-compatibles return no checksum headers, so the default
// WhenSupported mode logs a per-GetObject "Response has no supported
// checksum" warning while validating nothing. GLCB blobs carry their own
// digests and are verified on read.
func applyS3ClientOptions(cfg S3Config, o *s3.Options) {
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	if cfg.Endpoint != "" {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true // Required for MinIO and most S3-compatible stores.
	}
}

// sdkLogger adapts the AWS SDK's logging interface onto the structured
// logger so SDK diagnostics carry the blobstore.s3 component and respect
// log rules instead of landing as raw stderr lines.
type sdkLogger struct{ log *slog.Logger }

func (l sdkLogger) Logf(class smithylogging.Classification, format string, v ...any) {
	if l.log == nil {
		return
	}
	msg := fmt.Sprintf(format, v...)
	if class == smithylogging.Warn {
		l.log.Warn(msg)
		return
	}
	l.log.Debug(msg)
}

// s3StreamUploadCompat reports whether unseekable PutObject bodies should be
// spooled to a temp file before upload. Custom endpoints (MinIO, k8s mocks)
// are reached over plain HTTP and require a known Content-Length.
func s3StreamUploadCompat(endpoint string) bool {
	return endpoint != ""
}

func spoolReaderToTemp(r io.Reader) (*os.File, int64, error) {
	tmp, err := os.CreateTemp("", "gastrolog-s3-upload-*")
	if err != nil {
		return nil, 0, fmt.Errorf("create temp upload file: %w", err)
	}
	n, err := io.Copy(tmp, r)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name()) //nolint:gosec // G703: path from os.CreateTemp
		return nil, 0, fmt.Errorf("spool upload body: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name()) //nolint:gosec // G703: path from os.CreateTemp
		return nil, 0, fmt.Errorf("rewind spooled upload body: %w", err)
	}
	return tmp, n, nil
}

func (s *S3Store) EnsureBucket(ctx context.Context) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucket})
	if err != nil {
		// Ignore "bucket already exists" — BucketAlreadyExists and
		// BucketAlreadyOwnedByYou are both fine.
		return nil //nolint:nilerr
	}
	return nil
}

func (s *S3Store) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	body := data
	var contentLength *int64
	if s.customEndpoint && !isSeekableReader(data) {
		tmp, n, err := spoolReaderToTemp(data)
		if err != nil {
			return err
		}
		defer func() {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name()) //nolint:gosec // G703: path from os.CreateTemp
		}()
		body = tmp
		contentLength = aws.Int64(n)
	}

	input := &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &key,
		Body:          body,
		Metadata:      metadata,
		ContentLength: contentLength,
	}
	_, err := s.client.PutObject(ctx, input)
	return err
}

func isSeekableReader(r io.Reader) bool {
	if r == nil {
		return true
	}
	_, ok := r.(io.Seeker)
	return ok
}

func (s *S3Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		if isS3ArchivedError(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobArchived, key)
		}
		if isS3NotFoundError(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
		}
		return nil, err
	}
	return out.Body, nil
}

func (s *S3Store) DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Range:  &rangeHeader,
	})
	if err != nil {
		if isS3ArchivedError(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobArchived, key)
		}
		if isS3NotFoundError(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobNotFound, key)
		}
		return nil, err
	}
	return out.Body, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	return err
}

func (s *S3Store) List(ctx context.Context, prefix string, fn func(BlobInfo) error) error {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			info := BlobInfo{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				StorageClass: string(obj.StorageClass),
			}
			// S3 ListObjectsV2 does not return user metadata — fetch per object.
			// A 404 on Head means the object was deleted between the List page
			// and the Head call (S3 eventual consistency). Skip it.
			head, err := s.Head(ctx, info.Key)
			if err != nil {
				var nf *types.NotFound
				if errors.As(err, &nf) {
					continue
				}
				return fmt.Errorf("head %s: %w", info.Key, err)
			}
			info.Metadata = head.Metadata
			if err := fn(info); err != nil {
				if errors.Is(err, ErrStopIteration) {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (s *S3Store) Head(ctx context.Context, key string) (BlobInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return BlobInfo{}, err
	}
	return BlobInfo{
		Key:          key,
		Size:         aws.ToInt64(out.ContentLength),
		Metadata:     out.Metadata,
		StorageClass: string(out.StorageClass),
	}, nil
}

// isS3ArchivedError checks if an S3 error is InvalidObjectState, which occurs
// when trying to GetObject on a blob in Glacier Flexible Retrieval or Deep Archive.
func isS3ArchivedError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "InvalidObjectState"
	}
	// Also check for the typed error from the S3 SDK.
	var invalidState *types.InvalidObjectState
	return errors.As(err, &invalidState)
}

// isS3NotFoundError checks if an S3 error is NoSuchKey (HTTP 404).
func isS3NotFoundError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound"
	}
	var nsk *types.NoSuchKey
	return errors.As(err, &nsk)
}

// --- Archiver implementation ---

func (s *S3Store) Archive(ctx context.Context, key string, storageClass string) error {
	src := s.bucket + "/" + key
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:       &s.bucket,
		Key:          &key,
		CopySource:   &src,
		StorageClass: types.StorageClass(storageClass),
	})
	return err
}

func (s *S3Store) Restore(ctx context.Context, key string, speed string, days int) error {
	if speed == "" {
		speed = "Standard"
	}
	if days <= 0 {
		days = 7
	}
	d := int32(days)
	_, err := s.client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		RestoreRequest: &types.RestoreRequest{
			Days: &d,
			GlacierJobParameters: &types.GlacierJobParameters{
				Tier: types.Tier(speed),
			},
		},
	})
	// 409 = already restored or restore in progress — not an error.
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "RestoreAlreadyInProgress" {
			return nil
		}
	}
	return err
}

func (s *S3Store) IsRestoring(ctx context.Context, key string) (bool, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return false, err
	}
	// x-amz-restore header: ongoing-request="true" means restoring.
	restore := aws.ToString(out.Restore)
	return restore != "" && !strings.Contains(restore, "ongoing-request=\"false\""), nil
}

var _ Archiver = (*S3Store)(nil)
