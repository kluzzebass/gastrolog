package blobstore

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config holds configuration for an S3-compatible blob store.
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string // Optional: for MinIO or other S3-compatible stores.
	AccessKey string //nolint:gosec // config field, not a hardcoded secret
	SecretKey string //nolint:gosec // config field, not a hardcoded secret
}

// S3Store implements Store using AWS S3 or S3-compatible services.
type S3Store struct {
	client *s3.Client
	bucket string
}

// NewS3(cfg) creates a new S3Store.
func NewS3(ctx context.Context, cfg S3Config) (*S3Store, error) {
	var opts []func(*config.LoadOptions) error
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
	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // Required for MinIO and most S3-compatible stores.
		})
	}
	client := s3.NewFromConfig(awsCfg, s3Opts...)
	return &S3Store{client: client, bucket: cfg.Bucket}, nil
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
	input := &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     data,
		Metadata: metadata,
	}
	_, err := s.client.PutObject(ctx, input)
	return err
}

func (s *S3Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
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

func (s *S3Store) List(ctx context.Context, prefix string) ([]BlobInfo, error) {
	var result []BlobInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			info := BlobInfo{
				Key:  aws.ToString(obj.Key),
				Size: aws.ToInt64(obj.Size),
			}
			// S3 ListObjectsV2 does not return user metadata — fetch per object.
			head, err := s.Head(ctx, info.Key)
			if err != nil {
				return nil, fmt.Errorf("head %s: %w", info.Key, err)
			}
			info.Metadata = head.Metadata
			result = append(result, info)
		}
	}
	return result, nil
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
		Key:      key,
		Size:     aws.ToInt64(out.ContentLength),
		Metadata: out.Metadata,
	}, nil
}
