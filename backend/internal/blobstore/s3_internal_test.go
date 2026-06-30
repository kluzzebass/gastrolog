package blobstore

import (
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestApplyS3ClientOptionsChecksumWhenRequired(t *testing.T) {
	t.Parallel()
	var opts s3.Options
	applyS3ClientOptions(S3Config{Endpoint: "http://localhost:19000"}, &opts)
	if opts.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		t.Fatalf("RequestChecksumCalculation = %v, want WhenRequired", opts.RequestChecksumCalculation)
	}
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://localhost:19000" {
		t.Fatalf("BaseEndpoint = %v, want http://localhost:19000", opts.BaseEndpoint)
	}
	if !opts.UsePathStyle {
		t.Fatal("UsePathStyle = false, want true for custom endpoint")
	}
}

func TestS3StreamUploadCompat(t *testing.T) {
	t.Parallel()
	if !s3StreamUploadCompat("http://localhost:19000") {
		t.Fatal("custom endpoint should enable stream upload compat")
	}
	if s3StreamUploadCompat("") {
		t.Fatal("native AWS endpoint should not force UNSIGNED-PAYLOAD")
	}
}

func TestS3UploadUnseekableStreamIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if os.Getenv("BLOBSTORE_INTEGRATION") == "" {
		t.Skip("set BLOBSTORE_INTEGRATION=1 to run (requires cloud-storage emulators)")
	}
	ctx := t.Context()

	s, err := NewS3(ctx, S3Config{
		Bucket:    "blobstore-test",
		Region:    "us-east-1",
		Endpoint:  "http://localhost:19000",
		AccessKey: "gastrolog",
		SecretKey: "gastrolog",
	})
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	_ = s.EnsureBucket(ctx)

	key := "test/unseekable-pipe.bin"
	pr, pw := io.Pipe()
	go func() {
		_, _ = io.WriteString(pw, "glcb-like streaming payload")
		_ = pw.Close()
	}()

	if err := s.Upload(ctx, key, pr, map[string]string{"origin": "pipe-test"}); err != nil {
		t.Fatalf("Upload unseekable stream: %v", err)
	}
	t.Cleanup(func() { _ = s.Delete(ctx, key) })

	rc, err := s.Download(ctx, key)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "glcb-like streaming payload" {
		t.Fatalf("body = %q, want glcb-like streaming payload", got)
	}
}
