package blobstore

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithylogging "github.com/aws/smithy-go/logging"
)

func TestApplyS3ClientOptionsChecksumWhenRequired(t *testing.T) {
	t.Parallel()
	var opts s3.Options
	applyS3ClientOptions(S3Config{Endpoint: "http://localhost:19000"}, &opts)
	if opts.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		t.Fatalf("RequestChecksumCalculation = %v, want WhenRequired", opts.RequestChecksumCalculation)
	}
	if opts.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
		t.Fatalf("ResponseChecksumValidation = %v, want WhenRequired", opts.ResponseChecksumValidation)
	}
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://localhost:19000" {
		t.Fatalf("BaseEndpoint = %v, want http://localhost:19000", opts.BaseEndpoint)
	}
	if !opts.UsePathStyle {
		t.Fatal("UsePathStyle = false, want true for custom endpoint")
	}
}

// TestSDKLoggerRoutesToSlog pins the SDK→slog adapter: AWS SDK output must
// land in the structured logger with the blobstore.s3 component, at Warn for
// SDK warnings and Debug otherwise — never as raw stdlib stderr lines.
func TestSDKLoggerRoutesToSlog(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	l := sdkLogger{log: compS3.Apply(base)}

	l.Logf(smithylogging.Warn, "checksum %s", "missing")
	l.Logf(smithylogging.Debug, "retrying %d", 2)

	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "checksum missing") {
		t.Fatalf("warn line not routed: %q", out)
	}
	if !strings.Contains(out, "level=DEBUG") || !strings.Contains(out, "retrying 2") {
		t.Fatalf("debug line not routed: %q", out)
	}
	if !strings.Contains(out, "component=blobstore.s3") {
		t.Fatalf("component tag missing: %q", out)
	}
}

func TestSDKLoggerNilLoggerDiscards(t *testing.T) {
	t.Parallel()
	// Must not panic.
	sdkLogger{}.Logf(smithylogging.Warn, "dropped")
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
