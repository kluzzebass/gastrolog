package blobstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"

	"gastrolog/internal/blobstore"
)

// skipIfNoEmulators skips the test when the BLOBSTORE_INTEGRATION env var is
// not set.  Run with:
//
//	just cloud-storage-up
//	BLOBSTORE_INTEGRATION=1 go test ./internal/blobstore/... -count=1
func skipIfNoEmulators(t *testing.T) {
	t.Helper()
	if os.Getenv("BLOBSTORE_INTEGRATION") == "" {
		t.Skip("set BLOBSTORE_INTEGRATION=1 to run (requires cloud-storage emulators)")
	}
}

// storeTest runs the full CRUD + List + Head suite against any Store.
func storeTest(t *testing.T, store blobstore.Store) {
	t.Helper()
	ctx := context.Background()
	key := "test/hello.txt"
	body := "hello, blobstore"
	meta := map[string]string{"origin": "integration-test"}

	// Upload
	err := store.Upload(ctx, key, bytes.NewReader([]byte(body)), meta)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Download
	rc, err := store.Download(ctx, key)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != body {
		t.Fatalf("Download body = %q, want %q", got, body)
	}

	// Head
	info, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if info.Key != key {
		t.Errorf("Head key = %q, want %q", info.Key, key)
	}
	if info.Size != int64(len(body)) {
		t.Errorf("Head size = %d, want %d", info.Size, len(body))
	}
	if info.Metadata["origin"] != "integration-test" {
		t.Errorf("Head metadata[origin] = %q, want %q", info.Metadata["origin"], "integration-test")
	}

	// List
	var listCount int
	found := false
	err = store.List(ctx, "test/", func(item blobstore.BlobInfo) error {
		listCount++
		if item.Key == key {
			found = true
			if item.Metadata["origin"] != "integration-test" {
				t.Errorf("List metadata[origin] = %q, want %q", item.Metadata["origin"], "integration-test")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if listCount == 0 {
		t.Fatal("List returned 0 items")
	}
	if !found {
		t.Errorf("List did not contain key %q", key)
	}

	// Delete
	err = store.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	_, err = store.Download(ctx, key)
	if err == nil {
		t.Fatal("Download after Delete should fail")
	}
}

func ensureS3Bucket(ctx context.Context, endpoint, bucket, accessKey, secretKey string) error {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})
	if err != nil {
		// Ignore "bucket already exists" errors.
		return nil //nolint:nilerr
	}
	return nil
}

func ensureAzureContainer(connStr, name string) error {
	client, err := azblob.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return err
	}
	_, err = client.CreateContainer(context.Background(), name, nil)
	if err != nil {
		// Ignore "container already exists" errors.
		return nil //nolint:nilerr
	}
	return nil
}

func ensureGCSBucket(ctx context.Context, endpoint, bucket string) error {
	// fake-gcs-server: create bucket via REST API.
	url := fmt.Sprintf("%sb/%s", endpoint, bucket)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil // already exists
	}
	// Create it.
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(endpoint),
		option.WithoutAuthentication(),
	)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Bucket(bucket).Create(ctx, "", nil)
}

func TestS3Store(t *testing.T) {
	skipIfNoEmulators(t)
	ctx := context.Background()

	if err := ensureS3Bucket(ctx, "http://localhost:9000", "blobstore-test", "gastrolog", "gastrolog"); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	s, err := blobstore.NewS3(ctx, blobstore.S3Config{
		Bucket:    "blobstore-test",
		Region:    "us-east-1",
		Endpoint:  "http://localhost:9000",
		AccessKey: "gastrolog",
		SecretKey: "gastrolog",
	})
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	storeTest(t, s)
}

func TestAzureStore(t *testing.T) {
	skipIfNoEmulators(t)

	// Full Azurite connection string — the "UseDevelopmentStorage=true"
	// shorthand doesn't always work with the Go SDK.
	connStr := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
	if err := ensureAzureContainer(connStr, "blobstore-test"); err != nil {
		t.Fatalf("ensure container: %v", err)
	}

	s, err := blobstore.NewAzure(blobstore.AzureConfig{
		Container:        "blobstore-test",
		ConnectionString: connStr,
	})
	if err != nil {
		t.Fatalf("NewAzure: %v", err)
	}
	storeTest(t, s)
}

func TestGCSStore(t *testing.T) {
	skipIfNoEmulators(t)
	ctx := context.Background()

	// fake-gcs-server endpoint. The Go storage client also respects
	// STORAGE_EMULATOR_HOST, but we pass it explicitly.
	endpoint := "http://localhost:4443/storage/v1/"
	if err := ensureGCSBucket(ctx, endpoint, "blobstore-test"); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	s, err := blobstore.NewGCS(ctx, blobstore.GCSConfig{
		Bucket:   "blobstore-test",
		Endpoint: "http://localhost:4443",
	})
	if err != nil {
		t.Fatalf("NewGCS: %v", err)
	}
	storeTest(t, s)
}
