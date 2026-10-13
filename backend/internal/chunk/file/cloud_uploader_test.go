package file

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// countingUploadStore counts the PUTs that reach the store.
type countingUploadStore struct {
	blobstore.Store
	uploads atomic.Int64
}

func (s *countingUploadStore) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	s.uploads.Add(1)
	return s.Store.Upload(ctx, key, data, metadata)
}

// newGatedCloudManager returns a manager whose uploads consult isUploader, and
// one sealed chunk with its GLCB built locally and not uploaded.
func newGatedCloudManager(t *testing.T, store blobstore.Store, isUploader func() bool) (*Manager, chunk.ChunkID) {
	t.Helper()
	building := true
	cm, err := NewManager(Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
		CloudStore:     store,
		VaultID:        glid.New(),
		IsCloudUploader: func() bool {
			return !building && isUploader()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	ts := time.Now()
	for i := range 10 {
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts.Add(time.Duration(i) * time.Microsecond),
			WriteTS:  ts.Add(time.Duration(i) * time.Microsecond),
			Raw:      []byte("uploader-gate"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatal(err)
	}
	metas, err := cm.List()
	if err != nil {
		t.Fatal(err)
	}
	var id chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			id = m.ID
		}
	}
	if err := cm.PostSealProcess(context.Background(), id); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}
	building = false
	return cm, id
}

// The uploader check is read at every upload, not fixed when the manager is
// built: a node that is not the uploader refuses, and the same manager uploads
// once the node becomes the uploader.
func TestUploadToCloudFollowsUploaderAtCallTime(t *testing.T) {
	t.Parallel()

	store := &countingUploadStore{Store: blobstore.NewMemory()}
	var isUploader atomic.Bool
	cm, id := newGatedCloudManager(t, store, isUploader.Load)

	if err := cm.UploadToCloud(id); !errors.Is(err, chunk.ErrNotCloudUploader) {
		t.Fatalf("UploadToCloud on a non-uploader = %v, want ErrNotCloudUploader", err)
	}
	if n := store.uploads.Load(); n != 0 {
		t.Fatalf("non-uploader PUTs = %d, want 0", n)
	}

	isUploader.Store(true)
	if err := cm.UploadToCloud(id); err != nil {
		t.Fatalf("UploadToCloud on the uploader: %v", err)
	}
	if n := store.uploads.Load(); n != 1 {
		t.Fatalf("uploader PUTs = %d, want 1", n)
	}
	meta, err := cm.Meta(id)
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if !meta.CloudBacked {
		t.Fatal("uploaded chunk is not cloud-backed")
	}
}

// An upload that starts while this node is the uploader and loses that role
// before it writes the object leaves the object to the new uploader.
func TestUploadToCloudRechecksUploaderBeforeWriting(t *testing.T) {
	t.Parallel()

	store := &countingUploadStore{Store: blobstore.NewMemory()}
	var asked atomic.Int64
	cm, id := newGatedCloudManager(t, store, func() bool {
		return asked.Add(1) == 1
	})

	if err := cm.UploadToCloud(id); !errors.Is(err, chunk.ErrNotCloudUploader) {
		t.Fatalf("UploadToCloud after losing the uploader role = %v, want ErrNotCloudUploader", err)
	}
	if n := store.uploads.Load(); n != 0 {
		t.Fatalf("PUTs after losing the uploader role = %d, want 0", n)
	}
	if asked.Load() < 2 {
		t.Fatalf("uploader asked %d times, want a second ask before the PUT", asked.Load())
	}
}
