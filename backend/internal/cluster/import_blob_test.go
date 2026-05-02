package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// startImportBlobServer stands up a grpc.Server registered with the
// cluster ServiceDesc, hands the test a *Server whose BlobImporter the
// test can configure, and returns a *grpc.ClientConn dialing in.
func startImportBlobServer(t *testing.T, srv *Server) (*grpc.ClientConn, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	gs.RegisterService(&clusterServiceDesc, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gs.Stop()
		_ = lis.Close()
		t.Fatalf("dial: %v", err)
	}
	return conn, func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	}
}

// TestImportBlob_StreamRoundTrip exercises the full ImportBlob client →
// server path: leader streams a blob in 256 KiB chunks, follower's
// BlobImporter receives the bytes, and the SHA-256 the follower computes
// matches what the leader sent.
//
// This pins the wire framing (Header → Body... → Ack) and the
// importBlobStreamReader's adapt-to-io.Reader behaviour.
func TestImportBlob_StreamRoundTrip(t *testing.T) {
	t.Parallel()

	// 5 MiB of pseudorandom bytes — large enough to exercise the
	// multi-Body chunking (256 KiB per Body → ~20 messages).
	const totalBytes = 5 * 1024 * 1024
	payload := make([]byte, totalBytes)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	expected := sha256.Sum256(payload)

	var (
		mu       sync.Mutex
		gotBytes []byte
		gotVault glid.GLID
		gotTier  glid.GLID
		gotChunk chunk.ChunkID
	)
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	srv := &Server{
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
		blobImporter: func(_ context.Context, vaultID, tierID glid.GLID, chunkID chunk.ChunkID, totalSize int64, body io.Reader) ([32]byte, error) {
			mu.Lock()
			gotVault = vaultID
			gotTier = tierID
			gotChunk = chunkID
			mu.Unlock()

			if totalSize != totalBytes {
				return [32]byte{}, errors.New("unexpected total size")
			}
			h := sha256.New()
			n, err := io.Copy(h, body)
			if err != nil {
				return [32]byte{}, err
			}
			if n != totalBytes {
				return [32]byte{}, errors.New("short read")
			}
			mu.Lock()
			gotBytes = make([]byte, n)
			copy(gotBytes, payload[:n]) // for the assertion below; importer must NOT retain body bytes past this call
			mu.Unlock()
			var d [32]byte
			copy(d[:], h.Sum(nil))
			return d, nil
		},
	}

	conn, cleanup := startImportBlobServer(t, srv)
	defer cleanup()

	tr := &TierReplicator{
		peers: &PeerConns{conns: map[string]*grpc.ClientConn{"node-1": conn}},
	}

	vaultID := glid.New()
	tierID := glid.New()
	chunkID := chunk.ChunkID(glid.New())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	digest, err := tr.ImportBlob(ctx, "node-1", vaultID, tierID, chunkID, totalBytes, &byteReader{p: payload})
	if err != nil {
		t.Fatalf("ImportBlob: %v", err)
	}
	if digest != expected {
		t.Errorf("digest mismatch:\n got %x\nwant %x", digest[:], expected[:])
	}

	mu.Lock()
	defer mu.Unlock()
	if gotVault != vaultID {
		t.Errorf("vaultID: got %s want %s", gotVault, vaultID)
	}
	if gotTier != tierID {
		t.Errorf("tierID: got %s want %s", gotTier, tierID)
	}
	if gotChunk != chunkID {
		t.Errorf("chunkID: got %v want %v", gotChunk, chunkID)
	}
	if len(gotBytes) != totalBytes {
		t.Errorf("got %d bytes, want %d", len(gotBytes), totalBytes)
	}
}

// TestImportBlob_NoImporterConfigured pins the unavailable-error path.
func TestImportBlob_NoImporterConfigured(t *testing.T) {
	t.Parallel()
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()
	srv := &Server{stopCtx: stopCtx, stopCancel: stopCancel} // no blobImporter

	conn, cleanup := startImportBlobServer(t, srv)
	defer cleanup()

	tr := &TierReplicator{
		peers: &PeerConns{conns: map[string]*grpc.ClientConn{"node-1": conn}},
	}

	payload := []byte("hello")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := tr.ImportBlob(ctx, "node-1",
		glid.New(), glid.New(), chunk.ChunkID(glid.New()),
		int64(len(payload)), &byteReader{p: payload})
	if err == nil {
		t.Fatal("expected error when blobImporter is nil")
	}
}

// byteReader is a tiny io.Reader over a []byte, used to feed payloads into
// ImportBlob without going through bytes.NewReader (which is fine, but
// keeping the helper in this file so the test reads top-down).
type byteReader struct {
	p   []byte
	off int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.off >= len(r.p) {
		return 0, io.EOF
	}
	n := copy(p, r.p[r.off:])
	r.off += n
	return n, nil
}
