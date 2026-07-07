package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	"google.golang.org/grpc"
)

// startChunkGLCBPullServer stands up a real gRPC server with the
// ClusterService descriptor registered against a *Server whose
// chunkGLCBPullServer seam is fn, then returns a ChunkGLCBPuller wired to a
// pre-seeded peer connection plus a teardown. This exercises the full
// PullChunkGLCB RPC: client framing, the server handler, and the
// glcbChunkWriter adapter.
func startChunkGLCBPullServer(t *testing.T, fn ChunkGLCBPullServer) (*ChunkGLCBPuller, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	gsrv.RegisterService(&clusterServiceDesc, &Server{chunkGLCBPullServer: fn})
	go func() { _ = gsrv.Serve(lis) }()

	mgr := NewStaticPeerConns("local", func(id string) (string, bool) {
		if id == "node-origin" {
			return lis.Addr().String(), true
		}
		return "", false
	})
	p := &ChunkGLCBPuller{peers: mgr}
	return p, func() {
		_ = mgr.Close()
		gsrv.Stop()
		_ = lis.Close()
	}
}

func TestChunkGLCBPullerRoundTrip(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	// Larger than the 32KB io.Copy buffer / 64KB frame cap to exercise
	// multi-frame streaming and reassembly.
	payload := bytes.Repeat([]byte("glcb-bytes-"), 20000)

	var gotVault glid.GLID
	var gotChunk chunk.ChunkID
	p, cleanup := startChunkGLCBPullServer(t, func(v glid.GLID, c chunk.ChunkID, w io.Writer) error {
		gotVault, gotChunk = v, c
		_, err := io.Copy(w, bytes.NewReader(payload))
		return err
	})
	defer cleanup()

	var buf bytes.Buffer
	if err := p.Pull(context.Background(), "node-origin", vaultID, chunkID, &buf); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if gotVault != vaultID || gotChunk != chunkID {
		t.Fatalf("server saw vault=%s chunk=%s, want %s/%s", gotVault, gotChunk, vaultID, chunkID)
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Fatalf("pulled %d bytes, want %d", buf.Len(), len(payload))
	}
}

func TestChunkGLCBPullerMissingChunk(t *testing.T) {
	t.Parallel()
	p, cleanup := startChunkGLCBPullServer(t, func(glid.GLID, chunk.ChunkID, io.Writer) error {
		return fmt.Errorf("chunk not held here")
	})
	defer cleanup()

	var buf bytes.Buffer
	err := p.Pull(context.Background(), "node-origin", glid.New(), chunk.NewChunkID(), &buf)
	if err == nil {
		t.Fatal("expected error for missing chunk, got nil")
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written on failure, got %d", buf.Len())
	}
}
