package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"gastrolog/internal/glid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startSegmentPullServer stands up a real gRPC server with the ClusterService
// descriptor registered against a *Server whose segmentPullServer seam is fn,
// then returns a SegmentPuller wired to a pre-seeded peer connection plus a
// teardown. This exercises the full PullSegment RPC: client framing, the
// server handler, and the segmentChunkWriter adapter.
func startSegmentPullServer(t *testing.T, fn SegmentPullServer) (*SegmentPuller, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	gsrv.RegisterService(&clusterServiceDesc, &Server{segmentPullServer: fn})
	go func() { _ = gsrv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gsrv.Stop()
		_ = lis.Close()
		t.Fatalf("dial: %v", err)
	}
	sp := &SegmentPuller{peers: &PeerConns{conns: map[string]*grpc.ClientConn{"node-origin": conn}}}
	return sp, func() {
		_ = conn.Close()
		gsrv.Stop()
		_ = lis.Close()
	}
}

func TestSegmentPullerRoundTrip(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	segID := glid.New()
	// Larger than the 32KB io.Copy buffer / 64KB frame cap to exercise
	// multi-frame streaming and reassembly.
	payload := bytes.Repeat([]byte("segment-bytes-"), 20000)

	var gotVault, gotSeg glid.GLID
	sp, cleanup := startSegmentPullServer(t, func(v, s glid.GLID, w io.Writer) error {
		gotVault, gotSeg = v, s
		// Mimic distribution.ServePull: stream bytes through the writer.
		_, err := io.Copy(w, bytes.NewReader(payload))
		return err
	})
	defer cleanup()

	var buf bytes.Buffer
	if err := sp.Pull(context.Background(), "node-origin", vaultID, segID, &buf); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if gotVault != vaultID || gotSeg != segID {
		t.Fatalf("server saw vault=%s seg=%s, want %s/%s", gotVault, gotSeg, vaultID, segID)
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Fatalf("pulled %d bytes, want %d", buf.Len(), len(payload))
	}
}

func TestSegmentPullerMissingSegment(t *testing.T) {
	t.Parallel()
	sp, cleanup := startSegmentPullServer(t, func(glid.GLID, glid.GLID, io.Writer) error {
		return fmt.Errorf("segment not held here")
	})
	defer cleanup()

	var buf bytes.Buffer
	err := sp.Pull(context.Background(), "node-origin", glid.New(), glid.New(), &buf)
	if err == nil {
		t.Fatal("expected error for missing segment, got nil")
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written on failure, got %d", buf.Len())
	}
}
