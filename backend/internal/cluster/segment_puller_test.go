package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"

	"google.golang.org/grpc"
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

	mgr := NewStaticPeerConns("local", func(id string) (string, bool) {
		if id == "node-origin" {
			return lis.Addr().String(), true
		}
		return "", false
	})
	sp := &SegmentPuller{peers: mgr}
	return sp, func() {
		_ = mgr.Close()
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

// TestSegmentPullerMultiFrameFromFile exercises the production serve shape:
// distribution.StreamSegment io.Copies from an *os.File, which routes through
// segmentChunkWriter.ReadFrom — pullFrameSize frames through ONE buffer
// reused across SendMsg calls (gastrolog-47jm3m). The payload spans more than
// two frames and every byte is position-dependent, so any stale-buffer
// retention by the transport, frame reordering, or aliasing corrupts the
// reassembled bytes and fails the comparison. Run under -race this also
// guards the no-copy SendMsg contract.
func TestSegmentPullerMultiFrameFromFile(t *testing.T) {
	t.Parallel()
	const size = 2*pullFrameSize + 12345
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i) ^ byte(i>>9) ^ byte(i>>17)
	}
	path := filepath.Join(t.TempDir(), "segment.seg")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write segment file: %v", err)
	}

	sp, cleanup := startSegmentPullServer(t, func(_, _ glid.GLID, w io.Writer) error {
		// Mirror distribution.StreamSegment exactly.
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close() //nolint:errcheck // read-only
		_, err = io.Copy(w, f)
		return err
	})
	defer cleanup()

	var buf bytes.Buffer
	if err := sp.Pull(context.Background(), "node-origin", glid.New(), glid.New(), &buf); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	got := buf.Bytes()
	if len(got) != len(payload) {
		t.Fatalf("pulled %d bytes, want %d", len(got), len(payload))
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("pulled bytes diverge at offset %d: got %#x want %#x", i, got[i], payload[i])
		}
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
