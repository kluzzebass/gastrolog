package cluster

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// benchSinkStream satisfies grpc.ServerStream but implements only SendMsg,
// with the same allocation profile as the real serverStream.SendMsg in the
// pinned grpc-go: marshal the message through the registered proto CodecV2
// into grpc's pooled buffers, then free them the way the transport does after
// the wire write (stream.go SendMsg -> prepareMsg -> encode -> codecV2.Marshal;
// deferred data.Free()). This isolates the server-side write path from
// client-side unmarshal allocations that an end-to-end benchmark would
// conflate with it.
type benchSinkStream struct {
	grpc.ServerStream
	codec encoding.CodecV2
	sent  int64
}

func (s *benchSinkStream) SendMsg(m any) error {
	data, err := s.codec.Marshal(m)
	if err != nil {
		return err
	}
	s.sent += int64(data.Len())
	data.Free()
	return nil
}

// perFrameCopyChunkWriter is the writer shape that predates the no-copy
// SendMsg contract documented on segmentChunkWriter.Write: a fresh []byte per
// frame, defensive against the transport retaining the previous frame's Data.
// Benchmark baseline only.
type perFrameCopyChunkWriter struct {
	stream grpc.ServerStream
}

func (w *perFrameCopyChunkWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	data := append([]byte(nil), p...)
	if err := w.stream.SendMsg(&gastrologv1.PullSegmentChunk{Data: data}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// writeOnly hides ReadFrom so io.Copy falls back to its 32KB scratch loop —
// the frame size the serve path had after the no-copy fix but before
// pullFrameSize framing.
type writeOnly struct{ io.Writer }

// BenchmarkSegmentPullServeWrite measures one full segment served through the
// PullSegment write path (open file -> io.Copy -> chunk writer -> SendMsg
// marshal), per op, across the three shapes the writer has had:
//
//	per-frame-copy-32k: fresh []byte copy per 32KB frame
//	no-copy-32k:        no-copy SendMsg contract, io.Copy 32KB scratch
//	no-copy-1m-frames:  no-copy + ReadFrom pullFrameSize framing (current)
func BenchmarkSegmentPullServeWrite(b *testing.B) {
	const segSize = 8 << 20
	payload := make([]byte, segSize)
	for i := range payload {
		payload[i] = byte(i) ^ byte(i>>11)
	}
	path := filepath.Join(b.TempDir(), "segment.seg")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		b.Fatalf("write segment file: %v", err)
	}
	codec := encoding.GetCodecV2("proto")
	if codec == nil {
		b.Fatal("proto codec not registered")
	}

	run := func(b *testing.B, makeWriter func(stream grpc.ServerStream) io.Writer) {
		f, err := os.Open(path)
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		defer f.Close() //nolint:errcheck // read-only
		sink := &benchSinkStream{codec: codec}
		b.SetBytes(segSize)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				b.Fatalf("seek: %v", err)
			}
			// Fresh writer per op: pullSegmentStreamHandler builds one per RPC.
			if _, err := io.Copy(makeWriter(sink), f); err != nil {
				b.Fatalf("copy: %v", err)
			}
		}
		if sink.sent == 0 {
			b.Fatal("sink saw no bytes")
		}
	}

	b.Run("per-frame-copy-32k", func(b *testing.B) {
		run(b, func(s grpc.ServerStream) io.Writer { return &perFrameCopyChunkWriter{stream: s} })
	})
	b.Run("no-copy-32k", func(b *testing.B) {
		run(b, func(s grpc.ServerStream) io.Writer { return writeOnly{&segmentChunkWriter{stream: s}} })
	})
	b.Run("no-copy-1m-frames", func(b *testing.B) {
		run(b, func(s grpc.ServerStream) io.Writer { return &segmentChunkWriter{stream: s} })
	})
}
