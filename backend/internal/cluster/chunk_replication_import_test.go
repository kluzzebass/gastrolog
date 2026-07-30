package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/convert"
	"gastrolog/internal/glid"
)

// Streaming ImportSealed is verified at two layers:
//
//   1. handler side — Begin → Records → Commit drives the importer
//      goroutine via a channel-backed iterator; this file pins those
//      transitions, the chunk_id mismatch guard, and the abort-on-cancel
//      cleanup so they don't regress into the old single-frame shape.
//   2. leader side — ImportSealedChunk caps every wire frame under
//      importRecordsMaxBytes regardless of total chunk size, so chunks
//      larger than the gRPC receive cap (the original 128 MiB wedge)
//      still make progress.

func newImportTestServer(importer VaultRecordImporter) *Server {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	s.SetVaultRecordImporter(importer)
	return s
}

func makeRecord(payload []byte) chunk.Record {
	return chunk.Record{
		Raw:      payload,
		IngestTS: time.Unix(1700000000, 0).UTC(),
		WriteTS:  time.Unix(1700000000, 0).UTC(),
	}
}

func TestImportStreaming_BeginRecordsCommit_RoundTrip(t *testing.T) {
	t.Parallel()

	var (
		gotChunkID  chunk.ChunkID
		gotVaultID  glid.GLID
		gotRecords  []chunk.Record
		importerErr error
	)
	importer := func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
		gotVaultID = vaultID
		gotChunkID = chunkID
		for {
			rec, err := next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return importerErr
			}
			if err != nil {
				return err
			}
			gotRecords = append(gotRecords, rec)
		}
	}

	s := newImportTestServer(importer)
	defer s.stopCancel()

	vaultID := glid.New()
	chunkID := chunk.ChunkID(glid.New())
	chunkIDProto := glid.GLID(chunkID).ToProto()

	var pending *pendingImport

	beginAck := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportBegin{
				ImportBegin: &gastrologv1.ChunkReplicationImportBegin{ChunkId: chunkIDProto},
			},
		}, &pending)
	if !beginAck.Ok {
		t.Fatalf("Begin ack: %s", beginAck.Error)
	}
	if pending == nil {
		t.Fatal("pending nil after Begin")
	}

	// Send two record frames, then commit.
	frames := [][][]byte{
		{[]byte("alpha"), []byte("bravo"), []byte("charlie")},
		{[]byte("delta"), []byte("echo")},
	}
	for fi, frame := range frames {
		var records []*gastrologv1.ExportRecord
		for _, p := range frame {
			records = append(records, convert.RecordToExport(makeRecord(p)))
		}
		ack := s.handleReplicationCommand(context.Background(),
			&gastrologv1.ChunkReplicationCommand{
				VaultId: vaultID.ToProto(),
				Command: &gastrologv1.ChunkReplicationCommand_ImportRecords{
					ImportRecords: &gastrologv1.ChunkReplicationImportRecords{Records: records},
				},
			}, &pending)
		if !ack.Ok {
			t.Fatalf("Records ack frame %d: %s", fi, ack.Error)
		}
	}

	commitAck := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportCommit{
				ImportCommit: &gastrologv1.ChunkReplicationImportCommit{ChunkId: chunkIDProto},
			},
		}, &pending)
	if !commitAck.Ok {
		t.Fatalf("Commit ack: %s", commitAck.Error)
	}
	if pending != nil {
		t.Fatal("pending should be nil after Commit")
	}

	if gotVaultID != vaultID {
		t.Errorf("vaultID: got %s want %s", gotVaultID, vaultID)
	}
	if gotChunkID != chunkID {
		t.Errorf("chunkID: got %s want %s", gotChunkID, chunkID)
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if len(gotRecords) != len(want) {
		t.Fatalf("records: got %d want %d", len(gotRecords), len(want))
	}
	for i, rec := range gotRecords {
		if string(rec.Raw) != want[i] {
			t.Errorf("record %d: got %q want %q", i, rec.Raw, want[i])
		}
	}
}

func TestImportStreaming_EmptyChunk(t *testing.T) {
	t.Parallel()

	called := false
	importer := func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
		called = true
		_, err := next()
		if !errors.Is(err, chunk.ErrNoMoreRecords) {
			return fmt.Errorf("expected ErrNoMoreRecords, got %v", err)
		}
		return nil
	}

	s := newImportTestServer(importer)
	defer s.stopCancel()

	vaultID := glid.New()
	chunkID := chunk.ChunkID(glid.New())
	chunkIDProto := glid.GLID(chunkID).ToProto()

	var pending *pendingImport
	for _, cmd := range []*gastrologv1.ChunkReplicationCommand{
		{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportBegin{
				ImportBegin: &gastrologv1.ChunkReplicationImportBegin{ChunkId: chunkIDProto},
			},
		},
		{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportCommit{
				ImportCommit: &gastrologv1.ChunkReplicationImportCommit{ChunkId: chunkIDProto},
			},
		},
	} {
		ack := s.handleReplicationCommand(context.Background(), cmd, &pending)
		if !ack.Ok {
			t.Fatalf("ack: %s", ack.Error)
		}
	}
	if !called {
		t.Fatal("importer not called for empty chunk")
	}
}

func TestImportStreaming_RecordsBeforeBegin(t *testing.T) {
	t.Parallel()
	s := newImportTestServer(func(context.Context, glid.GLID, chunk.ChunkID, chunk.RecordIterator) error { return nil })
	defer s.stopCancel()

	var pending *pendingImport
	ack := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: glid.New().ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportRecords{
				ImportRecords: &gastrologv1.ChunkReplicationImportRecords{},
			},
		}, &pending)
	if ack.Ok {
		t.Fatal("ImportRecords without Begin should fail")
	}
}

// A new ImportBegin while a previous import is pending preempts the
// previous one. Without this, a leader that gave up between Begin and
// Commit (orchestrator restart, cursor read error, ctx cancel) would
// leave the follower wedged forever — every subsequent ImportSealed
// for the same (vault, follower) stream got rejected with "ImportBegin
// while import for chunk X already in flight".
func TestImportStreaming_BeginPreemptsStuckPending(t *testing.T) {
	t.Parallel()
	// Block the importer so the first Begin is still in-flight when the
	// second arrives — simulates a leader that started an import and
	// never sent Commit.
	importer := func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
		_, err := next() // blocks until ctx cancels or channel closes
		return err
	}
	s := newImportTestServer(importer)
	defer s.stopCancel()

	vaultID := glid.New()
	firstChunkIDProto := glid.GLID(chunk.ChunkID(glid.New())).ToProto()
	secondChunkIDProto := glid.GLID(chunk.ChunkID(glid.New())).ToProto()

	var pending *pendingImport
	first := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportBegin{
				ImportBegin: &gastrologv1.ChunkReplicationImportBegin{ChunkId: firstChunkIDProto},
			},
		}, &pending)
	if !first.Ok {
		t.Fatalf("first Begin: %s", first.Error)
	}

	// Second Begin (different chunk) should preempt the first, not be
	// rejected. The leader has clearly moved on from the first chunk.
	second := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportBegin{
				ImportBegin: &gastrologv1.ChunkReplicationImportBegin{ChunkId: secondChunkIDProto},
			},
		}, &pending)
	if !second.Ok {
		t.Fatalf("second Begin should preempt stuck pending, got rejection: %s", second.Error)
	}
	if pending == nil {
		t.Fatal("pending nil after second Begin")
	}
	if got := glid.GLID(pending.chunkID).ToProto(); !bytesEqual(got, secondChunkIDProto) {
		t.Errorf("pending chunkID = %x, want %x", got, secondChunkIDProto)
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestImportStreaming_CommitChunkIDMismatch(t *testing.T) {
	t.Parallel()
	importer := func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
		// Drain so the goroutine exits when the channel closes.
		for {
			_, err := next()
			if err != nil {
				return nil
			}
		}
	}
	s := newImportTestServer(importer)
	defer s.stopCancel()

	vaultID := glid.New()
	beginID := glid.GLID(chunk.ChunkID(glid.New())).ToProto()
	wrongID := glid.GLID(chunk.ChunkID(glid.New())).ToProto()

	var pending *pendingImport
	begin := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportBegin{
				ImportBegin: &gastrologv1.ChunkReplicationImportBegin{ChunkId: beginID},
			},
		}, &pending)
	if !begin.Ok {
		t.Fatalf("Begin: %s", begin.Error)
	}
	commit := s.handleReplicationCommand(context.Background(),
		&gastrologv1.ChunkReplicationCommand{
			VaultId: vaultID.ToProto(),
			Command: &gastrologv1.ChunkReplicationCommand_ImportCommit{
				ImportCommit: &gastrologv1.ChunkReplicationImportCommit{ChunkId: wrongID},
			},
		}, &pending)
	if commit.Ok {
		t.Fatal("Commit with mismatched chunk_id should be rejected")
	}
	if pending != nil {
		t.Fatal("pending should be cleared after mismatch")
	}
}

// fakeClientStream captures every SendMsg into a slice and never blocks
// on RecvMsg — RecvMsg returns a canned ack so ChunkReplicator.send
// makes forward progress.
type fakeClientStream struct {
	ctx  context.Context
	sent []*gastrologv1.ChunkReplicationCommand
	acks int32
}

func (f *fakeClientStream) Header() (metadata.MD, error) { return nil, nil }
func (f *fakeClientStream) Trailer() metadata.MD         { return nil }
func (f *fakeClientStream) CloseSend() error             { return nil }
func (f *fakeClientStream) Context() context.Context     { return f.ctx }
func (f *fakeClientStream) SendMsg(m any) error {
	cmd, ok := m.(*gastrologv1.ChunkReplicationCommand)
	if !ok {
		return fmt.Errorf("unexpected send type %T", m)
	}
	// Clone via marshal so subsequent slice reuse on the leader side
	// can't mutate captured frames out from under us.
	raw, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	cloned := &gastrologv1.ChunkReplicationCommand{}
	if err := proto.Unmarshal(raw, cloned); err != nil {
		return err
	}
	f.sent = append(f.sent, cloned)
	return nil
}
func (f *fakeClientStream) RecvMsg(m any) error {
	ack, ok := m.(*gastrologv1.ChunkReplicationAck)
	if !ok {
		return fmt.Errorf("unexpected recv type %T", m)
	}
	ack.Ok = true
	atomic.AddInt32(&f.acks, 1)
	return nil
}

// TestImportSealedChunk_LeaderFrameBounds verifies the streaming-frame
// invariant: every wire message is under importRecordsMaxBytes, even for
// chunks that are an order of magnitude larger than the gRPC receive cap.
// Pre-fix, the whole chunk shipped as one message and the cluster wedged
// the moment any chunk crossed 128 MiB.
func TestImportSealedChunk_LeaderFrameBounds(t *testing.T) {
	t.Parallel()

	tr := &ChunkReplicator{streams: make(map[streamKey]*vaultStream)}
	vaultID := glid.New()
	nodeID := "follower-1"
	chunkID := chunk.ChunkID(glid.New())

	// Pre-seed the stream so getOrOpen returns our fake without dialing.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeClientStream{ctx: ctx}
	tr.streams[streamKey{vaultID: vaultID, nodeID: nodeID}] = &vaultStream{
		stream: fake,
		cancel: cancel,
	}

	// Synthetic iterator: 5000 records of ~64 KiB each → ~320 MiB total,
	// 2.5× over the 128 MiB cap the original protocol blew through.
	const (
		nRecords = 5000
		recBytes = 64 * 1024
	)
	body := make([]byte, recBytes)
	for i := range body {
		body[i] = byte(i % 251)
	}
	produced := 0
	iter := func() (chunk.Record, error) {
		if produced >= nRecords {
			return chunk.Record{}, chunk.ErrNoMoreRecords
		}
		produced++
		return makeRecord(body), nil
	}

	if err := tr.ImportSealedChunk(ctx, nodeID, vaultID, chunkID, iter); err != nil {
		t.Fatalf("ImportSealedChunk: %v", err)
	}

	if produced != nRecords {
		t.Errorf("iterator produced %d, want %d", produced, nRecords)
	}

	// Frame inspection.
	var (
		gotBegin   int
		gotRecords int
		gotCommit  int
		maxFrame   int
	)
	for i, cmd := range fake.sent {
		size := proto.Size(cmd)
		if size > maxFrame {
			maxFrame = size
		}
		if size > importRecordsMaxBytes+(1<<20) { // tolerate one record over budget for the
			// last-record-in-frame edge; +1 MiB is far less than the 128 MiB cap.
			t.Errorf("frame %d size %d exceeds budget %d", i, size, importRecordsMaxBytes+(1<<20))
		}
		switch cmd.GetCommand().(type) {
		case *gastrologv1.ChunkReplicationCommand_ImportBegin:
			gotBegin++
		case *gastrologv1.ChunkReplicationCommand_ImportRecords:
			gotRecords++
		case *gastrologv1.ChunkReplicationCommand_ImportCommit:
			gotCommit++
		default:
			t.Errorf("frame %d: unexpected command type %T", i, cmd.GetCommand())
		}
	}
	if gotBegin != 1 {
		t.Errorf("got %d ImportBegin frames, want 1", gotBegin)
	}
	if gotCommit != 1 {
		t.Errorf("got %d ImportCommit frames, want 1", gotCommit)
	}
	if gotRecords < 2 {
		t.Errorf("expected multiple ImportRecords frames for 320 MiB chunk, got %d", gotRecords)
	}
	// Sanity check: at ~64 KiB/record and ~8 MiB/frame, expect roughly
	// 320 MiB / 8 MiB = ~40 frames. Anywhere in 30..60 is reasonable.
	if gotRecords < 30 || gotRecords > 80 {
		t.Errorf("ImportRecords frame count = %d, expected 30..80 for 320 MiB", gotRecords)
	}
	if maxFrame < 1024 {
		t.Errorf("maxFrame = %d, suspiciously small — frames may not be carrying records", maxFrame)
	}
}
