package multiraft

import (
	"log/slog"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// RaftAnnouncer implements chunk.MetadataAnnouncer by applying commands to
// a tier's Raft group. All methods are best-effort — failures are logged
// but don't block the local operation.
type RaftAnnouncer struct {
	raft    *hraft.Raft
	timeout time.Duration
	logger  *slog.Logger
}

// NewRaftAnnouncer creates an announcer that applies chunk metadata commands
// to the given Raft instance.
func NewRaftAnnouncer(r *hraft.Raft, timeout time.Duration, logger *slog.Logger) *RaftAnnouncer {
	return &RaftAnnouncer{raft: r, timeout: timeout, logger: logger}
}

var _ chunk.MetadataAnnouncer = (*RaftAnnouncer)(nil)

func (a *RaftAnnouncer) AnnounceCreate(id chunk.ChunkID, writeStart, ingestStart, sourceStart time.Time) {
	a.apply("create", id, MarshalCreateChunk(id, writeStart, ingestStart, sourceStart))
}

func (a *RaftAnnouncer) AnnounceSeal(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestEnd, sourceEnd time.Time) {
	a.apply("seal", id, MarshalSealChunk(id, writeEnd, recordCount, bytes, ingestEnd, sourceEnd))
}

func (a *RaftAnnouncer) AnnounceCompress(id chunk.ChunkID, diskBytes int64) {
	a.apply("compress", id, MarshalCompressChunk(id, diskBytes))
}

func (a *RaftAnnouncer) AnnounceUpload(id chunk.ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, numFrames int32) {
	a.apply("upload", id, MarshalUploadChunk(id, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize, numFrames))
}

func (a *RaftAnnouncer) AnnounceDelete(id chunk.ChunkID) {
	a.apply("delete", id, MarshalDeleteChunk(id))
}

func (a *RaftAnnouncer) apply(op string, id chunk.ChunkID, data []byte) {
	// Only the Raft leader can apply. Secondaries also run PostSealProcess
	// (compress, index) but their announcements are redundant — the primary
	// already announced. Skip silently to avoid log noise.
	if a.raft.State() != hraft.Leader {
		return
	}
	f := a.raft.Apply(data, a.timeout)
	if err := f.Error(); err != nil {
		if a.logger != nil {
			a.logger.Warn("chunk metadata announce failed",
				"op", op, "chunk", id.String(), "error", err)
		}
	}
}
