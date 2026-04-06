package raftfsm

import (
	"log/slog"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// Announcer implements chunk.MetadataAnnouncer by applying commands to
// a tier's Raft group. Only the tier leader can apply — non-leaders skip
// silently. All methods are best-effort.
type Announcer struct {
	raft    *hraft.Raft
	timeout time.Duration
	logger  *slog.Logger
}

// NewAnnouncer creates an announcer that applies chunk metadata commands
// to the given Raft instance.
func NewAnnouncer(r *hraft.Raft, timeout time.Duration, logger *slog.Logger) *Announcer {
	return &Announcer{raft: r, timeout: timeout, logger: logger}
}

var _ chunk.MetadataAnnouncer = (*Announcer)(nil)

func (a *Announcer) AnnounceCreate(id chunk.ChunkID, writeStart, ingestStart, sourceStart time.Time) {
	a.apply("create", id, MarshalCreateChunk(id, writeStart, ingestStart, sourceStart))
}

func (a *Announcer) AnnounceSeal(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestEnd, sourceEnd time.Time) {
	a.apply("seal", id, MarshalSealChunk(id, writeEnd, recordCount, bytes, ingestEnd, sourceEnd))
}

func (a *Announcer) AnnounceCompress(id chunk.ChunkID, diskBytes int64) {
	a.apply("compress", id, MarshalCompressChunk(id, diskBytes))
}

func (a *Announcer) AnnounceUpload(id chunk.ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, numFrames int32) {
	a.apply("upload", id, MarshalUploadChunk(id, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize, numFrames))
}

func (a *Announcer) AnnounceDelete(id chunk.ChunkID) {
	a.apply("delete", id, MarshalDeleteChunk(id))
}

func (a *Announcer) apply(op string, id chunk.ChunkID, data []byte) {
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
