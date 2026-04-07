package raftfsm

import (
	"log/slog"
	"time"

	"gastrolog/internal/chunk"
)

// Applier applies pre-marshaled tier FSM commands. Implementations handle
// local application and forwarding to the tier Raft leader as needed.
type Applier interface {
	Apply(data []byte) error
}

// Announcer implements chunk.MetadataAnnouncer by applying commands to
// a tier's Raft group via an Applier. The Applier handles leader routing —
// the Announcer doesn't need to know which node is the Raft leader.
// All methods are best-effort: errors are logged but not propagated.
type Announcer struct {
	applier Applier
	logger  *slog.Logger
}

// NewAnnouncer creates an announcer that applies chunk metadata commands
// via the given Applier.
func NewAnnouncer(applier Applier, logger *slog.Logger) *Announcer {
	return &Announcer{applier: applier, logger: logger}
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
	if err := a.applier.Apply(data); err != nil {
		if a.logger != nil {
			a.logger.Warn("chunk metadata announce failed",
				"op", op, "chunk", id.String(), "error", err)
		}
	}
}
