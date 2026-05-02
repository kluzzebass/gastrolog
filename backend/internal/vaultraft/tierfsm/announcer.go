package tierfsm

import (
	"log/slog"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/lifecycle"
)

// Applier applies pre-marshaled tier FSM commands. Implementations handle
// local application and forwarding to the vault-ctl Raft leader as needed.
type Applier interface {
	Apply(data []byte) error
}

// Announcer implements chunk.MetadataAnnouncer by applying commands to
// a tier's Raft group via an Applier. The Applier handles leader routing —
// the Announcer doesn't need to know which node is the Raft leader.
// All methods are best-effort: errors are logged but not propagated.
//
// During shutdown (phase.ShuttingDown()) the Announcer short-circuits
// every apply call before touching the Applier. This prevents the
// "chunk metadata announce failed: raft is already shutdown" warnings
// that fire when the orchestrator's drain queues a last-minute chunk
// event after the local vault-ctl Raft has been torn down. The tier FSM's
// reconcile-on-load pass covers any missed announces on the next
// startup. See gastrolog-1e5ke.
type Announcer struct {
	applier Applier
	phase   *lifecycle.Phase
	logger  *slog.Logger
}

// NewAnnouncer creates an announcer that applies chunk metadata commands
// via the given Applier. The phase parameter may be nil in tests; when
// non-nil the announcer short-circuits apply calls once the process
// begins shutting down.
func NewAnnouncer(applier Applier, phase *lifecycle.Phase, logger *slog.Logger) *Announcer {
	return &Announcer{applier: applier, phase: phase, logger: logger}
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
	// Skip entirely during shutdown. The local raft may already be down
	// (or about to be) — trying to apply would fail with "raft is already
	// shutdown" and produce noise. Missed announces are reconciled on the
	// next startup from local chunk state.
	if a.phase != nil && a.phase.ShuttingDown() {
		return
	}
	if err := a.applier.Apply(data); err != nil {
		if a.logger != nil {
			a.logger.Warn("chunk metadata announce failed",
				"op", op, "chunk", id.String(), "error", err)
		}
	}
}
