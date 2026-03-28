package chunk

import "time"

// MetadataAnnouncer is called by chunk managers after each metadata state
// change. The implementation typically applies the change to a Raft group
// for cluster-wide visibility. All methods are best-effort — failures are
// logged but don't block the local operation.
//
// When nil, no announcements are made (single-node mode, tests).
type MetadataAnnouncer interface {
	AnnounceCreate(id ChunkID, writeStart, ingestStart, sourceStart time.Time)
	AnnounceSeal(id ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestEnd, sourceEnd time.Time)
	AnnounceCompress(id ChunkID, diskBytes int64)
	AnnounceUpload(id ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, numFrames int32)
	AnnounceDelete(id ChunkID)
}
