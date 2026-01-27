package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	indextime "github.com/kluzzebass/gastrolog/internal/index/time"
)

// TimeIndexer builds a sparse time index for sealed chunks.
// For each chunk, it samples every N-th record's (IngestTS, RecordPos)
// and writes the result to <dir>/<chunkID>/time.idx.
type TimeIndexer struct {
	dir      string
	manager  chunk.ChunkManager
	sparsity int
}

func NewTimeIndexer(dir string, manager chunk.ChunkManager, sparsity int) *TimeIndexer {
	return &TimeIndexer{
		dir:      dir,
		manager:  manager,
		sparsity: sparsity,
	}
}

func (t *TimeIndexer) Name() string {
	return "time"
}

func (t *TimeIndexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	cursor, err := t.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	var entries []indextime.IndexEntry
	n := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rec, ref, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			return fmt.Errorf("read record: %w", err)
		}

		if n == 0 || n%t.sparsity == 0 {
			entries = append(entries, indextime.IndexEntry{
				TimestampUS: rec.IngestTS.UnixMicro(),
				RecordPos:   ref.Pos,
			})
		}
		n++
	}

	data := encodeIndex(entries)

	chunkDir := filepath.Join(t.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	target := filepath.Join(chunkDir, indexFileName)
	tmp := target + ".tmp"

	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp index: %w", err)
	}

	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename index: %w", err)
	}

	return nil
}
