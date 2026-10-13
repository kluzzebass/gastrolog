package file

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/chunk/glcb/glcbtest"
	"gastrolog/internal/glid"
)

// A sealed chunk with no records has TS indexes with no entries: lookups
// answer "nothing there", never "no index", whether the index manager reads
// the GLCB through the chunk manager or maps it from its own directory.
func TestEmptyTSIndexesHaveNoEntries(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	id := chunk.NewChunkID()
	path, info := glcbtest.WriteSealedBlob(t, root, id, glid.New(), nil)

	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	if err := cm.RegisterExternalGLCB(id, path, info); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}

	for name, im := range map[string]*Manager{
		"through the chunk manager": NewManager(t.TempDir(), nil, nil, cm),
		"mapped from the index dir": NewManager(root, nil, nil, nil),
	} {
		t.Run(name, func(t *testing.T) {
			for axis, lenFn := range map[string]func(chunk.ChunkID) (uint64, error){
				"ingest": im.IngestIndexLen,
				"source": im.SourceIndexLen,
			} {
				if n, err := lenFn(id); err != nil || n != 0 {
					t.Errorf("%s index len = (%d, %v), want (0, nil)", axis, n, err)
				}
			}
			for axis, find := range map[string]func(chunk.ChunkID, time.Time) (uint64, bool, error){
				"ingest": im.FindIngestEntryIndex,
				"source": im.FindSourceEntryIndex,
			} {
				if _, found, err := find(id, time.Unix(0, 1)); err != nil || found {
					t.Errorf("%s index rank lookup = (found=%v, %v), want (false, nil)", axis, found, err)
				}
			}
		})
	}
}
