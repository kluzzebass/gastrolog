package file

import (
	"os"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// scanAttrsCount drains ScanAttrs, asserting each projected record carries the
// attrs buildSealedGLCB wrote, and returns how many records were scanned.
func scanAttrsCount(t *testing.T, m *Manager, id chunk.ChunkID) int {
	t.Helper()
	var n int
	err := m.ScanAttrs(id, 0, func(_ time.Time, attrs chunk.Attributes) bool {
		if attrs["level"] != "info" {
			t.Errorf("record %d attrs = %v, want level=info", n, attrs)
		}
		n++
		return true
	})
	if err != nil {
		t.Fatalf("ScanAttrs: %v", err)
	}
	return n
}

// TestScanAttrsViaGLCBSealed drives the sealed-with-local-GLCB dispatch: the
// projection scan must yield every record's attrs.
func TestScanAttrsViaGLCBSealed(t *testing.T) {
	m, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	id, _ := buildSealedGLCB(t, m, 5)
	if got := scanAttrsCount(t, m, id); got != 5 {
		t.Fatalf("sealed GLCB scan yielded %d records, want 5", got)
	}
}

// TestScanAttrsCloudBackedWithLocalGLCB drives the cloud-backed branch when the
// warm cache still holds data.glcb: ScanAttrs routes through the local
// projection cursor rather than skipping.
func TestScanAttrsCloudBackedWithLocalGLCB(t *testing.T) {
	m, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	id, _ := buildSealedGLCB(t, m, 4)

	// Flip to cloud-backed but keep the warm-cache data.glcb on disk.
	m.mu.Lock()
	m.lookupMeta(id).cloudBacked = true
	m.mu.Unlock()

	if got := scanAttrsCount(t, m, id); got != 4 {
		t.Fatalf("cloud-backed-with-local-GLCB scan yielded %d records, want 4", got)
	}
}

// TestScanAttrsCloudBackedNoLocalGLCBNoFetch pins the no-fetch contract: a
// cloud-backed chunk with no warm-cache blob must NOT download — ScanAttrs
// returns nil with zero records scanned, leaving the histogram bucket hatched.
func TestScanAttrsCloudBackedNoLocalGLCBNoFetch(t *testing.T) {
	m, err := NewManager(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	id, glcbPath := buildSealedGLCB(t, m, 3)

	m.mu.Lock()
	m.lookupMeta(id).cloudBacked = true
	m.mu.Unlock()

	// Drop the warm-cache blob so no local GLCB remains.
	m.evictMappedGLCB(id)
	if err := os.Remove(glcbPath); err != nil {
		t.Fatalf("remove data.glcb: %v", err)
	}
	if m.hasLocalGLCB(id) {
		t.Fatal("expected no local GLCB after removal")
	}

	var scanned int
	if err := m.ScanAttrs(id, 0, func(time.Time, chunk.Attributes) bool {
		scanned++
		return true
	}); err != nil {
		t.Fatalf("ScanAttrs: %v", err)
	}
	if scanned != 0 {
		t.Fatalf("no-fetch cloud scan yielded %d records, want 0", scanned)
	}
}
