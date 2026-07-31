package chunking

import (
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// burstFSM builds a registry shaped like a burst backlog: n completed
// segments, half fully planned into sealed manifests (refs still queued, so
// the referenced-set walk has real work), holder acks present but one
// required home missing — the release pass's worst case: nothing releases,
// every gate is evaluated for every segment, every pass.
func burstFSM(b *testing.B, n int) *vaultctlfsm.FSM {
	b.Helper()
	fsm := vaultctlfsm.New()
	now := time.Unix(0, 1_700_000_000_000).UTC()
	apply := func(data []byte) {
		if err, ok := fsm.Apply(&hraft.Log{Data: data}).(error); ok && err != nil {
			b.Fatalf("apply: %v", err)
		}
	}
	var refs []vaultctlfsm.OpenChunkSegmentRef
	for i := range n {
		segID := glid.New()
		apply(vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID: segID, RecordCount: 100, ByteSize: 10 << 20,
			FirstIngestTS: now, LastIngestTS: now.Add(time.Second), Checksum: 1, PublishedAt: now,
		}))
		apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "home-a"))
		if i%2 == 0 {
			refs = append(refs, vaultctlfsm.OpenChunkSegmentRef{
				SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 99,
				SliceBytes: 10 << 20, RefAddedAt: now,
			})
		}
	}
	// Queue the refs across sealed manifests of 64 refs each — the
	// referenced-in-manifest walk covers all of them per query.
	for len(refs) > 0 {
		batch := refs
		if len(batch) > 64 {
			batch = refs[:64]
		}
		refs = refs[len(batch):]
		chunkID := chunk.NewChunkID()
		apply(vaultctlfsm.MarshalOpenChunkManifest(chunkID, now))
		apply(vaultctlfsm.MarshalAddOpenChunkSegmentRefs(chunkID, batch))
		apply(vaultctlfsm.MarshalSealOpenChunkManifest(chunkID, now.Add(time.Minute)))
	}
	return fsm
}

// BenchmarkReleasePassScan measures one release pass over a burst backlog the
// way production runs it: one SnapshotReleaseScan, pure gates over it.
func BenchmarkReleasePassScan(b *testing.B) {
	for _, n := range []int{512, 2048, 8192} {
		b.Run(fmt.Sprintf("segments=%d", n), func(b *testing.B) {
			fsm := burstFSM(b, n)
			required := []string{"home-a", "home-b"}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				scan := fsm.SnapshotReleaseScan(2)
				ready := 0
				for i := range scan.Entries {
					if scanMayRelease(scan, &scan.Entries[i], required, true, 0, time.Time{}) {
						ready++
					}
				}
				if ready != 0 {
					b.Fatalf("burst fixture must not release, got %d", ready)
				}
			}
		})
	}
}

// BenchmarkReleasePassPerSegment measures the pre-2m0f75 shape for the same
// decision set: per segment, re-query the FSM the way the old gates did
// (referenced-in-manifest walk + entry copy + resume lookup + supersession).
// Kept as the honest before/after baseline for the acceptance measurement.
func BenchmarkReleasePassPerSegment(b *testing.B) {
	for _, n := range []int{512, 2048, 8192} {
		b.Run(fmt.Sprintf("segments=%d", n), func(b *testing.B) {
			fsm := burstFSM(b, n)
			required := []string{"home-a", "home-b"}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ready := 0
				for _, entry := range fsm.ListCompletedSegments() {
					if fsm.SegmentReferencedInManifest(entry.SegmentID) {
						continue
					}
					e := fsm.GetCompletedSegment(entry.SegmentID)
					if e == nil {
						continue
					}
					if n, ok := fsm.ResumeRecordNumber(e.SegmentID); !ok || n < e.RecordCount {
						continue
					}
					if fsm.SegmentSuperseded(e.SegmentID, 2) || holdersCover(e.Holders, required) {
						ready++
					}
				}
				if ready != 0 {
					b.Fatalf("burst fixture must not release, got %d", ready)
				}
			}
		})
	}
}
