package chunking

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func testSealKey(sec int64) buildKey {
	return buildKey{chunkID: chunk.NewChunkID(), sealedAt: time.Unix(sec, 0)}
}

func TestSealProgressBuiltOncePerKey(t *testing.T) {
	var p sealProgress
	key, other := testSealKey(1), testSealKey(2)
	if p.alreadyBuilt(key) {
		t.Fatal("built before markBuilt")
	}
	p.markBuilt(key, BuildResult{RecordCount: 7})
	if !p.alreadyBuilt(key) {
		t.Fatal("not built after markBuilt")
	}
	if p.alreadyBuilt(other) {
		t.Fatal("other key reports built")
	}
}

func TestSealProgressCachedResultKeyed(t *testing.T) {
	var p sealProgress
	key, other := testSealKey(1), testSealKey(2)
	if _, ok := p.cachedResult(key); ok {
		t.Fatal("cache hit before markBuilt")
	}
	p.markBuilt(key, BuildResult{RecordCount: 7})
	result, ok := p.cachedResult(key)
	if !ok || result.RecordCount != 7 {
		t.Fatalf("cachedResult = %+v, %v; want RecordCount 7, true", result, ok)
	}
	if _, ok := p.cachedResult(other); ok {
		t.Fatal("cache hit for other key")
	}
	p.markBuilt(other, BuildResult{RecordCount: 9})
	if _, ok := p.cachedResult(key); ok {
		t.Fatal("stale cache survives markBuilt for a new key")
	}
}

func TestSealProgressProposeOncePerKey(t *testing.T) {
	var p sealProgress
	key := testSealKey(1)
	if !p.shouldPropose(key) {
		t.Fatal("shouldPropose false before markProposed")
	}
	p.markProposed(key)
	if p.shouldPropose(key) {
		t.Fatal("shouldPropose true after markProposed")
	}
	p.resetSealProposal()
	if !p.shouldPropose(key) {
		t.Fatal("shouldPropose false after resetSealProposal")
	}
}

func TestSealProgressResetSealProposalIf(t *testing.T) {
	var p sealProgress
	key, other := testSealKey(1), testSealKey(2)
	p.markProposed(key)
	if p.resetSealProposalIf(other) {
		t.Fatal("reset for a key that was not proposed")
	}
	if p.shouldPropose(key) {
		t.Fatal("mismatched resetSealProposalIf cleared the guard")
	}
	if !p.resetSealProposalIf(key) {
		t.Fatal("resetSealProposalIf false for the proposed key")
	}
	if !p.shouldPropose(key) {
		t.Fatal("guard still set after resetSealProposalIf")
	}
}

func TestSealProgressClaimOnBuiltFiresOnce(t *testing.T) {
	var p sealProgress
	key, other := testSealKey(1), testSealKey(2)
	if !p.claimOnBuilt(key) {
		t.Fatal("first claimOnBuilt lost")
	}
	if p.claimOnBuilt(key) {
		t.Fatal("second claimOnBuilt won")
	}
	if !p.claimOnBuilt(other) {
		t.Fatal("claimOnBuilt for a new cycle lost")
	}
}

func TestSealProgressClaimPostSealRequiresBuild(t *testing.T) {
	var p sealProgress
	key := testSealKey(1)
	if p.claimPostSeal(key) {
		t.Fatal("post-seal claimed before markBuilt")
	}
	p.markBuilt(key, BuildResult{})
	if !p.claimPostSeal(key) {
		t.Fatal("first claimPostSeal after build lost")
	}
	if p.claimPostSeal(key) {
		t.Fatal("second claimPostSeal won")
	}
	if !p.postSealDone(key) {
		t.Fatal("postSealDone false after claim")
	}
}

func TestSealProgressNoteSealClearedResetsGuards(t *testing.T) {
	var p sealProgress
	key := testSealKey(1)
	manifest := &vaultctlfsm.OpenChunkManifest{
		ChunkID: key.chunkID,
		Refs:    []vaultctlfsm.OpenChunkSegmentRef{{FirstRecordNumber: 1}},
	}
	p.setPending(manifest)
	p.markProposed(key)
	if !p.claimOnBuilt(key) {
		t.Fatal("setup: claimOnBuilt lost")
	}

	cleared := p.noteSealCleared()
	if cleared == nil || cleared.ChunkID != manifest.ChunkID {
		t.Fatalf("noteSealCleared = %+v; want copy of pending", cleared)
	}
	cleared.Refs[0].FirstRecordNumber = 99
	if manifest.Refs[0].FirstRecordNumber != 1 {
		t.Fatal("noteSealCleared returned a shallow Refs copy")
	}
	if !p.shouldPropose(key) {
		t.Fatal("seal-proposed guard survived noteSealCleared")
	}
	if !p.claimOnBuilt(key) {
		t.Fatal("OnBuilt guard survived noteSealCleared")
	}
	if p.pendingManifest() == nil {
		t.Fatal("noteSealCleared dropped the retained pending manifest")
	}
}

func TestSealProgressNoteSealClearedNoPending(t *testing.T) {
	var p sealProgress
	key := testSealKey(1)
	p.markProposed(key)
	if cleared := p.noteSealCleared(); cleared != nil {
		t.Fatalf("noteSealCleared = %+v; want nil without pending", cleared)
	}
	if !p.shouldPropose(key) {
		t.Fatal("guards not reset when pending is nil")
	}
}

func TestSealProgressClearPendingAfterBuilt(t *testing.T) {
	var p sealProgress
	key := testSealKey(1)
	manifest := &vaultctlfsm.OpenChunkManifest{ChunkID: key.chunkID}
	p.setPending(manifest)

	p.clearPendingAfterBuilt(key.chunkID, key)
	if p.pendingManifest() == nil {
		t.Fatal("pending cleared before markBuilt")
	}
	p.markBuilt(key, BuildResult{})
	p.clearPendingAfterBuilt(chunk.NewChunkID(), key)
	if p.pendingManifest() == nil {
		t.Fatal("pending cleared for a different chunk")
	}
	p.clearPendingAfterBuilt(key.chunkID, key)
	if p.pendingManifest() != nil {
		t.Fatal("pending retained after built clear")
	}
}
