package orchestrator

// Coverage for single-node/no-group mode: an orchestrator without a
// GroupManager never gets a vault-ctl handle, so the pipeline's origin
// publisher is the fail-closed noHandlePublisher. A publisher that returns nil
// instead marks segments published that no vault-ctl registry ever saw — the
// segment-publish stage counter lies, and nothing is staged for the publisher
// upgrade to republish. Fail-closed means: ingest durability is untouched (ack
// after fsync), the completed segment stays on disk, and the publish counter
// stays honest at zero.

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/system"
)

func TestNoGroupModePublishFailClosed(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "no-group-vault",
			Enabled: true,
			Type:    system.VaultTypeMemory,
		}},
		Routes: []system.RouteConfig{{
			ID:   glid.New(),
			Name: "match-all",
			Stages: []system.RouteStage{
				{Match: &system.MatchStage{Expression: "*"}},
			},
			Destinations: []glid.GLID{vaultID},
			Enabled:      true,
		}},
	}

	orch := newTestOrch(t, Config{
		LocalNodeID: "node-A",
		// Complete the working segment on the first commit so the single
		// record below becomes a completed segment without size/age waits.
		SegmentCompletePolicy: segmentation.CompletePolicy{MaxBytes: 1},
	})
	orch.setSystemLoader(testSystemLoader{cfg: cfg})
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if err := orch.pipeline.Start(context.Background()); err != nil {
		t.Fatalf("pipeline Start: %v", err)
	}
	t.Cleanup(func() { _ = orch.pipeline.Stop() })

	// Ingest durability is independent of the refused publish: the ack fires
	// after the durable segment commit, publisher outcome notwithstanding.
	ack := make(chan error, 1)
	rec := chunk.Record{
		IngestTS: time.Now().UTC(),
		Attrs:    map[string]string{"k": "v"},
		Raw:      []byte("fail-closed-but-durable"),
	}
	if err := orch.SubmitIngest(context.Background(), rec, ack); err != nil {
		t.Fatalf("SubmitIngest: %v", err)
	}
	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("durability ack: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no durability ack")
	}

	// The record's segment completes and STAYS in completed/ — durable truth
	// on disk, never dropped by the refused publish.
	root, err := orch.originRoot(vaultID)
	if err != nil {
		t.Fatalf("originRoot: %v", err)
	}
	var completed map[glid.GLID]struct{}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ids, err := paths.ListSegmentIDs(paths.CompletedDir(root))
		if err == nil && len(ids) > 0 {
			completed = ids
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no completed segment appeared under %s (err=%v)", paths.CompletedDir(root), err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Fail-closed honesty: with no vault-ctl handle the publish counter must
	// stay 0 (the noop publisher used to count these as published), and no
	// head/ promotion may happen (this origin is not a holder).
	for _, st := range orch.pipeline.PublishStats() {
		if st.VaultID == vaultID && st.Published != 0 {
			t.Fatalf("PublishStats = %d for vault without a vault-ctl handle, want 0", st.Published)
		}
	}
	head, err := paths.ListSegmentIDs(paths.HeadDir(root))
	if err == nil {
		for id := range completed {
			if _, promoted := head[id]; promoted {
				t.Fatalf("segment %s promoted to head/ without a vault-ctl publish", id)
			}
		}
	}
}
