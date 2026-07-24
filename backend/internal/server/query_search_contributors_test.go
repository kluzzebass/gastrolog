package server_test

import (
	"context"
	"io"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
)

// TestMultiNode_SearchReportsContributingVaults pins gastrolog-20lrg(a)
// end-to-end: a merged search's SearchResponse carries the remote vaults
// that contributed. The coordinator (node-A) leads its own vault locally,
// so only the remote node-B vault appears in the contributor set — the
// signal is about the cross-node stream health, and a local vault never
// streams over the wire.
func TestMultiNode_SearchReportsContributingVaults(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"})

	addMNRecords(t, h.Node(t, "node-A"), "A", 3, nil)
	addMNRecords(t, h.Node(t, "node-B"), "B", 3, nil)

	remoteVaultID := h.Node(t, "node-B").vaultID
	localVaultID := h.Node(t, "node-A").vaultID

	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: ""},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	contributors := map[glid.GLID]bool{}
	for stream.Receive() {
		for _, raw := range stream.Msg().ContributingVaultIds {
			contributors[glid.FromBytes(raw)] = true
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}

	if !contributors[remoteVaultID] {
		t.Errorf("contributing vaults %v missing remote vault %s", contributors, remoteVaultID)
	}
	if contributors[localVaultID] {
		t.Errorf("contributing vaults %v include the local vault %s (should be remote-only)", contributors, localVaultID)
	}
}

// TestMultiNode_SearchNoRemoteVaultsNoContributors pins the happy-path
// quiet case: a single-node cluster search fans out to nobody, so the
// contributor list is empty and the UI shows nothing.
func TestMultiNode_SearchNoRemoteVaultsNoContributors(t *testing.T) {
	t.Parallel()
	h := setupMultiNode(t, []string{"node-A", "node-B"}, WithoutVault("node-B"))

	addMNRecords(t, h.Node(t, "node-A"), "A", 3, nil)

	stream, err := h.client.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{
		Query: &gastrologv1.Query{Expression: ""},
	}))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	var contributorCount int
	for stream.Receive() {
		contributorCount += len(stream.Msg().ContributingVaultIds)
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}
	if contributorCount != 0 {
		t.Errorf("contributor count = %d, want 0 (no remote vaults in scope)", contributorCount)
	}
}
