package server_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// stubVaultValidator answers for peers. A node in dead fails with a transport
// error; every other node returns an audit naming itself, plus one unreadable
// chunk when listed in damaged.
type stubVaultValidator struct {
	dead    map[string]bool
	damaged map[string]bool
	// blank, when true, omits the node ID from the peer's answer, modelling a
	// peer that does not name itself and must be attributed by the requester.
	blank bool
}

func (s *stubVaultValidator) ValidateVault(_ context.Context, nodeID string, _ *gastrologv1.ForwardValidateVaultRequest) (*gastrologv1.ForwardValidateVaultResponse, error) {
	if s.dead[nodeID] {
		return nil, errors.New("connection refused")
	}
	answeringNode := nodeID
	if s.blank {
		answeringNode = ""
	}
	resp := &gastrologv1.ForwardValidateVaultResponse{
		Valid: !s.damaged[nodeID],
		CloudIndexAudit: &gastrologv1.CloudIndexAudit{
			NodeId:       answeringNode,
			StoreObjects: 3,
		},
	}
	if s.damaged[nodeID] {
		resp.Chunks = []*gastrologv1.ChunkValidation{{
			NodeId: answeringNode,
			Valid:  false,
			Issues: []string{"cannot open cursor"},
		}}
	}
	return resp, nil
}

// Damage on a peer must reach the caller. Validating only where the request
// landed is the bug this covers: chunk bytes and the cloud index are per-node,
// so a local-only check reports a healthy vault while a peer holds the broken
// copy.
func TestValidateVault_FanOutSurfacesRemoteDamage(t *testing.T) {
	t.Parallel()

	const localID, healthyID, damagedID = "node-local", "node-healthy", "node-damaged"
	validator := &stubVaultValidator{damaged: map[string]bool{damagedID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{healthyID, damagedID},
		contributionDeps{validator: validator})

	resp, err := vs.ValidateVault(context.Background(),
		connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ValidateVault: %v", err)
	}
	if resp.Msg.GetValid() {
		t.Error("Valid = true with a damaged chunk on a peer")
	}
	var sawDamagedNode bool
	for _, cv := range resp.Msg.GetChunks() {
		if cv.GetNodeId() == damagedID && !cv.GetValid() {
			sawDamagedNode = true
		}
	}
	if !sawDamagedNode {
		t.Errorf("no finding attributed to %s; the merged result cannot say where to look", damagedID)
	}
	if len(resp.Msg.GetCloudIndexAudits()) != 2 {
		t.Errorf("got %d cloud audits, want 2 (one per peer)", len(resp.Msg.GetCloudIndexAudits()))
	}
}

// A peer that cannot be reached must be named. "No damage found" over a subset
// of nodes is the one output an operator must never read as an all-clear.
func TestValidateVault_PartialFanOutNamesDeadPeer(t *testing.T) {
	t.Parallel()

	const localID, aliveID, deadID = "node-local", "node-alive", "node-dead"
	validator := &stubVaultValidator{dead: map[string]bool{deadID: true}}
	vs, vaultID := newContributionVaultServer(t, localID, []string{aliveID, deadID},
		contributionDeps{validator: validator})

	resp, err := vs.ValidateVault(context.Background(),
		connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ValidateVault: %v", err)
	}
	report := resp.Msg.GetContributionReport()
	if report == nil {
		t.Fatalf("ContributionReport = nil, want the dead peer named")
	}
	if len(report.GetDegraded()) != 1 || report.GetDegraded()[0].GetNodeId() != deadID {
		t.Errorf("degraded = %+v, want just %s", report.GetDegraded(), deadID)
	}
}

// A peer's answer carries no node ID of its own — the responding side does not
// need to name itself — so the requester stamps the peer it asked. Without this
// every remote finding reads as local.
func TestValidateVault_AttributesUnnamedPeerAnswers(t *testing.T) {
	t.Parallel()

	const localID, peerID = "node-local", "node-peer"
	validator := &stubVaultValidator{damaged: map[string]bool{peerID: true}, blank: true}
	vs, vaultID := newContributionVaultServer(t, localID, []string{peerID},
		contributionDeps{validator: validator})

	resp, err := vs.ValidateVault(context.Background(),
		connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ValidateVault: %v", err)
	}
	for _, cv := range resp.Msg.GetChunks() {
		if !cv.GetValid() && cv.GetNodeId() != peerID {
			t.Errorf("remote finding attributed to %q, want %s", cv.GetNodeId(), peerID)
		}
	}
	for _, a := range resp.Msg.GetCloudIndexAudits() {
		if a.GetNodeId() != peerID {
			t.Errorf("remote audit attributed to %q, want %s", a.GetNodeId(), peerID)
		}
	}
}

// No validator wired (single-node deployment) must still validate locally
// rather than fail or report nothing.
func TestValidateVault_WithoutRemoteValidator(t *testing.T) {
	t.Parallel()

	vs, vaultID := newContributionVaultServer(t, "node-local", nil, contributionDeps{})

	resp, err := vs.ValidateVault(context.Background(),
		connect.NewRequest(&gastrologv1.ValidateVaultRequest{Vault: vaultID}))
	if err != nil {
		t.Fatalf("ValidateVault: %v", err)
	}
	if !resp.Msg.GetValid() {
		t.Error("an empty local vault reports invalid")
	}
	if resp.Msg.GetContributionReport() != nil {
		t.Error("ContributionReport set with no peers to fan out to")
	}
}
