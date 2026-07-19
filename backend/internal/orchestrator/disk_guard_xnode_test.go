package orchestrator

// Cross-node admission coverage (gastrolog-5jobl5 / gastrolog-20ywka): a vault
// protected or size-capped on ANOTHER node must be refused here, learned via
// the NodeStats broadcast. This wires the REAL cluster.PeerState to the gate
// (as app startup does with SetRemoteVault*), rather than a fake lookup — so
// it proves the actual peer-broadcast → gate path, including TTL expiry. The
// server multi-node harness mocks the stat providers and cannot exercise this.

import (
	"errors"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
)

func TestVaultAdmissionGateHonorsPeerBroadcast(t *testing.T) {
	t.Parallel()
	protectedElsewhere := glid.New()
	cappedElsewhere := glid.New()
	healthy := glid.New()

	ttl := time.Minute
	ps := cluster.NewPeerState(ttl)
	// Peer "node-a" broadcasts that it has one vault below its disk floor and
	// one at its max-size bound.
	ps.Update("node-a", &gastrologv1.NodeStats{
		DiskProtectedVaultIds: [][]byte{protectedElsewhere.ToProto()},
		SizeCappedVaultIds:    [][]byte{cappedElsewhere.ToProto()},
	}, time.Now())

	// This node has no local guard state; only the peer broadcast informs it —
	// exactly the common case, since the starved volume is usually elsewhere.
	o := &Orchestrator{}
	o.SetRemoteVaultDiskProtected(ps.VaultDiskProtected)
	o.SetRemoteVaultSizeCapped(ps.VaultSizeCapped)

	if err := o.vaultAdmissionGate(protectedElsewhere); !errors.Is(err, ErrVaultDiskProtect) {
		t.Fatalf("vault protected on a peer must be refused here: got %v", err)
	}
	if err := o.vaultAdmissionGate(cappedElsewhere); !errors.Is(err, ErrVaultMaxSize) {
		t.Fatalf("vault capped on a peer must be refused here: got %v", err)
	}
	if err := o.vaultAdmissionGate(healthy); err != nil {
		t.Fatalf("vault healthy everywhere must be admitted: %v", err)
	}

	// The reporting peer goes silent past the TTL: its verdicts expire, and
	// admission reopens here — a dead peer must not wedge a vault shut forever.
	ps.Update("node-a", &gastrologv1.NodeStats{
		DiskProtectedVaultIds: [][]byte{protectedElsewhere.ToProto()},
		SizeCappedVaultIds:    [][]byte{cappedElsewhere.ToProto()},
	}, time.Now().Add(-2*ttl))
	if err := o.vaultAdmissionGate(protectedElsewhere); err != nil {
		t.Fatalf("expired peer protect must reopen admission: %v", err)
	}
	if err := o.vaultAdmissionGate(cappedElsewhere); err != nil {
		t.Fatalf("expired peer cap must reopen admission: %v", err)
	}
}
