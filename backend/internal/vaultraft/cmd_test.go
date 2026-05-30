package vaultraft

import (
	"bytes"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

func TestMarshalVaultReserveSeqRange(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	cmd, err := MarshalVaultReserveSeqRange(vaultID, "node-1", vaultctlfsm.InitialSeqEpoch, 100)
	if err != nil {
		t.Fatal(err)
	}
	// The wire is a marshaled VaultRaftCommand wrapping a vault-scoped
	// ReserveSeqRange for vaultID (gastrolog-5lrg7).
	var decoded gastrologv1.VaultRaftCommand
	if err := proto.Unmarshal(cmd, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	scoped := decoded.GetVaultScoped()
	if scoped == nil {
		t.Fatalf("expected vault-scoped command, got %T", decoded.GetCommand())
	}
	if !bytes.Equal(scoped.GetVaultId(), vaultID[:]) {
		t.Fatalf("vault id: %x != %x", scoped.GetVaultId(), vaultID[:])
	}
	if scoped.GetCommand().GetReserveSeqRange() == nil {
		t.Fatalf("expected reserve-seq-range inner, got %T", scoped.GetCommand().GetCommand())
	}

	f := NewFSM()
	resp := f.Apply(&hraft.Log{Data: cmd})
	grant, ok := resp.(vaultctlfsm.SeqLeaseGrant)
	if !ok {
		t.Fatalf("response: %T %v", resp, resp)
	}
	if grant.Start != 1 || grant.End != 100 {
		t.Fatalf("grant: %+v", grant)
	}
}

func TestMarshalVaultBumpSeqAllocatorEpoch(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	reserve, err := MarshalVaultReserveSeqRange(vaultID, "node-1", vaultctlfsm.InitialSeqEpoch, 5)
	if err != nil {
		t.Fatal(err)
	}
	f := NewFSM()
	if r := f.Apply(&hraft.Log{Data: reserve}); r != nil {
		if err, ok := r.(error); ok {
			t.Fatal(err)
		}
	}

	bump := MarshalVaultBumpSeqAllocatorEpoch(vaultID)
	resp := f.Apply(&hraft.Log{Data: bump})
	newEpoch, ok := resp.(uint64)
	if !ok {
		t.Fatalf("bump response: %T %v", resp, resp)
	}
	if newEpoch != vaultctlfsm.InitialSeqEpoch+1 {
		t.Fatalf("epoch: %d", newEpoch)
	}
}
