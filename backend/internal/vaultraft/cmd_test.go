package vaultraft

import (
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestMarshalVaultReserveSeqRange(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	cmd, err := MarshalVaultReserveSeqRange(vaultID, "node-1", vaultctlfsm.InitialSeqEpoch, 100)
	if err != nil {
		t.Fatal(err)
	}
	if cmd[0] != OpVaultChunkFSM {
		t.Fatalf("opcode: %d", cmd[0])
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
