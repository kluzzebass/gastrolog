package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func TestRecordRefFromChunkMaterialized(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	chunkID := chunk.ChunkID(glid.New())
	ref := recordRefFromChunk(chunk.Record{
		VaultID:  vaultID,
		VaultSeq: 99,
		Ref:      chunk.RecordRef{ChunkID: chunkID, Pos: 3},
	})
	if ref.GetVaultSeq() != 0 {
		t.Fatalf("materialized ref must not set vault_seq; got %d", ref.GetVaultSeq())
	}
	if chunk.ChunkID(glid.FromBytes(ref.GetChunkId())) != chunkID || ref.GetPos() != 3 {
		t.Fatalf("chunk ref = %+v", ref)
	}
}

func TestRecordRefFromChunkSpool(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	ref := recordRefFromChunk(chunk.Record{
		VaultID:  vaultID,
		VaultSeq: 42,
	})
	if ref.GetVaultSeq() != 42 {
		t.Fatalf("vault_seq = %d, want 42", ref.GetVaultSeq())
	}
	if len(ref.GetChunkId()) != 0 {
		t.Fatal("spool ref must not set chunk_id")
	}
}

func TestRecordToProtoRoundTripVaultSeq(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	ingesterID := glid.New()
	nodeID := glid.New()
	internal := chunk.Record{
		VaultID:  vaultID,
		VaultSeq: 7,
		Raw:      []byte("spool"),
		EventID: chunk.EventID{
			IngesterID: ingesterID,
			NodeID:     nodeID,
			IngestSeq:  1,
		},
	}
	proto := recordToProto(internal)
	if proto.GetRef().GetVaultSeq() != 7 {
		t.Fatalf("proto vault_seq = %d, want 7", proto.GetRef().GetVaultSeq())
	}
	back := protoToChunkRecord(proto)
	if back.VaultSeq != 7 || back.VaultID != vaultID {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestExportToRecordSpoolRef(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	proto := exportToRecord(&apiv1.ExportRecord{
		VaultId:  vaultID.ToProto(),
		VaultSeq: 11,
		Raw:      []byte("x"),
	})
	if proto.GetRef().GetVaultSeq() != 11 {
		t.Fatalf("vault_seq = %d, want 11", proto.GetRef().GetVaultSeq())
	}
	if len(proto.GetRef().GetChunkId()) != 0 {
		t.Fatal("expected spool-only ref")
	}
}
