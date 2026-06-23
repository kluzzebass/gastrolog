package main

import (
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// chunkID returns a deterministic 16-byte chunk ID for tests.
func chunkID(b byte) (id [glid.Size]byte) {
	id[0] = b
	return id
}

// wrap wraps an inner vaultctlfsm command in the outer vault-scoped envelope
// exactly as the production proposers do, so the test exercises the real
// VaultRaftCommand bytes that land in the WAL.
func wrap(vaultID glid.GLID, inner []byte) []byte {
	return vaultraft.MarshalVaultChunkCommand(vaultID, inner)
}

// TestDecodeFSMCmd_AllCommandTypes verifies walinspect decodes every command
// type produced by the protobuf encoders (gastrolog-5lrg7 / gastrolog-62ywk).
func TestDecodeFSMCmd_AllCommandTypes(t *testing.T) {
	vaultID := glid.New()
	cid := chunkID(0x7A)
	cidStr := glid.FromBytes(cid[:]).String()
	now := time.Unix(0, 1_700_000_000_000_000_000)

	cases := []struct {
		name     string
		data     []byte
		wantName string
		wantID   string
	}{
		{"noop", vaultraft.MarshalNoop(), "OpNoop", ""},
		{"create", wrap(vaultID, vaultctlfsm.MarshalCreateChunk(cid, now, now, now)), "CmdCreateChunk", cidStr},
		{"begin_seal", wrap(vaultID, vaultctlfsm.MarshalBeginSeal(cid)), "CmdBeginSeal", cidStr},
		{"seal", wrap(vaultID, vaultctlfsm.MarshalSealChunk(cid, now, 1, 2, now, now, now, false, now)), "CmdSealChunk", cidStr},
		{"compress", wrap(vaultID, vaultctlfsm.MarshalCompressChunk(cid, 99)), "CmdCompressChunk", cidStr},
		{"attach_offsets", wrap(vaultID, vaultctlfsm.MarshalAttachOffsets(cid, 1, 2, 3, 4)), "CmdAttachOffsets", cidStr},
		{"upload", wrap(vaultID, vaultctlfsm.MarshalUploadChunk(cid, 1, 2, 3, 4, 5, [32]byte{}, glid.GLID{}, 0)), "CmdUploadChunk", cidStr},
		{"delete", wrap(vaultID, vaultctlfsm.MarshalDeleteChunk(cid)), "CmdDeleteChunk", cidStr},
		{"retention_pending", wrap(vaultID, vaultctlfsm.MarshalRetentionPending(cid)), "CmdRetentionPending", cidStr},
		{"request_delete", wrap(vaultID, vaultctlfsm.MarshalRequestDelete(cid, now, "ttl", []string{"n1"})), "CmdRequestDelete", cidStr},
		{"ack_delete", wrap(vaultID, vaultctlfsm.MarshalAckDelete(cid, "node-7")), "CmdAckDelete", cidStr},
		{"finalize_delete", wrap(vaultID, vaultctlfsm.MarshalFinalizeDelete(cid)), "CmdFinalizeDelete", cidStr},
		{"prune_node", wrap(vaultID, vaultctlfsm.MarshalPruneNode("node-9")), "CmdPruneNode", "node-9"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotName, gotID := decodeFSMCmd(tc.data, hraft.LogCommand)
			if gotName != tc.wantName {
				t.Errorf("name: got %q want %q", gotName, tc.wantName)
			}
			if gotID != tc.wantID {
				t.Errorf("id: got %q want %q", gotID, tc.wantID)
			}
		})
	}
}

// TestDecodeFSMCmd_RepatriateChunk covers the one command whose chunk ID lives
// in a nested ManifestEntry rather than a top-level id field.
func TestDecodeFSMCmd_RepatriateChunk(t *testing.T) {
	vaultID := glid.New()
	cid := chunkID(0x33)
	entry := vaultctlfsm.ManifestEntry{ID: cid}
	inner, err := vaultctlfsm.MarshalRepatriateChunk(entry)
	if err != nil {
		t.Fatal(err)
	}
	name, id := decodeFSMCmd(wrap(vaultID, inner), hraft.LogCommand)
	if name != "CmdRepatriateChunk" {
		t.Errorf("name: got %q", name)
	}
	if id != glid.FromBytes(cid[:]).String() {
		t.Errorf("id: got %q", id)
	}
}

// TestDecodeFSMCmd_NonCommandAndGarbage verifies non-command log types and
// undecodable payloads yield empty results instead of panicking.
func TestDecodeFSMCmd_NonCommandAndGarbage(t *testing.T) {
	if name, id := decodeFSMCmd(vaultraft.MarshalNoop(), hraft.LogConfiguration); name != "" || id != "" {
		t.Errorf("non-command log type: got (%q,%q)", name, id)
	}
	if name, id := decodeFSMCmd(nil, hraft.LogCommand); name != "" || id != "" {
		t.Errorf("empty data: got (%q,%q)", name, id)
	}
	// Field 1 declared as varint but truncated — not a valid VaultRaftCommand.
	if name, id := decodeFSMCmd([]byte{0x08}, hraft.LogCommand); name != "" || id != "" {
		t.Errorf("garbage data: got (%q,%q)", name, id)
	}
}
