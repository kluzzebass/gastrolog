package vaultraft

import (
	"errors"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/raftlevel"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type stop struct {
	group  string
	index  uint64
	reason error
}

func recordStops(stops *[]stop) Option {
	return WithOnUnapplicable(func(group string, index uint64, reason error) {
		*stops = append(*stops, stop{group, index, reason})
	})
}

// unknownField encodes a message whose only field is a field number no
// binary knows: a newer binary's oneof case, as an older binary decodes it.
func unknownField() []byte {
	b := protowire.AppendTag(nil, 999, protowire.BytesType)
	return protowire.AppendBytes(b, []byte{0x08, 0x01})
}

func scopedWithInnerBytes(inner []byte) []byte {
	scoped := protowire.AppendTag(nil, 1, protowire.BytesType)
	scoped = protowire.AppendBytes(scoped, make([]byte, 16))
	if inner != nil {
		scoped = protowire.AppendTag(scoped, 2, protowire.BytesType)
		scoped = protowire.AppendBytes(scoped, inner)
	}
	b := protowire.AppendTag(nil, 2, protowire.BytesType)
	return protowire.AppendBytes(b, scoped)
}

func TestUnapplicableEntryStopsWithoutApplying(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"undecodable entry", []byte{0xFF}, raftlevel.ErrUndecodableCommand},
		{"unknown envelope command", unknownField(), raftlevel.ErrUnknownCommand},
		{"unknown vault-scoped command", scopedWithInnerBytes(unknownField()), raftlevel.ErrUnknownCommand},
		{"vault-scoped command without a body", scopedWithInnerBytes(nil), raftlevel.ErrUnknownCommand},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stops []stop
			f := NewFSM(WithGroup("vault-ctl/test"), recordStops(&stops))
			if got := f.Apply(&hraft.Log{Index: 1, Data: MarshalNoop()}); got != nil {
				t.Fatalf("noop: %v", got)
			}

			res := f.Apply(&hraft.Log{Index: 2, Data: tc.data})

			if len(stops) != 1 {
				t.Fatalf("unapplicable hook called %d times, want 1", len(stops))
			}
			if stops[0].group != "vault-ctl/test" || stops[0].index != 2 || !errors.Is(stops[0].reason, tc.want) {
				t.Fatalf("hook got (%q, %d, %v), want (vault-ctl/test, 2, %v)", stops[0].group, stops[0].index, stops[0].reason, tc.want)
			}
			if err, ok := res.(error); !ok || !errors.Is(err, tc.want) {
				t.Fatalf("Apply result = %v, want %v", res, tc.want)
			}
			if got := f.ApplyWait().Applied(); got != 1 {
				t.Fatalf("applied index = %d after the unapplicable entry, want 1", got)
			}
			if vaults := f.Vaults(); len(vaults) != 0 {
				t.Fatalf("the unapplicable entry created vault sub-FSMs: %v", vaults)
			}
		})
	}
}

// TestEveryVaultCommandCaseIsHandled applies every vault-ctl envelope case
// and every vault chunk FSM command case with an empty payload. A command may
// refuse an empty payload; none may be a type the FSM does not handle.
func TestEveryVaultCommandCaseIsHandled(t *testing.T) {
	t.Parallel()
	each := func(msg protoreflect.Message, fn func(protoreflect.Message)) {
		fields := msg.Descriptor().Oneofs().ByName("command").Fields()
		for i := range fields.Len() {
			fd := fields.Get(i)
			m := msg.Type().New()
			m.Set(fd, protoreflect.ValueOfMessage(m.NewField(fd).Message()))
			fn(m)
		}
	}
	apply := func(name string, cmd proto.Message) {
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		var stops []stop
		NewFSM(recordStops(&stops)).Apply(&hraft.Log{Index: 1, Data: data})
		if len(stops) != 0 {
			t.Errorf("%s: FSM does not handle it: %v", name, stops[0].reason)
		}
	}
	each((&gastrologv1.VaultRaftCommand{}).ProtoReflect(), func(m protoreflect.Message) {
		if m.Interface().(*gastrologv1.VaultRaftCommand).GetVaultScoped() != nil {
			return
		}
		apply(string(m.WhichOneof(m.Descriptor().Oneofs().ByName("command")).Name()), m.Interface())
	})
	each((&gastrologv1.VaultCtlCommand{}).ProtoReflect(), func(m protoreflect.Message) {
		inner := m.Interface().(*gastrologv1.VaultCtlCommand)
		apply(string(m.WhichOneof(m.Descriptor().Oneofs().ByName("command")).Name()),
			&gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_VaultScoped{
				VaultScoped: &gastrologv1.VaultScopedCommand{VaultId: make([]byte, 16), Command: inner},
			}})
	})
}

// TestMissingVaultIDIsARefusal pins that a malformed but known command is a
// refusal every replica reaches alike, not a stop.
func TestMissingVaultIDIsARefusal(t *testing.T) {
	t.Parallel()
	var stops []stop
	f := NewFSM(recordStops(&stops))
	data, err := proto.Marshal(&gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_VaultScoped{
		VaultScoped: &gastrologv1.VaultScopedCommand{Command: &gastrologv1.VaultCtlCommand{
			Command: &gastrologv1.VaultCtlCommand_SealChunk{SealChunk: &gastrologv1.SealChunkCommand{}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res, ok := f.Apply(&hraft.Log{Index: 1, Data: data}).(error); !ok || res == nil {
		t.Fatalf("Apply result = %v, want a refusal", res)
	}
	if len(stops) != 0 {
		t.Fatalf("a refusal stopped the FSM: %v", stops)
	}
	if got := f.ApplyWait().Applied(); got != 1 {
		t.Fatalf("applied index = %d, want 1", got)
	}
}
