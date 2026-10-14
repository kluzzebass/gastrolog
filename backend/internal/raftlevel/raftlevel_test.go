package raftlevel

import (
	"errors"
	"strings"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestDefaultTableCoversEveryCommandCase(t *testing.T) {
	t.Parallel()
	table := DefaultTable()
	known := map[protoreflect.FullName]bool{}
	for _, msg := range CommandMessages {
		oneof := msg.Oneofs().ByName("command")
		if oneof == nil {
			t.Fatalf("%s has no oneof named command", msg.FullName())
		}
		fields := oneof.Fields()
		for i := range fields.Len() {
			name := fields.Get(i).FullName()
			known[name] = true
			level, ok := table[name]
			if !ok {
				t.Errorf("%s has no protocol level in the table", name)
				continue
			}
			if level < Base || level > Binary {
				t.Errorf("%s is at level %d, outside [%d, %d]", name, level, Base, Binary)
			}
		}
	}
	for name := range table {
		if !known[name] {
			t.Errorf("table entry %s is not a command case", name)
		}
	}
}

func TestCaseRejectsANameThatIsNotACommandCase(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("Case accepted a field that is not a oneof case")
		}
	}()
	Case(systemCommand, "no_such_command")
}

func marshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// unknownCase is the encoding of a command whose oneof case is a field
// number no binary knows, as a newer binary's command decodes in an older
// one.
func unknownCase() []byte {
	b := protowire.AppendTag(nil, 999, protowire.BytesType)
	return protowire.AppendBytes(b, nil)
}

func putSetting() *gastrologv1.SystemCommand {
	return &gastrologv1.SystemCommand{Command: &gastrologv1.SystemCommand_PutSetting{PutSetting: &gastrologv1.PutSettingCommand{}}}
}

func vaultScoped(inner *gastrologv1.VaultCtlCommand) *gastrologv1.VaultRaftCommand {
	return &gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_VaultScoped{
		VaultScoped: &gastrologv1.VaultScopedCommand{VaultId: make([]byte, 16), Command: inner},
	}}
}

func sealChunk() *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_SealChunk{SealChunk: &gastrologv1.SealChunkCommand{}}}
}

func activeAt(level uint32) func() uint32 { return func() uint32 { return level } }

func TestGateRefusesCommandsAboveTheActiveLevel(t *testing.T) {
	t.Parallel()
	table := DefaultTable().
		With(Case(systemCommand, "put_setting"), 2).
		With(Case(vaultCtlCommand, "seal_chunk"), 2).
		With(Case(vaultRaftCommand, "noop"), 2)

	checks := []struct {
		name  string
		check func(*Gate) error
	}{
		{"cluster-ctl command", func(g *Gate) error { return g.CheckSystemCommand(marshal(t, putSetting())) }},
		{"vault-scoped inner command", func(g *Gate) error { return g.CheckVaultRaftCommand(marshal(t, vaultScoped(sealChunk()))) }},
		{"vault-ctl envelope", func(g *Gate) error {
			return g.CheckVaultRaftCommand(marshal(t, &gastrologv1.VaultRaftCommand{Command: &gastrologv1.VaultRaftCommand_Noop{Noop: &gastrologv1.NoopCommand{}}}))
		}},
		{"vault chunk command", func(g *Gate) error { return g.CheckVaultCtlCommand(marshal(t, sealChunk())) }},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := c.check(NewGate(activeAt(1)).WithTable(table))
			if !errors.Is(err, ErrCommandAboveClusterLevel) {
				t.Fatalf("active level 1: err = %v, want ErrCommandAboveClusterLevel", err)
			}
			if !strings.Contains(err.Error(), "needs protocol level 2") || !strings.Contains(err.Error(), "active level is 1") {
				t.Fatalf("refusal does not name both levels: %v", err)
			}
			if err := c.check(NewGate(activeAt(2)).WithTable(table)); err != nil {
				t.Fatalf("active level 2: %v", err)
			}
		})
	}
}

func TestGateAdmitsEveryKnownCommandAtBase(t *testing.T) {
	t.Parallel()
	var nilGate *Gate
	for _, g := range []*Gate{nilGate, NewGate(nil), NewGate(activeAt(0))} {
		if got := g.ActiveLevel(); got != Base {
			t.Fatalf("ActiveLevel() = %d, want Base %d", got, Base)
		}
		if err := g.CheckSystemCommand(marshal(t, putSetting())); err != nil {
			t.Fatalf("put_setting at Base: %v", err)
		}
		if err := g.CheckVaultRaftCommand(marshal(t, vaultScoped(sealChunk()))); err != nil {
			t.Fatalf("seal_chunk at Base: %v", err)
		}
	}
}

func TestGateRefusesWhatThisBinaryCannotDecode(t *testing.T) {
	t.Parallel()
	g := NewGate(activeAt(Binary))
	garbage := []byte{0xff, 0xff, 0xff}
	unknownInner := append(
		protowire.AppendTag(nil, 1, protowire.BytesType), // VaultScopedCommand.vault_id
		0)
	unknownInner = protowire.AppendTag(unknownInner, 2, protowire.BytesType) // VaultScopedCommand.command
	unknownInner = protowire.AppendBytes(unknownInner, unknownCase())
	scopedUnknown := protowire.AppendTag(nil, 2, protowire.BytesType) // VaultRaftCommand.vault_scoped
	scopedUnknown = protowire.AppendBytes(scopedUnknown, unknownInner)

	cases := []struct {
		name  string
		check func() error
		want  error
	}{
		{"cluster-ctl garbage", func() error { return g.CheckSystemCommand(garbage) }, ErrUndecodableCommand},
		{"cluster-ctl unknown case", func() error { return g.CheckSystemCommand(unknownCase()) }, ErrUnknownCommand},
		{"cluster-ctl empty", func() error { return g.CheckSystemCommand(nil) }, ErrUnknownCommand},
		{"vault-ctl garbage", func() error { return g.CheckVaultRaftCommand(garbage) }, ErrUndecodableCommand},
		{"vault-ctl unknown envelope", func() error { return g.CheckVaultRaftCommand(unknownCase()) }, ErrUnknownCommand},
		{"vault-ctl unknown inner", func() error { return g.CheckVaultRaftCommand(scopedUnknown) }, ErrUnknownCommand},
		{"vault-ctl missing inner", func() error { return g.CheckVaultRaftCommand(marshal(t, vaultScoped(nil))) }, ErrUnknownCommand},
		{"vault chunk garbage", func() error { return g.CheckVaultCtlCommand(garbage) }, ErrUndecodableCommand},
		{"vault chunk unknown case", func() error { return g.CheckVaultCtlCommand(unknownCase()) }, ErrUnknownCommand},
	}
	for _, c := range cases {
		err := c.check()
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
		if !IsRefusal(err) {
			t.Errorf("%s: IsRefusal(%v) = false", c.name, err)
		}
	}
}

func TestUnknownCommandNamesWhatItCannotApply(t *testing.T) {
	t.Parallel()
	if err := UnknownCommand(putSetting()); !errors.Is(err, ErrUnknownCommand) || !strings.Contains(err.Error(), "put_setting") {
		t.Fatalf("set case: %v", err)
	}
	var cmd gastrologv1.SystemCommand
	if err := proto.Unmarshal(unknownCase(), &cmd); err != nil {
		t.Fatal(err)
	}
	if err := UnknownCommand(&cmd); !errors.Is(err, ErrUnknownCommand) || !strings.Contains(err.Error(), "does not know") {
		t.Fatalf("unknown case: %v", err)
	}
}
