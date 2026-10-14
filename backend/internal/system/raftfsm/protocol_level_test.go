package raftfsm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/raftlevel"
	"gastrolog/internal/system"
	"gastrolog/internal/system/command"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type stop struct {
	group  string
	index  uint64
	reason error
}

// stops records the FSM's unapplicable-hook calls in place of stopping the
// process.
type stops struct {
	mu    sync.Mutex
	calls []stop
}

func (s *stops) hook(group string, index uint64, reason error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, stop{group, index, reason})
}

func (s *stops) all() []stop {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stop(nil), s.calls...)
}

func mustMarshal(t *testing.T, cmd *gastrologv1.SystemCommand) []byte {
	t.Helper()
	data, err := command.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// unknownCommandType is a SystemCommand whose oneof case is a field number
// this binary does not know — what a newer binary's command type is to an
// older one.
func unknownCommandType() []byte {
	b := protowire.AppendTag(nil, 999, protowire.BytesType)
	return protowire.AppendBytes(b, []byte{0x08, 0x01})
}

func loadState(t *testing.T, f *FSM) *system.System {
	t.Helper()
	sys, err := f.Store().Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return sys
}

func TestUnapplicableEntryStopsWithoutApplying(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"unknown command type", unknownCommandType(), raftlevel.ErrUnknownCommand},
		{"undecodable entry", []byte("not a protobuf"), raftlevel.ErrUndecodableCommand},
		{"entry without a command", nil, raftlevel.ErrUnknownCommand},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var rec stops
			var notified []Notification
			f := New(WithOnUnapplicable(rec.hook), WithOnApply(func(n Notification) { notified = append(notified, n) }))
			if res := f.Apply(&raft.Log{Index: 1, Data: mustMarshal(t, command.NewPutNodeConfig(system.NodeConfig{ID: newID(), Name: "n1"}))}); res != nil {
				t.Fatalf("apply node config: %v", res)
			}
			before := loadState(t, f)
			notified = nil

			res := f.Apply(&raft.Log{Index: 2, Data: tc.data})

			calls := rec.all()
			if len(calls) != 1 {
				t.Fatalf("unapplicable hook called %d times, want 1", len(calls))
			}
			if calls[0].group != "cluster-ctl" || calls[0].index != 2 || !errors.Is(calls[0].reason, tc.want) {
				t.Fatalf("hook got (%q, %d, %v), want (cluster-ctl, 2, %v)", calls[0].group, calls[0].index, calls[0].reason, tc.want)
			}
			if err, ok := res.(error); !ok || !errors.Is(err, tc.want) {
				t.Fatalf("Apply result = %v, want %v", res, tc.want)
			}
			if got := f.ApplyWait().Applied(); got != 1 {
				t.Fatalf("applied index = %d after the unapplicable entry, want 1", got)
			}
			if after := loadState(t, f); !reflect.DeepEqual(before, after) {
				t.Fatalf("state changed: before %+v, after %+v", before, after)
			}
			if len(notified) != 0 {
				t.Fatalf("notifications fired for an unapplicable entry: %v", notified)
			}
		})
	}
}

// TestEveryCommandCaseIsHandled applies every SystemCommand oneof case with
// an empty payload. A command may refuse an empty payload; none may be a
// type the FSM does not handle.
func TestEveryCommandCaseIsHandled(t *testing.T) {
	t.Parallel()
	oneof := (&gastrologv1.SystemCommand{}).ProtoReflect().Descriptor().Oneofs().ByName("command")
	fields := oneof.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		var rec stops
		f := New(WithOnUnapplicable(rec.hook))
		m := (&gastrologv1.SystemCommand{}).ProtoReflect()
		m.Set(fd, protoreflect.ValueOfMessage(m.NewField(fd).Message()))
		data, err := proto.Marshal(m.Interface())
		if err != nil {
			t.Fatal(err)
		}
		f.Apply(&raft.Log{Index: 1, Data: data})
		if calls := rec.all(); len(calls) != 0 {
			t.Errorf("%s: FSM does not handle it: %v", fd.Name(), calls[0].reason)
		}
	}
}

func TestSetProtocolLevelNeverLowers(t *testing.T) {
	t.Parallel()
	f := New(WithBinaryLevel(3))
	apply := func(index uint64, level uint32) any {
		return f.Apply(&raft.Log{Index: index, Data: mustMarshal(t, command.NewSetProtocolLevel(level))})
	}
	if res := apply(1, 2); res != nil {
		t.Fatalf("raise to 2: %v", res)
	}
	res := apply(2, 1)
	if err, ok := res.(error); !ok || !errors.Is(err, system.ErrProtocolLevelLowered) {
		t.Fatalf("lower to 1: result %v, want ErrProtocolLevelLowered", res)
	}
	if got := f.ActiveProtocolLevel(); got != 2 {
		t.Fatalf("active level = %d after a refused lowering, want 2", got)
	}
	if got := f.ApplyWait().Applied(); got != 2 {
		t.Fatalf("applied index = %d, want 2: a refused command is still applied", got)
	}
	if res := apply(3, 2); res != nil {
		t.Fatalf("re-commit 2: %v", res)
	}
	if res := apply(4, 3); res != nil {
		t.Fatalf("raise to 3: %v", res)
	}
	if got := f.ActiveProtocolLevel(); got != 3 {
		t.Fatalf("active level = %d, want 3", got)
	}
}

func TestBinaryBelowActiveLevelStopsOnApply(t *testing.T) {
	t.Parallel()
	var rec stops
	f := New(WithBinaryLevel(1), WithOnUnapplicable(rec.hook))
	f.Apply(&raft.Log{Index: 1, Data: mustMarshal(t, command.NewSetProtocolLevel(1))})

	res := f.Apply(&raft.Log{Index: 2, Data: mustMarshal(t, command.NewSetProtocolLevel(2))})

	calls := rec.all()
	if len(calls) != 1 || calls[0].index != 2 || !errors.Is(calls[0].reason, raftlevel.ErrBinaryBelowActiveLevel) {
		t.Fatalf("hook calls = %v, want one ErrBinaryBelowActiveLevel at index 2", calls)
	}
	if msg := calls[0].reason.Error(); !strings.Contains(msg, "level is 1") || !strings.Contains(msg, "raised to 2") {
		t.Fatalf("reason does not name both levels: %s", msg)
	}
	if _, ok := res.(error); !ok {
		t.Fatalf("Apply result = %v, want an error", res)
	}
	if got := f.ActiveProtocolLevel(); got != 1 {
		t.Fatalf("active level = %d, want 1: the raise must not apply", got)
	}
	if got := f.ApplyWait().Applied(); got != 1 {
		t.Fatalf("applied index = %d, want 1", got)
	}
}

func snapshotBytes(t *testing.T, f *FSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&bufSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestBinaryBelowActiveLevelStopsOnRestore(t *testing.T) {
	t.Parallel()
	src := New(WithBinaryLevel(2))
	nodeID := newID()
	src.Apply(&raft.Log{Index: 1, Data: mustMarshal(t, command.NewPutNodeConfig(system.NodeConfig{ID: nodeID, Name: "n", ProtocolLevel: 2}))})
	src.Apply(&raft.Log{Index: 2, Data: mustMarshal(t, command.NewSetProtocolLevel(2))})
	data := snapshotBytes(t, src)

	var rec stops
	old := New(WithBinaryLevel(1), WithOnUnapplicable(rec.hook))
	err := old.Restore(io.NopCloser(bytes.NewReader(data)))
	if !errors.Is(err, raftlevel.ErrBinaryBelowActiveLevel) {
		t.Fatalf("Restore = %v, want ErrBinaryBelowActiveLevel", err)
	}
	calls := rec.all()
	if len(calls) != 1 || calls[0].index != 2 || !errors.Is(calls[0].reason, raftlevel.ErrBinaryBelowActiveLevel) {
		t.Fatalf("hook calls = %v, want one ErrBinaryBelowActiveLevel at the snapshot's index 2", calls)
	}
	if sys := loadState(t, old); sys != nil {
		t.Fatalf("the snapshot was restored: %+v", sys)
	}
	if got := old.ApplyWait().Applied(); got != 0 {
		t.Fatalf("applied index = %d, want 0", got)
	}

	same := New(WithBinaryLevel(2))
	if err := same.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("restore into a binary at the active level: %v", err)
	}
	if got := same.ActiveProtocolLevel(); got != 2 {
		t.Fatalf("restored active level = %d, want 2", got)
	}
	node, err := same.Store().GetNode(context.Background(), nodeID)
	if err != nil || node == nil || node.ProtocolLevel != 2 {
		t.Fatalf("restored node record = %+v (%v), want protocol level 2", node, err)
	}
}

// TestStoreReadsAreSafeDuringRestore drives the readers the proposer gate
// and the cluster-ctl store use — Store and ActiveProtocolLevel, called from
// any goroutine — while Restore replaces the store on the apply goroutine.
// Under -race a non-atomic swap is reported.
func TestStoreReadsAreSafeDuringRestore(t *testing.T) {
	f := New()
	data := snapshotBytes(t, New())

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = f.ActiveProtocolLevel()
				_ = f.Store()
			}
		}
	}()
	for range 50 {
		if err := f.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	}
	close(stop)
	<-done
}
