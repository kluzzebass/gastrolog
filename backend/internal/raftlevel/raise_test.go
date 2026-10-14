package raftlevel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

func TestRaiseTarget(t *testing.T) {
	t.Parallel()
	a, b, c, outsider := glid.New(), glid.New(), glid.New(), glid.New()
	members := []string{a.String(), b.String(), c.String()}
	nodes := func(levels ...uint32) []system.NodeConfig {
		ids := []glid.GLID{a, b, c}
		out := make([]system.NodeConfig, 0, len(levels))
		for i, l := range levels {
			out = append(out, system.NodeConfig{ID: ids[i], ProtocolLevel: l})
		}
		return out
	}
	cases := []struct {
		name    string
		active  uint32
		members []string
		nodes   []system.NodeConfig
		want    uint32
		raise   bool
	}{
		{"every member reports the next level", 1, members, nodes(2, 2, 2), 2, true},
		{"the lowest report wins", 1, members, nodes(3, 2, 4), 2, true},
		{"from no committed level", 0, members, nodes(1, 1, 1), 1, true},
		{"a member without a record", 1, members, nodes(2, 2), 0, false},
		{"a member that has not reported", 1, members, nodes(2, 0, 2), 0, false},
		{"one member still on the active level", 1, members, nodes(2, 1, 2), 0, false},
		{"never below the active level", 3, members, nodes(2, 2, 2), 0, false},
		{"no members", 1, nil, nodes(2, 2, 2), 0, false},
		{"a member ID that is not a node ID", 1, []string{a.String(), "not-a-node"}, nodes(2, 2, 2), 0, false},
		{"records of non-members are ignored", 1, members[:2],
			append(nodes(2, 2), system.NodeConfig{ID: outsider, ProtocolLevel: 1}), 2, true},
	}
	for _, tc := range cases {
		got, ok := RaiseTarget(tc.active, tc.members, tc.nodes)
		if got != tc.want || ok != tc.raise {
			t.Errorf("%s: RaiseTarget = (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.raise)
		}
	}
}

// levelStore is a LevelStore holding the active level the way the cluster-ctl
// store does: it refuses to lower it.
type levelStore struct {
	mu     sync.Mutex
	nodes  []system.NodeConfig
	active uint32
	sets   []uint32
	setCh  chan uint32
}

func (s *levelStore) ListNodes(context.Context) ([]system.NodeConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]system.NodeConfig(nil), s.nodes...), nil
}

func (s *levelStore) GetProtocolLevel(context.Context) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, nil
}

func (s *levelStore) SetProtocolLevel(_ context.Context, level uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if level < s.active {
		return fmt.Errorf("%w: %d below %d", system.ErrProtocolLevelLowered, level, s.active)
	}
	s.active = level
	s.sets = append(s.sets, level)
	if s.setCh != nil {
		s.setCh <- level
	}
	return nil
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRaiserRaisesOnceEveryMemberReports(t *testing.T) {
	t.Parallel()
	a, b := glid.New(), glid.New()
	members := func() ([]string, error) { return []string{a.String(), b.String()}, nil }
	store := &levelStore{active: 1, nodes: []system.NodeConfig{{ID: a, ProtocolLevel: 2}, {ID: b, ProtocolLevel: 1}}}
	r := NewRaiser(store, func() bool { return true }, members, discard())

	if err := r.Raise(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.sets) != 0 {
		t.Fatalf("raised to %v while a member reports level 1", store.sets)
	}

	store.nodes[1].ProtocolLevel = 2
	if err := r.Raise(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.active != 2 {
		t.Fatalf("active level = %d after every member reports 2, want 2", store.active)
	}

	// A member falling back to a lower report never lowers the level.
	store.nodes[1].ProtocolLevel = 1
	if err := r.Raise(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.active != 2 || len(store.sets) != 1 {
		t.Fatalf("active level = %d, sets = %v; want 2 with one raise", store.active, store.sets)
	}
}

func TestRaiserActsOnlyAsLeader(t *testing.T) {
	t.Parallel()
	a := glid.New()
	store := &levelStore{active: 1, nodes: []system.NodeConfig{{ID: a, ProtocolLevel: 2}}, setCh: make(chan uint32, 1)}
	var leading atomic.Bool
	asked := make(chan struct{})
	r := NewRaiser(store, func() bool {
		asked <- struct{}{}
		return leading.Load()
	}, func() ([]string, error) { return []string{a.String()}, nil }, discard())

	leaderCh := make(chan hraft.Observation, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx, leaderCh); close(done) }()

	// Run evaluates one event at a time, so the second leadership question
	// is asked only after the first evaluation has finished, raise included.
	r.Wake()
	<-asked
	r.Wake()
	<-asked
	store.mu.Lock()
	sets := slices.Clone(store.sets)
	store.mu.Unlock()
	if len(sets) != 0 {
		t.Fatalf("a follower raised the level to %v", sets)
	}

	leading.Store(true)
	leaderCh <- hraft.Observation{}
	<-asked
	select {
	case level := <-store.setCh:
		if level != 2 {
			t.Fatalf("raised to %d, want 2", level)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no raise after gaining leadership")
	}
	cancel()
	<-done
}

func TestRaiserReportsMembershipFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("no configuration")
	r := NewRaiser(&levelStore{}, func() bool { return true }, func() ([]string, error) { return nil, want }, discard())
	if err := r.Raise(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Raise = %v, want %v", err, want)
	}
}
