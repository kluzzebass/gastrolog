package raftlevel

import (
	"context"
	"log/slog"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

// RaiseTarget returns the level the cluster's active level may move to: the
// lowest level any member of the cluster-ctl Raft configuration reports. It
// reports false while any member has no reported level, or when that level
// is not above active. members are Raft server IDs.
func RaiseTarget(active uint32, members []string, nodes []system.NodeConfig) (uint32, bool) {
	if len(members) == 0 {
		return 0, false
	}
	reported := make(map[glid.GLID]uint32, len(nodes))
	for _, n := range nodes {
		reported[n.ID] = n.ProtocolLevel
	}
	lowest := uint32(0)
	for i, m := range members {
		id, err := glid.ParseAny(m)
		if err != nil {
			return 0, false
		}
		level := reported[id]
		if level == 0 {
			return 0, false
		}
		if i == 0 || level < lowest {
			lowest = level
		}
	}
	if lowest <= active {
		return 0, false
	}
	return lowest, true
}

// LevelStore is the cluster-ctl state a Raiser reads and writes.
type LevelStore interface {
	ListNodes(ctx context.Context) ([]system.NodeConfig, error)
	GetProtocolLevel(ctx context.Context) (uint32, error)
	SetProtocolLevel(ctx context.Context, level uint32) error
}

// Raiser raises the cluster's active level when this node leads cluster-ctl.
// It runs on every node and acts only on the leader, on each Wake and on
// each leadership change.
type Raiser struct {
	store    LevelStore
	isLeader func() bool
	members  func() ([]string, error)
	logger   *slog.Logger
	wake     chan struct{}
}

// NewRaiser returns a Raiser. members lists the Raft server IDs in the
// cluster-ctl configuration, voters and learners alike: a learner applies the
// log too.
func NewRaiser(store LevelStore, isLeader func() bool, members func() ([]string, error), logger *slog.Logger) *Raiser {
	return &Raiser{
		store:    store,
		isLeader: isLeader,
		members:  members,
		logger:   logger,
		wake:     make(chan struct{}, 1),
	}
}

// Wake asks for an evaluation. It never blocks, so the cluster-ctl FSM can
// call it from inside Apply when a node record changes.
func (r *Raiser) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run evaluates on every Wake and leadership change until ctx ends.
func (r *Raiser) Run(ctx context.Context, leaderChanges <-chan hraft.Observation) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-leaderChanges:
		case <-r.wake:
		}
		if !r.isLeader() {
			continue
		}
		if err := r.Raise(ctx); err != nil {
			r.logger.Warn("raise protocol level", "error", err)
		}
	}
}

// Raise commits the raise RaiseTarget allows, if any.
func (r *Raiser) Raise(ctx context.Context) error {
	members, err := r.members()
	if err != nil {
		return err
	}
	nodes, err := r.store.ListNodes(ctx)
	if err != nil {
		return err
	}
	active, err := r.store.GetProtocolLevel(ctx)
	if err != nil {
		return err
	}
	target, ok := RaiseTarget(active, members, nodes)
	if !ok {
		return nil
	}
	if err := r.store.SetProtocolLevel(ctx, target); err != nil {
		return err
	}
	r.logger.Info("raised the cluster's active protocol level", "from", active, "to", target)
	return nil
}
