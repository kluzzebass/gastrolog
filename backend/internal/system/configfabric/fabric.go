// Package configfabric gives a multi-node test one config store per node,
// each backed by the production FSM and the production command encoding, with
// replication between them modelled explicitly instead of by Raft.
//
// A write on any node is appended to one shared log and applied to that node
// at once, as a leader applies its own entry. Every other node receives the
// entry immediately or on demand, depending on the fabric's mode, so a test
// can choose the staleness it wants to exercise: none, or exactly the entries
// it holds back.
package configfabric

import (
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"
)

// ErrStalled is what a stalled node's writes fail with. It stands for any
// reason a node cannot get an entry committed — no quorum, partitioned from
// the leader, forward timed out — all of which leave the node unable to
// bring its own view current.
var ErrStalled = errors.New("configfabric: node cannot reach the log")

// Mode is how entries reach nodes other than the writer.
type Mode int

const (
	// Immediate applies every entry to every node before the write returns:
	// the cluster converges instantly, which is what most tests want.
	Immediate Mode = iota
	// Manual holds entries for the other nodes until Deliver or DeliverAll,
	// so a test can observe and act on a stale node.
	Manual
)

// Fabric is the shared log and the nodes attached to it.
type Fabric struct {
	mu    sync.Mutex
	mode  Mode
	log   []raft.Log
	nodes []*Node
}

// Node is one member's FSM, its store, and how far through the log it is.
type Node struct {
	fabric  *Fabric
	index   int
	fsm     *raftfsm.FSM
	store   *raftstore.Store
	applied uint64 // index of the last log entry applied here
	stalled bool
}

// New builds a fabric of n nodes in Immediate mode.
func New(n int, fsmOpts ...raftfsm.Option) *Fabric {
	f := &Fabric{}
	for i := range n {
		fsm := raftfsm.New(fsmOpts...)
		node := &Node{fabric: f, index: i, fsm: fsm}
		node.store = raftstore.New(node, fsm, 10*time.Second)
		f.nodes = append(f.nodes, node)
	}
	return f
}

// SetMode changes how later writes reach the other nodes. Switching to
// Immediate does not deliver entries already held back; call DeliverAll.
func (f *Fabric) SetMode(m Mode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = m
}

// Node returns the i-th node.
func (f *Fabric) Node(i int) *Node { return f.nodes[i] }

// Len returns the number of nodes.
func (f *Fabric) Len() int { return len(f.nodes) }

// Deliver applies every entry node i has not yet applied.
func (f *Fabric) Deliver(i int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[i].applyUpTo(uint64(len(f.log)))
}

// DeliverAll brings every node up to the end of the log.
func (f *Fabric) DeliverAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.nodes {
		n.applyUpTo(uint64(len(f.log)))
	}
}

// Lag returns how many committed entries node i has not applied.
func (f *Fabric) Lag(i int) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.log)) - f.nodes[i].applied
}

// Stall makes node i's writes fail, so a test can exercise a node that
// cannot bring its view current. Entries already held for it still arrive
// through Deliver.
func (f *Fabric) Stall(i int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[i].stalled = true
}

// Resume undoes Stall.
func (f *Fabric) Resume(i int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[i].stalled = false
}

// Store is the node's config store: reads serve its own FSM, writes go
// through the fabric.
func (n *Node) Store() system.Store { return n.store }

// Apply implements raftstore.Applier: append the command, apply it here, and
// replicate per the fabric's mode.
func (n *Node) Apply(cmd []byte, _ time.Duration) raft.ApplyFuture {
	f := n.fabric
	f.mu.Lock()
	defer f.mu.Unlock()
	if n.stalled {
		return &future{err: ErrStalled}
	}
	index := uint64(len(f.log)) + 1
	f.log = append(f.log, raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: cmd})
	resp := n.applyUpTo(index)
	if f.mode == Immediate {
		for _, other := range f.nodes {
			if other != n {
				other.applyUpTo(index)
			}
		}
	}
	return &future{index: index, resp: resp}
}

// applyUpTo applies log entries through index in order and returns the FSM's
// response to the entry at index (nil when it was applied earlier). The
// caller holds the fabric lock.
func (n *Node) applyUpTo(index uint64) any {
	var resp any
	for n.applied < index {
		entry := n.fabric.log[n.applied]
		r := n.fsm.Apply(&entry)
		n.applied++
		if n.applied == index {
			resp = r
		}
	}
	return resp
}

// future is an already-completed raft.ApplyFuture.
type future struct {
	index uint64
	resp  any
	err   error
}

func (f *future) Error() error  { return f.err }
func (f *future) Index() uint64 { return f.index }
func (f *future) Response() any { return f.resp }
