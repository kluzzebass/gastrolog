package multiraft

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// testNode represents one node in a test cluster with a gRPC server,
// a Transport, and a bufconn listener.
type testNode struct {
	transport *Transport[string]
	server    *grpc.Server
	lis       *bufconn.Listener
}

// makeTestCluster creates n interconnected nodes using bufconn (in-memory gRPC).
func makeTestCluster(t *testing.T, n int) []*testNode {
	t.Helper()
	nodes := make([]*testNode, n)

	// Create listeners and servers first.
	for i := range n {
		lis := bufconn.Listen(bufSize)
		addr := raft.ServerAddress(lis.Addr().String())
		srv := grpc.NewServer()

		tp := New[string](addr, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			func(s string) []byte { return []byte(s) },
			func(b []byte) string { return string(b) },
		)
		tp.Register(srv)

		nodes[i] = &testNode{transport: tp, server: srv, lis: lis}
		go func() { _ = srv.Serve(lis) }()
	}

	// Override dial options so nodes dial each other via bufconn.
	dialers := make(map[string]func() (net.Conn, error))
	for _, node := range nodes {
		addr := string(node.transport.localAddress)
		l := node.lis
		dialers[addr] = func() (net.Conn, error) { return l.Dial() }
	}

	for _, node := range nodes {
		node.transport.dialOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(_ context.Context, addr string) (net.Conn, error) {
				d, ok := dialers[addr]
				if !ok {
					return nil, net.UnknownNetworkError("no dialer for " + addr)
				}
				return d()
			}),
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.server.Stop()
			_ = node.transport.Close()
		}
	})

	return nodes
}

// ---------- Tests ----------

func TestAppendEntriesRoundTrip(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	group := "test-group"
	tp0 := nodes[0].transport.GroupTransport(group)
	tp1 := nodes[1].transport.GroupTransport(group)

	// Consumer on node 1 responds to AppendEntries.
	go func() {
		for rpc := range tp1.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{
				RPCHeader: raft.RPCHeader{ProtocolVersion: 3},
				Term:      42,
				LastLog:   100,
				Success:   true,
			}, nil)
		}
	}()

	req := &raft.AppendEntriesRequest{
		RPCHeader:         raft.RPCHeader{ProtocolVersion: 3, ID: []byte("node-0"), Addr: []byte("addr-0")},
		Term:              42,
		Leader:            []byte("node-0"),
		PrevLogEntry:      10,
		PrevLogTerm:       41,
		LeaderCommitIndex: 9,
		Entries: []*raft.Log{
			{Index: 11, Term: 42, Type: raft.LogCommand, Data: []byte("hello")},
		},
	}
	var resp raft.AppendEntriesResponse

	err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress, req, &resp)
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if resp.Term != 42 {
		t.Errorf("Term: got %d, want 42", resp.Term)
	}
	if resp.LastLog != 100 {
		t.Errorf("LastLog: got %d, want 100", resp.LastLog)
	}
	if !resp.Success {
		t.Error("Success: got false, want true")
	}
}

func TestRequestVoteRoundTrip(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	group := "vote-group"
	tp0 := nodes[0].transport.GroupTransport(group)
	tp1 := nodes[1].transport.GroupTransport(group)

	go func() {
		for rpc := range tp1.Consumer() {
			rpc.Respond(&raft.RequestVoteResponse{
				RPCHeader: raft.RPCHeader{ProtocolVersion: 3},
				Term:      5,
				Granted:   true,
			}, nil)
		}
	}()

	req := &raft.RequestVoteRequest{
		RPCHeader:    raft.RPCHeader{ProtocolVersion: 3, ID: []byte("node-0"), Addr: []byte("addr-0")},
		Term:         5,
		Candidate:    []byte("node-0"),
		LastLogIndex: 50,
		LastLogTerm:  4,
	}
	var resp raft.RequestVoteResponse

	if err := tp0.RequestVote("node-1", nodes[1].transport.localAddress, req, &resp); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.Granted {
		t.Error("Granted: got false, want true")
	}
}

func TestInstallSnapshotRoundTrip(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	group := "snap-group"
	tp0 := nodes[0].transport.GroupTransport(group)
	tp1 := nodes[1].transport.GroupTransport(group)

	// Generate random snapshot data.
	snapData := make([]byte, 654321)
	if _, err := rand.Read(snapData); err != nil {
		t.Fatal(err)
	}

	var received []byte
	go func() {
		for rpc := range tp1.Consumer() {
			if rpc.Reader != nil {
				data, _ := io.ReadAll(rpc.Reader)
				received = data
			}
			rpc.Respond(&raft.InstallSnapshotResponse{
				RPCHeader: raft.RPCHeader{ProtocolVersion: 3},
				Term:      10,
				Success:   true,
			}, nil)
		}
	}()

	req := &raft.InstallSnapshotRequest{
		RPCHeader:    raft.RPCHeader{ProtocolVersion: 3, ID: []byte("node-0"), Addr: []byte("addr-0")},
		Term:         10,
		Leader:       []byte("node-0"),
		LastLogIndex: 200,
		LastLogTerm:  9,
		Size:         int64(len(snapData)),
	}
	var resp raft.InstallSnapshotResponse

	if err := tp0.InstallSnapshot("node-1", nodes[1].transport.localAddress, req, &resp, bytes.NewReader(snapData)); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if !resp.Success {
		t.Error("Success: got false, want true")
	}
	if !bytes.Equal(received, snapData) {
		t.Errorf("Snapshot data mismatch: got %d bytes, want %d", len(received), len(snapData))
	}
}

func TestMultipleGroupsIsolated(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	// Create two groups on both nodes.
	tpA0 := nodes[0].transport.GroupTransport("group-a")
	tpA1 := nodes[1].transport.GroupTransport("group-a")
	tpB0 := nodes[0].transport.GroupTransport("group-b")
	tpB1 := nodes[1].transport.GroupTransport("group-b")

	var muA, muB sync.Mutex
	countA, countB := 0, 0

	// Group A consumer on node 1.
	go func() {
		for rpc := range tpA1.Consumer() {
			muA.Lock()
			countA++
			muA.Unlock()
			rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
		}
	}()

	// Group B consumer on node 1.
	go func() {
		for rpc := range tpB1.Consumer() {
			muB.Lock()
			countB++
			muB.Unlock()
			rpc.Respond(&raft.AppendEntriesResponse{Term: 2, Success: true}, nil)
		}
	}()

	target := nodes[1].transport.localAddress

	// Send 5 RPCs to group A, 3 to group B.
	for range 5 {
		var resp raft.AppendEntriesResponse
		req := &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: 3}, Term: 1, Leader: []byte("l")}
		if err := tpA0.AppendEntries("node-1", target, req, &resp); err != nil {
			t.Fatalf("group-a AppendEntries: %v", err)
		}
		if resp.Term != 1 {
			t.Errorf("group-a Term: got %d, want 1", resp.Term)
		}
	}
	for range 3 {
		var resp raft.AppendEntriesResponse
		req := &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: 3}, Term: 2, Leader: []byte("l")}
		if err := tpB0.AppendEntries("node-1", target, req, &resp); err != nil {
			t.Fatalf("group-b AppendEntries: %v", err)
		}
		if resp.Term != 2 {
			t.Errorf("group-b Term: got %d, want 2", resp.Term)
		}
	}

	muA.Lock()
	gotA := countA
	muA.Unlock()
	muB.Lock()
	gotB := countB
	muB.Unlock()

	if gotA != 5 {
		t.Errorf("group-a received %d RPCs, want 5", gotA)
	}
	if gotB != 3 {
		t.Errorf("group-b received %d RPCs, want 3", gotB)
	}

	_ = tpA0
	_ = tpB0
}

func TestUnknownGroupReturnsError(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	// Only register group on node 0 (sender), not on node 1 (receiver).
	tp0 := nodes[0].transport.GroupTransport("exists-on-sender-only")

	req := &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: 3}, Term: 1, Leader: []byte("l")}
	var resp raft.AppendEntriesResponse

	err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress, req, &resp)
	if err == nil {
		t.Fatal("expected error for unknown group, got nil")
	}
}

func TestRemoveGroupClosesConsumer(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 1)

	tp := nodes[0].transport.GroupTransport("ephemeral")
	ch := tp.Consumer()

	nodes[0].transport.RemoveGroup("ephemeral")

	// Consumer channel should be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for channel close")
	}
}

func TestHeartbeatFastPath(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	group := "heartbeat-group"
	tp0 := nodes[0].transport.GroupTransport(group)
	tp1 := nodes[1].transport.GroupTransport(group)

	heartbeatReceived := make(chan struct{}, 1)
	tp1.(raft.Transport).SetHeartbeatHandler(func(rpc raft.RPC) {
		heartbeatReceived <- struct{}{}
		rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
	})

	// Also start a consumer to handle non-heartbeat RPCs (required by the transport).
	go func() {
		for rpc := range tp1.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 1, Success: true}, nil)
		}
	}()

	// Send a heartbeat (empty AppendEntries with Term + RPCHeader.Addr set).
	req := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{ProtocolVersion: 3, Addr: []byte("node-0")},
		Term:      1,
	}
	var resp raft.AppendEntriesResponse
	if err := tp0.AppendEntries("node-1", nodes[1].transport.localAddress, req, &resp); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	select {
	case <-heartbeatReceived:
		// OK — heartbeat went through fast path.
	case <-time.After(time.Second):
		t.Error("heartbeat was not delivered via fast path")
	}
}

func TestPipelineAppendEntries(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 2)

	group := "pipeline-group"
	tp0 := nodes[0].transport.GroupTransport(group)
	tp1 := nodes[1].transport.GroupTransport(group)

	go func() {
		for rpc := range tp1.Consumer() {
			req := rpc.Command.(*raft.AppendEntriesRequest)
			rpc.Respond(&raft.AppendEntriesResponse{
				Term:    req.Term,
				LastLog: req.PrevLogEntry + uint64(len(req.Entries)),
				Success: true,
			}, nil)
		}
	}()

	pipeline, err := tp0.AppendEntriesPipeline("node-1", nodes[1].transport.localAddress)
	if err != nil {
		t.Fatalf("open pipeline: %v", err)
	}
	defer func() { _ = pipeline.Close() }()

	const n = 10
	for i := range n {
		req := &raft.AppendEntriesRequest{
			RPCHeader:    raft.RPCHeader{ProtocolVersion: 3},
			Term:         1,
			Leader:       []byte("node-0"),
			PrevLogEntry: uint64(i),
			Entries:      []*raft.Log{{Index: uint64(i + 1), Term: 1, Type: raft.LogCommand, Data: []byte("x")}},
		}
		if _, err := pipeline.AppendEntries(req, nil); err != nil {
			t.Fatalf("pipeline send %d: %v", i, err)
		}
	}

	// Drain responses.
	for range n {
		select {
		case fut := <-pipeline.Consumer():
			if err := fut.Error(); err != nil {
				t.Fatalf("pipeline response error: %v", err)
			}
			if !fut.Response().Success {
				t.Error("pipeline response: got false, want true")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for pipeline response")
		}
	}
}

func TestThreeNodeThreeGroupsIndependentLeaders(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.
	nodes := makeTestCluster(t, 3)
	groups := []string{"config", "tier-1", "tier-2"}

	// Each group responds with a different term to prove isolation.
	for gi, group := range groups {
		expectedTerm := uint64(gi + 10)
		for ni := range 3 {
			tp := nodes[ni].transport.GroupTransport(group)
			term := expectedTerm
			go func() {
				for rpc := range tp.Consumer() {
					rpc.Respond(&raft.AppendEntriesResponse{Term: term, Success: true}, nil)
				}
			}()
		}
	}

	// Send one RPC per group from node 0 to node 1 and verify the term matches.
	for gi, group := range groups {
		expectedTerm := uint64(gi + 10)
		tp := nodes[0].transport.GroupTransport(group)
		req := &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: 3}, Term: 1, Leader: []byte("l")}
		var resp raft.AppendEntriesResponse
		if err := tp.AppendEntries("node-1", nodes[1].transport.localAddress, req, &resp); err != nil {
			t.Fatalf("group %s: %v", group, err)
		}
		if resp.Term != expectedTerm {
			t.Errorf("group %s: Term got %d, want %d", group, resp.Term, expectedTerm)
		}
	}
}

// ---------- Non-string group ID ----------

// tierID is a custom integer type used as a Raft group ID to prove
// the generic works with non-string types.
type tierID uint64

func encodeTierID(id tierID) []byte {
	b := make([]byte, 8)
	b[0] = byte(id >> 56)
	b[1] = byte(id >> 48)
	b[2] = byte(id >> 40)
	b[3] = byte(id >> 32)
	b[4] = byte(id >> 24)
	b[5] = byte(id >> 16)
	b[6] = byte(id >> 8)
	b[7] = byte(id)
	return b
}

func decodeTierID(b []byte) tierID {
	return tierID(uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]))
}

func TestNonStringGroupID(t *testing.T) {
	// Not parallel — gRPC servers + bufconn need clean sequential lifecycle.

	lis1 := bufconn.Listen(bufSize)
	lis2 := bufconn.Listen(bufSize)
	srv1 := grpc.NewServer()
	srv2 := grpc.NewServer()

	tp1 := New[tierID](raft.ServerAddress(lis1.Addr().String()),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		encodeTierID, decodeTierID,
	)
	tp2 := New[tierID](raft.ServerAddress(lis2.Addr().String()),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		encodeTierID, decodeTierID,
	)
	tp1.Register(srv1)
	tp2.Register(srv2)
	go func() { _ = srv1.Serve(lis1) }()
	go func() { _ = srv2.Serve(lis2) }()

	// Cross-connect via bufconn.
	dialers := map[string]func() (net.Conn, error){
		lis1.Addr().String(): func() (net.Conn, error) { return lis1.Dial() },
		lis2.Addr().String(): func() (net.Conn, error) { return lis2.Dial() },
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, addr string) (net.Conn, error) {
			return dialers[addr]()
		}),
	}
	tp1.SetDialOptions(dialOpts)
	tp2.SetDialOptions(dialOpts)

	t.Cleanup(func() {
		srv1.Stop()
		srv2.Stop()
		_ = tp1.Close()
		_ = tp2.Close()
	})

	// Use integer group IDs.
	var group1 tierID = 42
	var group2 tierID = 9999

	gt1a := tp1.GroupTransport(group1)
	gt1b := tp2.GroupTransport(group1)
	gt2a := tp1.GroupTransport(group2)
	gt2b := tp2.GroupTransport(group2)

	// Consumer on node 2 for both groups.
	go func() {
		for rpc := range gt1b.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 42, Success: true}, nil)
		}
	}()
	go func() {
		for rpc := range gt2b.Consumer() {
			rpc.Respond(&raft.AppendEntriesResponse{Term: 9999, Success: true}, nil)
		}
	}()

	// Send to group 42 — expect term 42 back.
	req := &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: 3}, Term: 1, Leader: []byte("l")}
	var resp1 raft.AppendEntriesResponse
	if err := gt1a.AppendEntries("n2", tp2.LocalAddr(), req, &resp1); err != nil {
		t.Fatalf("group 42: %v", err)
	}
	if resp1.Term != 42 {
		t.Errorf("group 42: Term got %d, want 42", resp1.Term)
	}

	// Send to group 9999 — expect term 9999 back.
	var resp2 raft.AppendEntriesResponse
	if err := gt2a.AppendEntries("n2", tp2.LocalAddr(), req, &resp2); err != nil {
		t.Fatalf("group 9999: %v", err)
	}
	if resp2.Term != 9999 {
		t.Errorf("group 9999: Term got %d, want 9999", resp2.Term)
	}
}
