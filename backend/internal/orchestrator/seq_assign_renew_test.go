package orchestrator

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestWaitSeqLeaseGrantRejectsStaleLocalFSM(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	holder := "node-follower"
	orch := newTestOrch(t, Config{LocalNodeID: holder})
	fsm := wireTestSeqAllocator(orch, vaultID)

	// Lagging local view: holder still has the fully consumed 1-256 swath.
	staleWire, err := vaultctlfsm.MarshalReserveSeqRange(holder, vaultctlfsm.InitialSeqEpoch, defaultSeqLeaseBatch)
	if err != nil {
		t.Fatal(err)
	}
	if result := fsm.Apply(&hraft.Log{Data: staleWire}); result == nil {
		t.Fatal("seed stale swath: nil result")
	} else if _, ok := result.(vaultctlfsm.SeqLeaseGrant); !ok {
		if err, ok := result.(error); ok {
			t.Fatalf("seed stale swath: %v", err)
		}
		t.Fatalf("seed stale swath: unexpected %T", result)
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		burnWire, err := vaultctlfsm.MarshalBurnSeqLeaseTail(holder, vaultctlfsm.InitialSeqEpoch, defaultSeqLeaseBatch)
		if err != nil {
			t.Error(err)
			return
		}
		if result := fsm.Apply(&hraft.Log{Data: burnWire}); result != nil {
			if err, ok := result.(error); ok {
				t.Errorf("burn stale swath: %v", err)
			}
			return
		}
		freshWire, err := vaultctlfsm.MarshalReserveSeqRange(holder, vaultctlfsm.InitialSeqEpoch, defaultSeqLeaseBatch)
		if err != nil {
			t.Error(err)
			return
		}
		if result := fsm.Apply(&hraft.Log{Data: freshWire}); result == nil {
			t.Error("reserve fresh swath: nil result")
		} else if _, ok := result.(vaultctlfsm.SeqLeaseGrant); !ok {
			if err, ok := result.(error); ok {
				t.Errorf("reserve fresh swath: %v", err)
			}
		}
	}()

	grant, err := orch.waitSeqLeaseGrant(vaultID, holder, vaultctlfsm.InitialSeqEpoch, defaultSeqLeaseBatch)
	if err != nil {
		t.Fatalf("waitSeqLeaseGrant: %v", err)
	}
	if grant.Start != defaultSeqLeaseBatch+1 {
		t.Fatalf("grant.Start = %d, want %d (stale 1-256 rejected)", grant.Start, defaultSeqLeaseBatch+1)
	}
}

func TestOrchestratorSeqAssignRenewsPastSwathBatch(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	registerSequencedTestVault(t, orch, vaultID, nil)

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "renew")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	ingester := glid.New()
	wantPastRenew := defaultSeqLeaseBatch + 4
	var lastRec chunk.Record
	for i := range wantPastRenew {
		lastRec = sequencedTestRecord("renew", ingester, uint32(i+1))
		if err := orch.Ingest(lastRec); err != nil {
			t.Fatalf("ingest %d: %v", i+1, err)
		}
	}

	seq, ok := orch.vaultSpoolStore(vaultID).LookupSeq(lastRec.EventID)
	if !ok {
		t.Fatal("last EventID not assigned")
	}
	if seq <= defaultSeqLeaseBatch {
		t.Fatalf("seq after renew = %d, want > %d", seq, defaultSeqLeaseBatch)
	}
}

func TestConcurrentSeqAssignUnique(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-1"})
	registerSequencedTestVault(t, orch, vaultID, nil)

	cr, _ := CompileRoute(glid.New(), "all", 0, "*", []RouteDestination{{VaultID: vaultID}}, "concurrent")
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	ingester := glid.New()
	const goroutines = 16
	const perG = 8

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for g := range goroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range perG {
				rec := sequencedTestRecord("c", ingester, uint32(base*perG+i+1))
				if err := orch.Ingest(rec); err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
