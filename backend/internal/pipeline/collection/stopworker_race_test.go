package collection_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
)

// TestCollectOnceRacesWorkerLifecycle reproduces the interleaving behind
// gastrolog-54kqlj: CollectOnce used to read v.stopWorker after releasing
// m.mu, racing startWorkerLocked's write of that same field under m.mu.
//
// Each vault is registered BEFORE Run so v.stopWorker starts nil and only
// transitions to non-nil when Run's startup loop actually runs (a write
// gated entirely by goroutine scheduling relative to the concurrent
// CollectOnce calls below). Many freshly constructed vaults/managers are
// exercised across many iterations, each hammered by several concurrent
// CollectOnce callers, because the unsynchronized read/write window is a
// handful of instructions around one pointer assignment — a single
// attempt is not reliable enough to land in it.
//
// A concurrent cancel() partway through each iteration also races
// CollectOnce against worker shutdown, covering the residual "worker
// stops between the locked read and awaitCollectPass registering" path
// documented on CollectOnce: that path is expected to return promptly
// (bounded by each call's own short-lived context) rather than hang or
// panic, never to corrupt state.
//
// Run with `go test -race`: before the CollectOnce/UnregisterVault fix in
// this change, this test reliably reports a DATA RACE on v.stopWorker
// (verified by temporarily reverting the fix — see issue notes). After the
// fix, it passes clean under -race across repeated -count runs.
func TestCollectOnceRacesWorkerLifecycle(t *testing.T) {
	t.Parallel()
	const iterations = 300
	const collectors = 6

	for i := 0; i < iterations; i++ {
		vaultID := glid.New()
		root := t.TempDir()
		mgr := collection.New(collection.Config{})
		if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
			Log:      &staticLog{},
			Pull:     newMemoryPull(),
			Receipts: &recordingReceipts{},
		}); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		var runWG sync.WaitGroup
		runWG.Add(1)
		go func() {
			defer runWG.Done()
			_ = mgr.Run(ctx)
		}()

		var callersWG sync.WaitGroup
		for c := 0; c < collectors; c++ {
			callersWG.Add(1)
			go func() {
				defer callersWG.Done()
				// Each CollectOnce call gets its own short-lived, bounded
				// context: the residual TOCTOU window documented on
				// CollectOnce (worker exits between the locked read and
				// awaitCollectPass registering the waiter) means a call
				// can otherwise block on a waiter nobody will ever drain.
				// That is expected pre-existing behavior, not the race
				// this test proves; the bound just keeps the test itself
				// from hanging on it.
				cctx, ccancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer ccancel()
				_ = mgr.CollectOnce(cctx, vaultID)
			}()
		}

		// Race Run's shutdown against the collectors too, instead of only
		// racing startup: cancel concurrently rather than after the
		// collectors finish, so some CollectOnce calls land while the
		// worker goroutine is exiting.
		go func() {
			time.Sleep(time.Millisecond)
			cancel()
		}()

		callersWG.Wait()
		cancel()
		runWG.Wait()
	}
}

// TestUnregisterVaultRacesWorkerStartup exercises the sibling unlocked
// access this issue also fixed: UnregisterVault used to read v.stopWorker
// after releasing m.mu (same field, same writer as above). Vaults are
// registered before Run so the worker-start write and UnregisterVault's
// read/call of stopWorker are driven purely by goroutine scheduling.
func TestUnregisterVaultRacesWorkerStartup(t *testing.T) {
	t.Parallel()
	const iterations = 200

	for i := 0; i < iterations; i++ {
		vaultID := glid.New()
		root := t.TempDir()
		mgr := collection.New(collection.Config{})
		if err := mgr.RegisterVault(vaultID, root, collection.VaultConfig{
			Log:      &staticLog{},
			Pull:     newMemoryPull(),
			Receipts: &recordingReceipts{},
		}); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		var runWG sync.WaitGroup
		runWG.Add(1)
		go func() {
			defer runWG.Done()
			_ = mgr.Run(ctx)
		}()

		var unregWG sync.WaitGroup
		unregWG.Add(1)
		go func() {
			defer unregWG.Done()
			mgr.UnregisterVault(vaultID)
		}()
		unregWG.Wait()

		cancel()
		runWG.Wait()
	}
}
