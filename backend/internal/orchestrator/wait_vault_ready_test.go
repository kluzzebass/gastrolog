package orchestrator_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

// notReadyVault builds a registered-but-not-ready vault: a non-nil instance
// whose IsFSMReady never returns true, so ReadinessErr keeps reporting the
// vault as not ready. The returned channel receives a token the first time
// the orchestrator evaluates readiness — the test uses it to know a waiter
// has observed the vault present before the test mutates the registry, which
// keeps "deleted while waiting" deterministic instead of timing-based.
func notReadyVault(id glid.GLID) (*orchestrator.Vault, <-chan struct{}) {
	checked := make(chan struct{}, 1)
	inst := &orchestrator.VaultInstance{
		VaultID: id,
		IsFSMReady: func() bool {
			select {
			case checked <- struct{}{}:
			default:
			}
			return false
		},
	}
	return orchestrator.NewVault(id, inst), checked
}

// TestWaitVaultReady_AlreadyReady: a vault registered (as a routing shell)
// before the wait begins is observed ready and returns immediately.
func TestWaitVaultReady_AlreadyReady(t *testing.T) {
	t.Parallel()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()
	orch.RegisterVault(orchestrator.NewVault(id, nil)) // routing shell == ready

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := orch.WaitVaultReady(ctx, id); err != nil {
		t.Fatalf("WaitVaultReady on already-ready vault: %v", err)
	}
}

// TestWaitVaultReady_BecomesReady: a waiter blocked on an unregistered vault
// wakes and returns nil once the vault is registered (the readiness
// transition), with no polling.
func TestWaitVaultReady_BecomesReady(t *testing.T) {
	t.Parallel()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- orch.WaitVaultReady(ctx, id) }()

	// Drive the readiness transition explicitly.
	orch.RegisterVault(orchestrator.NewVault(id, nil))

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("WaitVaultReady after registration: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitVaultReady did not wake after vault registration")
	}
}

// TestWaitVaultReady_NeverReady_CtxCancel: a waiter on a vault that never
// becomes ready returns the context error when the caller cancels. Driven by
// an explicit cancel, not a timer.
func TestWaitVaultReady_NeverReady_CtxCancel(t *testing.T) {
	t.Parallel()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- orch.WaitVaultReady(ctx, id) }()

	cancel() // caller gives up

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitVaultReady did not return after ctx cancel")
	}
}

// TestWaitVaultReady_DeletedWhileWaiting: once a waiter has observed the vault
// present (but not ready), removing it wakes the waiter with ErrVaultNotFound
// rather than hanging until the context expires.
func TestWaitVaultReady_DeletedWhileWaiting(t *testing.T) {
	t.Parallel()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()

	vault, checked := notReadyVault(id)
	orch.RegisterVault(vault)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- orch.WaitVaultReady(ctx, id) }()

	// Wait until the waiter has evaluated readiness at least once and thus
	// observed the vault present. Then remove it — deterministic ordering.
	select {
	case <-checked:
	case <-time.After(10 * time.Second):
		t.Fatal("waiter never evaluated readiness")
	}

	if err := orch.UnregisterVault(id); err != nil {
		t.Fatalf("UnregisterVault: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, orchestrator.ErrVaultNotFound) {
			t.Fatalf("expected ErrVaultNotFound after deletion, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitVaultReady hung after vault deletion")
	}
}

// TestWaitVaultReady_ConcurrentWaiters: many waiters blocked on the same
// pending vault are all woken by a single registration and each returns nil
// exactly once.
func TestWaitVaultReady_ConcurrentWaiters(t *testing.T) {
	t.Parallel()
	orch := mustNewTestOrch(t, orchestrator.Config{})
	id := glid.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 32
	var wg sync.WaitGroup
	results := make([]error, n)
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			results[i] = orch.WaitVaultReady(ctx, id)
		}()
	}

	// Single registration must wake every waiter.
	orch.RegisterVault(orchestrator.NewVault(id, nil))

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not all concurrent waiters woke after registration")
	}

	for i, err := range results {
		if err != nil {
			t.Fatalf("waiter %d: expected nil, got %v", i, err)
		}
	}
}
