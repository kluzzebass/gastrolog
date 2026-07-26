package segmentation_test

import (
	"context"
	"errors"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segmentation"
)

// gastrolog-40anrs: RegisterVault racing Run's startup must never observe the
// exited-manager state. The old lifecycle split (atomic started flag flipped
// outside the mutex, runCtx assigned under it) made a starting manager
// momentarily indistinguishable from an exited one, so a Register landing in
// that window was rejected with ErrNotRunning — the CI failure shape of
// TestSupervisorReconcilePlacementFlap. Hammer the exact interleaving; run
// with -race.
func TestRegisterDuringRunStartupNeverErrNotRunning(t *testing.T) {
	t.Parallel()
	for i := range 300 {
		mgr, _ := segmentation.New(segmentation.Config{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = mgr.Run(ctx)
			close(done)
		}()
		_, err := mgr.RegisterVault(glid.New(), t.TempDir(), segmentation.VaultConfig{})
		if errors.Is(err, segmentation.ErrNotRunning) {
			cancel()
			<-done
			t.Fatalf("iteration %d: RegisterVault observed ErrNotRunning while Run was starting", i)
		}
		if err != nil {
			cancel()
			<-done
			t.Fatalf("iteration %d: RegisterVault: %v", i, err)
		}
		cancel()
		<-done
	}
}

// The exited-manager guard must still hold: after Run returns, RegisterVault
// reports ErrNotRunning rather than silently registering a writer nothing
// will ever run.
func TestRegisterAfterRunExitReturnsErrNotRunning(t *testing.T) {
	t.Parallel()
	mgr, _ := segmentation.New(segmentation.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = mgr.Run(ctx)
		close(done)
	}()
	cancel()
	<-done
	if _, err := mgr.RegisterVault(glid.New(), t.TempDir(), segmentation.VaultConfig{}); !errors.Is(err, segmentation.ErrNotRunning) {
		t.Fatalf("RegisterVault after Run exit: err=%v, want ErrNotRunning", err)
	}
}
