package distribution_test

import (
	"context"
	"testing"
	"testing/synctest"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/segmentation"
)

// A deleted vault's files are removed after UnregisterVault's wait returns,
// so the wait must cover a publish still in flight: a publish that commits
// promotes the segment into the vault's head/.
func TestUnregisterVaultWaitCoversInFlightPublish(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		vaultID := glid.New()
		root := t.TempDir()
		pub := &blockingPublisher{entered: make(chan struct{}, 1), release: make(chan struct{})}
		mgr, _ := distribution.New(distribution.Config{})
		if err := mgr.RegisterVault(vaultID, root, distribution.VaultConfig{
			Publisher:   pub,
			LocalHolder: func() bool { return true },
		}); err != nil {
			t.Fatal(err)
		}
		seg := writeCompletedSegment(t, root, vaultID, "in-flight")
		completed := make(chan segmentation.CompletedSegment, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = mgr.Run(ctx, completed) }()
		completed <- seg
		<-pub.entered

		wait := mgr.UnregisterVault(vaultID)
		waited := make(chan struct{})
		go func() {
			wait()
			close(waited)
		}()
		synctest.Wait()
		select {
		case <-waited:
			t.Fatal("UnregisterVault's wait returned while a publish was still in flight")
		default:
		}
		close(pub.release)
		<-waited
	})
}
