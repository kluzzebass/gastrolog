package chunking_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// A deleted vault's files are removed after UnregisterVault's wait returns,
// so the wait must cover the vault's worker — whose builds write GLCBs under
// the vault's root — until it has exited.
func TestUnregisterVaultWaitCoversWorker(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		vaultID := glid.New()
		home := t.TempDir()
		mgr := chunking.New(chunking.Config{})
		if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
			RequiredHolders: chunking.NoRequiredHolders,
			VaultRoot:       home,
			ChunkRoot:       filepath.Join(home, "chunks"),
			FSM:             vaultctlfsm.New(),
			Locate:          chunking.HeadSegmentLocator{Root: home},
			IsLeader:        func() bool { return false },
		}); err != nil {
			t.Fatal(err)
		}
		parked, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		mgr.SetWorkerPassHookForTest(vaultID, func() {
			once.Do(func() {
				close(parked)
				<-release
			})
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = mgr.Run(ctx) }()
		<-parked

		wait := mgr.UnregisterVault(vaultID)
		waited := make(chan struct{})
		go func() {
			wait()
			close(waited)
		}()
		synctest.Wait()
		select {
		case <-waited:
			t.Fatal("UnregisterVault's wait returned while the vault's worker was still running")
		default:
		}
		close(release)
		<-waited
	})
}
