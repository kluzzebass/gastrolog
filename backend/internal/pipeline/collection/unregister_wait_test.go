package collection_test

import (
	"context"
	"testing"
	"testing/synctest"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/collection"
)

// A deleted vault's files are removed after UnregisterVault's wait returns,
// so the wait must cover a pull still writing under the vault's root: it
// returns only once the worker that ran the pull has exited.
func TestUnregisterVaultWaitCoversInFlightPull(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		vaultID, segID := glid.New(), glid.New()
		pull := newMemoryPull()
		pull.Put(segID, writeSegmentBytes(t, vaultID, segID, "in-flight"))
		gated := &gatedPull{inner: pull, started: make(chan struct{}), release: make(chan struct{})}
		log := &staticLog{}
		log.setAssigned(collection.AssignedSegment{VaultID: vaultID, SegmentID: segID})

		mgr := collection.New(collection.Config{})
		if err := mgr.RegisterVault(vaultID, t.TempDir(), collection.VaultConfig{
			Log: log, Pull: gated, Receipts: &recordingReceipts{},
		}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = mgr.Run(ctx) }()
		<-gated.started

		wait := mgr.UnregisterVault(vaultID)
		waited := make(chan struct{})
		go func() {
			wait()
			close(waited)
		}()
		synctest.Wait()
		select {
		case <-waited:
			t.Fatal("UnregisterVault's wait returned while a pull was still writing under the vault's root")
		default:
		}
		close(gated.release)
		<-waited
	})
}
