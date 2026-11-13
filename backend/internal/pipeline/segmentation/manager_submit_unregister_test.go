package segmentation_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segmentation"
)

// A direct submit (import, export-to-vault) parked on a full encode queue when
// the vault re-registers must come back with ErrUnknownVault, which callers
// treat as retryable, instead of panicking on the closed queue: in an export
// job that panic kills the node.
func TestManagerSubmitParkedOnFullQueueSurvivesUnregister(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		vaultID := glid.New()
		mgr, _ := segmentation.New(segmentation.Config{EncodeQueueCap: 1})
		// Not running: nothing drains the queue, so the second submit parks.
		if _, err := mgr.RegisterVault(vaultID, dir, segmentation.VaultConfig{}); err != nil {
			t.Fatal(err)
		}
		ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
		if err := mgr.Submit(context.Background(), vaultID, segmentation.Input{Record: sampleRecord(0, ts)}); err != nil {
			t.Fatalf("first submit: %v", err)
		}

		result := make(chan error, 1)
		go func() {
			defer func() {
				if p := recover(); p != nil {
					result <- fmt.Errorf("panic: %v", p)
				}
			}()
			result <- mgr.Submit(context.Background(), vaultID, segmentation.Input{Record: sampleRecord(1, ts)})
		}()
		synctest.Wait()

		mgr.UnregisterVault(vaultID)
		if err := <-result; !errors.Is(err, segmentation.ErrUnknownVault) {
			t.Fatalf("parked submit across unregister: got %v, want ErrUnknownVault", err)
		}
	})
}

// A submit that starts after the vault unregistered sees an unknown vault.
func TestManagerSubmitAfterUnregister(t *testing.T) {
	t.Parallel()
	mgr, _ := segmentation.New(segmentation.Config{})
	vaultID := glid.New()
	if _, err := mgr.RegisterVault(vaultID, t.TempDir(), segmentation.VaultConfig{}); err != nil {
		t.Fatal(err)
	}
	mgr.UnregisterVault(vaultID)
	err := mgr.Submit(context.Background(), vaultID, segmentation.Input{Record: sampleRecord(0, time.Now())})
	if !errors.Is(err, segmentation.ErrUnknownVault) {
		t.Fatalf("submit after unregister: got %v, want ErrUnknownVault", err)
	}
}
