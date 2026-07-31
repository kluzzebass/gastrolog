package orchestrator

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// A retentionRunner outlives the sweep that refreshes it and is read by
// goroutines that never take o.mu. vaultRetentionGiveUpTTL — the give-up bound
// the chunking release worker uses to decide whether unchunkable segments are
// SHED — reads disposition and rules under runner.mu, having deliberately
// released o.mu first. retentionRunnerFor used to refresh those same fields
// holding only o.mu, so the two ran unsynchronised against each other: the
// sweep's write and the release worker's read, with shedding as the
// consequence of the read.
//
// This test drives both sides concurrently. It is a race-detector test: it
// asserts nothing about interleaving (there is nothing deterministic to assert)
// and passes trivially without -race. Its value is under `go test -race`, where
// it reports the unsynchronised access directly. Run it there, not just here.
func TestRetentionRunnerRefreshRacesGiveUpBound(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	vaultID := glid.New()
	inst := newMemoryInstance(t, vaultID)
	o.RegisterVault(&Vault{ID: vaultID, Instance: inst})

	// Seed the runner and give it rules, so the reader gets past its
	// disposition gate and actually reads the fields the sweep rewrites.
	active := map[string]bool{}
	o.mu.Lock()
	runner := o.retentionRunnerFor(
		system.VaultConfig{ID: vaultID, Name: "v", Type: "memory"}, inst, active)
	o.mu.Unlock()
	runner.mu.Lock()
	runner.rules = []retentionRule{{policy: chunk.NewTTLRetentionPolicy(3 * time.Minute)}}
	runner.disposition = system.RetentionDispositionDelete
	runner.mu.Unlock()

	const iterations = 500
	var wg sync.WaitGroup

	// Writer: the retention sweep refreshing the runner from config. Alternates
	// the disposition so the write is a real change each pass rather than a
	// same-value store the detector could miss.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			disposition := system.RetentionDispositionDelete
			if i%2 == 1 {
				disposition = system.RetentionDispositionRoute
			}
			cfg := system.VaultConfig{
				ID:                   vaultID,
				Name:                 "v",
				Type:                 "memory",
				RetentionDisposition: disposition,
			}
			o.mu.Lock()
			o.retentionRunnerFor(cfg, inst, active)
			o.mu.Unlock()
		}
	}()

	// Reader: the chunking release worker resolving its give-up bound.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			o.vaultRetentionGiveUpTTL(vaultID)
		}
	}()

	wg.Wait()
}
