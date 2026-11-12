package orchestrator

import (
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
)

// The scheduler runs from New, so a job — a vault teardown, a sweep — can
// read the vault-ctl group manager while ApplyConfig is still publishing it.
// Under -race this reports any unsynchronized publication.
func TestGroupManagerPublishedSafelyToConcurrentReaders(t *testing.T) {
	orch := newTestOrch(t, Config{LocalNodeID: "node"})
	gm := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{NodeID: "node"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			orch.vaultCtlHandle(glid.New())
		}
	}()
	if err := orch.ApplyConfig(nil, Factories{GroupManager: gm, HomeDir: t.TempDir()}); err != nil {
		t.Fatalf("ApplyConfig(nil): %v", err)
	}
	<-done
}
