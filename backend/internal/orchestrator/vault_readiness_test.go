package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
)

func TestVaultReplicationReadinessErr_nilVault(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	err := vaultReplicationReadinessErr(vid, nil)
	if !errors.Is(err, ErrVaultNotFound) {
		t.Fatalf("got %v, want ErrVaultNotFound", err)
	}
}

func TestVaultReplicationReadinessErr_noInstance(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	v := &Vault{ID: vid, Instance: nil}
	err := vaultReplicationReadinessErr(vid, v)
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("got %v, want ErrVaultNotReady", err)
	}
}

func TestVaultReplicationReadinessErr_fsmNotReady(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  s.CM,
		Indexes: s.IM,
		Query:   s.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return false },
		},
	}
	v := NewVault(vid, vaultInst)
	if err := vaultReplicationReadinessErr(vid, v); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("got %v, want ErrVaultNotReady", err)
	}
}

func TestVaultReplicationReadinessErr_ready(t *testing.T) {
	t.Parallel()
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  s.CM,
		Indexes: s.IM,
		Query:   s.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return true },
		},
	}
	v := NewVault(vid, vaultInst)
	if err := vaultReplicationReadinessErr(vid, v); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}

func TestListAllChunkMetas_vaultNotReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  s.CM,
		Indexes: s.IM,
		Query:   s.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return false },
		},
	}
	o.RegisterVault(NewVault(vid, vaultInst))
	_, err = o.ListAllChunkMetas(vid)
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("got %v, want ErrVaultNotReady", err)
	}
}

// Regression: ListChunks fans out to remote nodes; a node with the vault
// registered but no local vault placements must not fail ListAllChunkMetas
// with ErrVaultNotReady, or the UI sees 503 and empty chunks.
func TestListAllChunkMetas_noLocalVaultsReturnsEmpty(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	o.RegisterVault(NewVault(vid, nil))
	metas, err := o.ListAllChunkMetas(vid)
	if err != nil {
		t.Fatalf("ListAllChunkMetas: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("expected nil or empty slice, got len=%d", len(metas))
	}
	if err := vaultReplicationReadinessErr(vid, o.vaults[vid]); !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("writes should still see not-ready, got %v", err)
	}
}

func TestSearch_ErrVaultNotReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  s.CM,
		Indexes: s.IM,
		Query:   s.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return false },
		},
	}
	o.RegisterVault(NewVault(vid, vaultInst))
	_, _, err = o.Search(context.Background(), vid, query.Query{}, nil)
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("Search: got %v, want ErrVaultNotReady", err)
	}
}

func TestAppendToVault_ErrVaultNotReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	vid := glid.New()
	vaultID := glid.New()
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vaultInst := &VaultInstance{
		VaultID: vaultID,
		Type:    "memory",
		Chunks:  s.CM,
		Indexes: s.IM,
		Query:   s.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return false },
		},
	}
	o.RegisterVault(NewVault(vid, vaultInst))
	err = o.AppendToVault(vid, chunk.ChunkID{}, chunk.Record{Raw: []byte("x")})
	if !errors.Is(err, ErrVaultNotReady) {
		t.Fatalf("AppendToVault: got %v, want ErrVaultNotReady", err)
	}
}

func TestLocalVaultsReplicationReady(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// liveReplicationReady is used here because the cached
	// LocalVaultsReplicationReady is only refreshed by the goroutine
	// started in Start(), which this test does not call. See
	// gastrolog-5n6xz for the rationale behind splitting the methods.
	if !o.liveReplicationReady() {
		t.Fatal("empty orchestrator should be replication-ready")
	}
	vid := glid.New()
	o.RegisterVault(NewVault(vid, nil))
	if !o.liveReplicationReady() {
		t.Fatal("routing-only vault (has no local vaults — should not block readiness")
	}
	s, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	vaultInst := &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  s.CM,
		Indexes: s.IM,
		Query:   s.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return false },
		},
	}
	o.RegisterVault(NewVault(vid, vaultInst))
	if o.liveReplicationReady() {
		t.Fatal("expected false when local vault-ctl FSM is not ready")
	}
}

// TestReadyzPathNotBlockedByWriter is the gastrolog-5n6xz regression for
// the full /readyz handler responsiveness fix. Every method the handler
// invokes — IsRunning, draining.Load, LocalVaultsReplicationReady — must
// return immediately even while another goroutine is holding o.mu.Lock().
// kubelet's probe times out otherwise, which is the original failure mode
// from the K8s burst scale-out report.
//
// The test acquires o.mu.Lock() directly and asserts that each probe-path
// method completes within a tight deadline. Catching IsRunning here is the
// reason an earlier draft of this fix still produced stuck leader pods —
// the cached LocalVaultsReplicationReady alone wasn't enough; IsRunning
// was also taking o.mu.RLock and starving behind the writer.
func TestReadyzPathNotBlockedByWriter(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Pretend Start() ran: IsRunning must report true so the /readyz
	// handler's conjunction evaluates the rest. CompareAndSwap is used
	// instead of a raw Store to mirror the lifecycle code path.
	if !o.running.CompareAndSwap(false, true) {
		t.Fatal("running flag should have been false on a fresh orchestrator")
	}

	// Cache is seeded true on construction so the empty orchestrator
	// reports ready without needing Start() to run the refresher.
	if !o.LocalVaultsReplicationReady() {
		t.Fatal("freshly-constructed orchestrator should report ready")
	}
	if !o.IsRunning() {
		t.Fatal("IsRunning should report true after CompareAndSwap")
	}

	// Hold the write lock from a background goroutine for longer than
	// any individual /readyz call should ever wait on the probe path.
	released := make(chan struct{})
	holding := make(chan struct{})
	go func() {
		o.mu.Lock()
		close(holding)
		<-released
		o.mu.Unlock()
	}()
	<-holding
	defer close(released)

	// Each /readyz probe-path method must respond promptly regardless
	// of the held writer. 200 ms is generous — every method is an
	// atomic load — but cleanly distinguishes "lock-free" from
	// "starved behind the writer".
	type probe struct {
		name string
		fn   func() bool
	}
	probes := []probe{
		{"IsRunning", o.IsRunning},
		{"LocalVaultsReplicationReady", o.LocalVaultsReplicationReady},
	}
	for _, p := range probes {
		done := make(chan bool, 1)
		go func() { done <- p.fn() }()
		select {
		case got := <-done:
			if !got {
				t.Fatalf("%s returned false under contention; should reflect last-good value", p.name)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("%s blocked while o.mu.Lock was held — /readyz handler would starve", p.name)
		}
	}
}

func TestSearchReadyRegistry_skipsNotReadyVault(t *testing.T) {
	t.Parallel()
	o, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	readyID := glid.New()
	sReady, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	o.RegisterVault(NewVault(readyID, &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  sReady.CM,
		Indexes: sReady.IM,
		Query:   sReady.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return true },
		},
	}))
	notReadyID := glid.New()
	sNR, err := memtest.NewVault(chunkmem.Config{})
	if err != nil {
		t.Fatal(err)
	}
	o.RegisterVault(NewVault(notReadyID, &VaultInstance{
		VaultID: glid.New(),
		Type:    "memory",
		Chunks:  sNR.CM,
		Indexes: sNR.IM,
		Query:   sNR.QE,
		ManifestReadFacet: ManifestReadFacet{
			IsFSMReady: func() bool { return false },
		},
	}))
	reg := &searchReadyRegistry{o: o}
	ids := reg.ListVaults()
	if len(ids) != 1 || ids[0] != readyID {
		t.Fatalf("ListVaults: got %v, want single ready vault %v", ids, readyID)
	}
}
