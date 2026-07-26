package orchestrator_test

import (
	"context"
	"gastrolog/internal/glid"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/chanwatch"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/routing"
)

// pressureAwareIngester is a test ingester that implements
// ingestion.PressureAware. It records the gate it receives via
// SetPressureGate and exposes the number of times gate.Wait returned.
type pressureAwareIngester struct {
	gate       *chanwatch.PressureGate
	waitCalls  atomic.Int32
	started    chan struct{}
	gateSetCh  chan struct{}
	releasedCh chan struct{}
}

func newPressureAwareIngester() *pressureAwareIngester {
	return &pressureAwareIngester{
		started:    make(chan struct{}),
		gateSetCh:  make(chan struct{}),
		releasedCh: make(chan struct{}, 16),
	}
}

func (p *pressureAwareIngester) SetPressureGate(gate *chanwatch.PressureGate) {
	p.gate = gate
	close(p.gateSetCh)
}

func (p *pressureAwareIngester) Run(ctx context.Context, _ chan<- ingestion.IngesterMessage) error {
	close(p.started)
	for {
		if p.gate != nil {
			if err := p.gate.Wait(ctx); err != nil {
				return nil
			}
			p.waitCalls.Add(1)
			// Signal that a Wait call just returned. Non-blocking so the
			// loop never stalls on a full test channel.
			select {
			case p.releasedCh <- struct{}{}:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestPressureGateInjectedIntoPressureAwareIngester verifies that the
// orchestrator calls SetPressureGate on any ingester implementing the
// ingestion.PressureAware interface, before Run is invoked.
func TestPressureGateInjectedIntoPressureAwareIngester(t *testing.T) {
	t.Parallel()
	orch, _ := newIngesterTestSetup(t)

	ing := newPressureAwareIngester()
	orch.RegisterIngester(glid.New(), "pressure-aware", "test", ing)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	// SetPressureGate must have fired before Run.
	select {
	case <-ing.gateSetCh:
	case <-time.After(time.Second):
		t.Fatal("SetPressureGate was not called within 1s of Start")
	}

	// Gate must be non-nil.
	if ing.gate == nil {
		t.Fatal("pressure gate was nil")
	}

	// Orchestrator exposes the same gate through its accessor.
	if got := orch.PressureGate(); got == nil {
		t.Fatal("orchestrator.PressureGate() returned nil")
	} else if got != ing.gate {
		t.Fatal("orchestrator gate does not match injected gate")
	}
}

// TestPressureGateStartsInNormalState verifies the gate is in PressureNormal
// when an orchestrator starts — a PressureAware ingester should not block
// on Wait() at steady state.
func TestPressureGateStartsInNormalState(t *testing.T) {
	t.Parallel()
	orch, _ := newIngesterTestSetup(t)

	ing := newPressureAwareIngester()
	orch.RegisterIngester(glid.New(), "pressure-aware", "test", ing)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	// Wait until the ingester has completed at least one Wait/cycle.
	select {
	case <-ing.releasedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ingester did not complete a cycle within 2s — gate may be blocking at startup")
	}

	if orch.PressureGate().Level() != chanwatch.PressureNormal {
		t.Errorf("gate level at steady state: got %v, want normal", orch.PressureGate().Level())
	}
}

// TestPressureGateFiresTransitionCallbacks verifies that AddOnChange
// callbacks registered on the orchestrator's gate fire on level changes.
// This is the mechanism that the self-ingester uses to adjust min capture
// level under load.
func TestPressureGateFiresTransitionCallbacks(t *testing.T) {
	t.Parallel()
	orch, _ := newIngesterTestSetup(t)

	// Need a PressureAware ingester so the gate is created at Start time.
	ing := newPressureAwareIngester()
	orch.RegisterIngester(glid.New(), "pressure-aware", "test", ing)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	// Grab the gate via the public accessor.
	gate := orch.PressureGate()
	if gate == nil {
		t.Fatal("orchestrator has no pressure gate after Start")
	}

	var (
		mu          sync.Mutex
		transitions []chanwatch.PressureTransition
	)
	gate.AddOnChange(func(tr chanwatch.PressureTransition) {
		mu.Lock()
		defer mu.Unlock()
		transitions = append(transitions, tr)
	})

	// Register a probe that starts critical so the tick forces a transition
	// from normal -> critical. Using AddProbe after Start is supported per
	// the PressureGate contract.
	var fill atomic.Int32
	fill.Store(100)
	gate.AddProbe("test-probe", func() (int, int) {
		return int(fill.Load()), 100
	})

	// The gate runs on a 200ms interval; wait long enough for two ticks.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(transitions)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(transitions) == 0 {
		t.Fatal("expected at least one transition, got 0")
	}
	if transitions[0].To != chanwatch.PressureCritical {
		t.Errorf("first transition: got %v, want critical", transitions[0].To)
	}
	if transitions[0].From != chanwatch.PressureNormal {
		t.Errorf("first transition: From got %v, want normal", transitions[0].From)
	}
}

// slowDigester adds per-message latency so the ingestion→digestion queue can
// fill faster than workers drain it.
type slowDigester struct {
	delay time.Duration
}

func (d *slowDigester) Digest(_ *ingestion.IngestMessage) error {
	time.Sleep(d.delay)
	return nil
}

// floodIngester emits a fixed number of messages as fast as the bounded
// digestion queue accepts them.
type floodIngester struct {
	started chan struct{}
	n       int
}

func (f *floodIngester) Run(ctx context.Context, out chan<- ingestion.IngesterMessage) error {
	close(f.started)
	for range f.n {
		select {
		case out <- ingestion.IngesterMessage{Raw: []byte("x")}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func newPressureQueueFillSetup(t *testing.T) *orchestrator.Orchestrator {
	t.Helper()

	s, _ := memtest.NewVault(chunkmem.Config{
		RotationPolicy: recordCountPolicy(10000),
	})
	defaultID := glid.New()
	orch := mustNewTestOrch(t, orchestrator.Config{
		IngestChannelSize: 10,
		Digesters:         []digestion.Digester{&slowDigester{delay: 200 * time.Millisecond}},
	})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))
	cr, _ := routing.CompileRoute(glid.New(), "all", 0, "*", []glid.GLID{defaultID})
	orch.SetTestRoutingTable(routing.NewTable([]*routing.Route{cr}))
	return orch
}

// TestPressureGateElevatesOnSupervisorQueueFill verifies the orchestrator wires
// the supervisor's ingestion→digestion queue as a gate probe and that
// artificial queue saturation raises pressure above normal.
func TestPressureGateElevatesOnSupervisorQueueFill(t *testing.T) {
	orch := newPressureQueueFillSetup(t)

	flood := &floodIngester{n: 80, started: make(chan struct{})}
	orch.RegisterIngester(glid.New(), "flood", "test", flood)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	select {
	case <-flood.started:
	case <-time.After(time.Second):
		t.Fatal("flood ingester did not start")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if orch.PressureGate().Level() > chanwatch.PressureNormal {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("gate level = %v at depth %d/%d, want elevated or critical",
		orch.PressureGate().Level(), orch.IngestQueueDepth(), orch.IngestQueueCapacity())
}

// TestPressureGateBlocksIngesterUnderSupervisorQueueFill verifies gate.Wait
// blocks while the wired supervisor queue probe reports elevated pressure.
func TestPressureGateBlocksIngesterUnderSupervisorQueueFill(t *testing.T) {
	orch := newPressureQueueFillSetup(t)

	pa := newPressureAwareIngester()
	flood := &floodIngester{n: 80, started: make(chan struct{})}
	orch.RegisterIngester(glid.New(), "pressure-aware", "test", pa)
	orch.RegisterIngester(glid.New(), "flood", "test", flood)

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = orch.Stop() }()

	select {
	case <-pa.gateSetCh:
	case <-time.After(time.Second):
		t.Fatal("SetPressureGate was not called")
	}
	select {
	case <-flood.started:
	case <-time.After(time.Second):
		t.Fatal("flood ingester did not start")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if orch.PressureGate().Level() > chanwatch.PressureNormal {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if orch.PressureGate().Level() <= chanwatch.PressureNormal {
		t.Fatalf("gate never elevated; depth %d/%d",
			orch.IngestQueueDepth(), orch.IngestQueueCapacity())
	}

	gate := orch.PressureGate()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer waitCancel()
	if err := gate.Wait(waitCtx); err == nil {
		t.Fatal("gate.Wait returned while supervisor queue probe reports elevated pressure")
	}
}
