package routing_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/record"
)

// distHarness runs a routing manager over one catch-all route with the given
// distribution, its destinations backed by buffered channels and a gate that
// refuses whichever vaults are marked refusing.
type distHarness struct {
	t        *testing.T
	mgr      *routing.Manager
	in       chan routing.Input
	done     chan struct{}
	cancel   context.CancelFunc
	sinks    map[glid.GLID]chan segmentation.Input
	refusing map[glid.GLID]*atomic.Bool
}

var errRefused = errors.New("vault refuses records")

func newDistHarness(t *testing.T, dist routing.Distribution, accepting, disabled []glid.GLID) *distHarness {
	t.Helper()
	r, err := routing.CompileRoute(glid.New(), "spread", 0, "*", accepting)
	if err != nil {
		t.Fatal(err)
	}
	r.Distribution = dist
	r.DisabledVaultIDs = disabled
	h := &distHarness{
		t:        t,
		in:       make(chan routing.Input, 64),
		done:     make(chan struct{}),
		sinks:    map[glid.GLID]chan segmentation.Input{},
		refusing: map[glid.GLID]*atomic.Bool{},
	}
	vaults := map[glid.GLID]chan<- segmentation.Input{}
	for _, id := range append(append([]glid.GLID(nil), accepting...), disabled...) {
		ch := make(chan segmentation.Input, 64)
		h.sinks[id] = ch
		vaults[id] = ch
		h.refusing[id] = &atomic.Bool{}
	}
	h.mgr = routing.New(routing.Config{
		Workers: 1,
		Table:   routing.NewTable([]*routing.Route{r}),
		Vaults:  vaults,
		VaultGate: func(id glid.GLID) error {
			if b := h.refusing[id]; b != nil && b.Load() {
				return errRefused
			}
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		_ = h.mgr.Run(ctx, h.in)
		close(h.done)
	}()
	t.Cleanup(cancel)
	return h
}

// send routes one record and returns its ack. A delivered record's ack is
// resolved here, as segmentation would once the record is durable.
func (h *distHarness) send() error {
	rec := &record.Record{Attrs: record.Attributes{"k": "v"}}
	ack := make(chan error, 1)
	h.in <- routing.Input{Record: rec, Source: routing.IngestSource(rec), Ack: ack}
	return h.settle(ack)
}

func (h *distHarness) settle(ack chan error) error {
	for {
		select {
		case err := <-ack:
			return err
		default:
		}
		for _, ch := range h.sinks {
			select {
			case got := <-ch:
				// Put it back for counting; resolve its ack.
				if got.Ack != nil {
					got.Ack <- nil
					got.Ack = nil
				}
				ch <- got
			default:
			}
		}
	}
}

// received counts what each destination got, once routing has stopped.
func (h *distHarness) received() map[glid.GLID]int {
	close(h.in)
	<-h.done
	out := map[glid.GLID]int{}
	for id, ch := range h.sinks {
		out[id] = len(ch)
	}
	return out
}

// Failover sends each record to the first destination, in configured order,
// that admits it, and refuses the record only when none does.
func TestFailoverDeliversToTheFirstAdmittingDestination(t *testing.T) {
	t.Parallel()
	a, b := glid.New(), glid.New()
	h := newDistHarness(t, routing.DistributeFailover, []glid.GLID{a, b}, nil)

	for range 3 {
		if err := h.send(); err != nil {
			t.Fatalf("send with both admitting: %v", err)
		}
	}
	h.refusing[a].Store(true)
	for range 2 {
		if err := h.send(); err != nil {
			t.Fatalf("send with the first refusing: %v", err)
		}
	}
	h.refusing[b].Store(true)
	err := h.send()
	if !errors.Is(err, errRefused) {
		t.Fatalf("send with every destination refusing = %v, want the refusal", err)
	}
	for _, id := range []glid.GLID{a, b} {
		if !strings.Contains(err.Error(), id.String()) {
			t.Fatalf("refusal %q does not name destination %s", err, id)
		}
	}

	got := h.received()
	if got[a] != 3 || got[b] != 2 {
		t.Fatalf("received a=%d b=%d, want 3 and 2: each record once, at the first admitting destination", got[a], got[b])
	}
}

// Round-robin rotates records across the destinations, one each, skipping a
// destination that refuses rather than refusing the record.
func TestRoundRobinRotatesAndSkipsARefusingDestination(t *testing.T) {
	t.Parallel()
	a, b := glid.New(), glid.New()
	h := newDistHarness(t, routing.DistributeRoundRobin, []glid.GLID{a, b}, nil)

	for range 4 {
		if err := h.send(); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	h.refusing[b].Store(true)
	for range 2 {
		if err := h.send(); err != nil {
			t.Fatalf("send with one destination refusing: %v", err)
		}
	}

	got := h.received()
	if got[a] != 4 || got[b] != 2 {
		t.Fatalf("received a=%d b=%d, want 4 and 2: rotated 2/2, then both to a while b refuses", got[a], got[b])
	}
}

// Fanout still sends every record to every destination, and a refusing
// destination refuses the whole record.
func TestFanoutSendsEveryRecordToEveryDestination(t *testing.T) {
	t.Parallel()
	a, b := glid.New(), glid.New()
	h := newDistHarness(t, routing.DistributeFanout, []glid.GLID{a, b}, nil)

	for range 2 {
		if err := h.send(); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	h.refusing[b].Store(true)
	if err := h.send(); !errors.Is(err, errRefused) {
		t.Fatalf("send with one destination refusing = %v, want the refusal", err)
	}

	got := h.received()
	if got[a] != 2 || got[b] != 2 {
		t.Fatalf("received a=%d b=%d, want 2 each", got[a], got[b])
	}
}

// A disabled destination receives nothing, and each matched record counts as
// dropped for it; a route whose destinations are all disabled delivers
// nowhere without refusing the record.
func TestDisabledDestinationReceivesNothing(t *testing.T) {
	t.Parallel()
	a, off := glid.New(), glid.New()
	h := newDistHarness(t, routing.DistributeFanout, []glid.GLID{a}, []glid.GLID{off})
	for range 3 {
		if err := h.send(); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	got := h.received()
	if got[a] != 3 || got[off] != 0 {
		t.Fatalf("received a=%d disabled=%d, want 3 and 0", got[a], got[off])
	}
	if dropped := h.mgr.Stats().PerVaultDropped[off]; dropped != 3 {
		t.Fatalf("dropped for the disabled destination = %d, want 3", dropped)
	}

	allOff := newDistHarness(t, routing.DistributeFailover, nil, []glid.GLID{off})
	if err := allOff.send(); err != nil {
		t.Fatalf("send to a route whose destinations are all disabled = %v, want accepted", err)
	}
	if got := allOff.received(); got[off] != 0 {
		t.Fatalf("a disabled destination received %d records", got[off])
	}
	if dropped := allOff.mgr.Stats().PerVaultDropped[off]; dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
}

func TestParseDistribution(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]routing.Distribution{
		"":            routing.DistributeFanout,
		"fanout":      routing.DistributeFanout,
		"round-robin": routing.DistributeRoundRobin,
		"failover":    routing.DistributeFailover,
	} {
		if got, err := routing.ParseDistribution(in); err != nil || got != want {
			t.Errorf("ParseDistribution(%q) = (%q, %v), want %q", in, got, err, want)
		}
	}
	if _, err := routing.ParseDistribution("random"); err == nil {
		t.Error("ParseDistribution accepted an unknown distribution")
	}
}

// A round-robin route keeps rotating across a table swap: the table is
// rebuilt on every config reload, and restarting each route at its first
// destination would favour that destination on every reload.
func TestRoundRobinRotationSurvivesATableSwap(t *testing.T) {
	t.Parallel()
	a, b := glid.New(), glid.New()
	h := newDistHarness(t, routing.DistributeRoundRobin, []glid.GLID{a, b}, nil)
	route, _ := h.mgr.Table().MatchRoute(nil, routing.IngestSource(nil))

	rebuilt := func() *routing.Table {
		r, err := routing.CompileRoute(route.ID, route.Name, route.Priority, "*", []glid.GLID{a, b})
		if err != nil {
			t.Fatal(err)
		}
		r.Distribution = routing.DistributeRoundRobin
		return routing.NewTable([]*routing.Route{r})
	}
	for range 3 {
		if err := h.send(); err != nil {
			t.Fatal(err)
		}
		h.mgr.SetTable(rebuilt())
	}
	got := h.received()
	if got[a] != 2 || got[b] != 1 {
		t.Fatalf("received a=%d b=%d across table swaps, want 2 and 1 (rotation a, b, a)", got[a], got[b])
	}
}
