package cluster

import (
	"math"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// rateSeriesStep is one observation fed to both the old pair window and the new
// split rateSeries during the differential comparison.
type rateSeriesStep struct {
	dt         time.Duration // advance from the previous step's clock
	sent       int64         // tx-side cumulative counter
	recv       int64         // rx-side cumulative counter
	membership string
	step       bool // true = advance windows (broadcast tick); false = read-only
}

// --- Frozen oracle -----------------------------------------------------------
//
// frozenPairWindow is a self-contained, frozen copy of the pre-refactor tx/rx
// pair kernel (peerConnStatsWindow + observeTrafficWindowRates + snapshotRates).
// It is the regression oracle: the split rateSeries must forever emit
// byte-identical ThroughputRate protos to this reference. Do NOT "simplify" or
// "clean up" this code — its entire value is being an exact, independent
// snapshot of the old semantics that cannot drift with the production type.
// TestRateSeries_FrozenOracleMatchesProduction verified it against the real
// kernel before that kernel was deleted.

const frozenSparkPoints = 20

var frozenEwmaTaus = [3]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

type frozenPairWindow struct {
	seeded             bool
	lastSent, lastRecv int64
	lastAt             time.Time
	membership         string
	txEwma, rxEwma     [3]float64
	txRates, rxRates   []float64
}

func (w *frozenPairWindow) snapshot() (txPerSec, rxPerSec float64, txEwma, rxEwma [3]float64, txSpark, rxSpark []float64) {
	txSpark = append([]float64(nil), w.txRates...)
	rxSpark = append([]float64(nil), w.rxRates...)
	if len(txSpark) > 0 {
		txPerSec = txSpark[len(txSpark)-1]
	}
	if len(rxSpark) > 0 {
		rxPerSec = rxSpark[len(rxSpark)-1]
	}
	return txPerSec, rxPerSec, w.txEwma, w.rxEwma, txSpark, rxSpark
}

func (w *frozenPairWindow) observe(now time.Time, sent, recv int64, membership string, step bool) (txPerSec, rxPerSec float64, txEwma, rxEwma [3]float64, txSpark, rxSpark []float64) {
	if !w.seeded {
		w.seeded = true
		w.lastSent, w.lastRecv, w.lastAt, w.membership = sent, recv, now, membership
		return 0, 0, w.txEwma, w.rxEwma, nil, nil
	}
	if !step {
		return w.snapshot()
	}
	dt := now.Sub(w.lastAt).Seconds()
	if dt <= 0 {
		return w.snapshot()
	}
	if membership != w.membership {
		w.membership, w.lastSent, w.lastRecv, w.lastAt = membership, sent, recv, now
		return 0, 0, w.txEwma, w.rxEwma, append([]float64(nil), w.txRates...), append([]float64(nil), w.rxRates...)
	}
	if sent < w.lastSent || recv < w.lastRecv {
		w.lastSent, w.lastRecv, w.lastAt = sent, recv, now
		return 0, 0, w.txEwma, w.rxEwma, append([]float64(nil), w.txRates...), append([]float64(nil), w.rxRates...)
	}
	txPerSec = float64(sent-w.lastSent) / dt
	rxPerSec = float64(recv-w.lastRecv) / dt
	for i, tau := range frozenEwmaTaus {
		decay := math.Exp(-dt / tau.Seconds())
		w.txEwma[i] = w.txEwma[i]*decay + txPerSec*(1-decay)
		w.rxEwma[i] = w.rxEwma[i]*decay + rxPerSec*(1-decay)
	}
	w.lastSent, w.lastRecv, w.lastAt = sent, recv, now
	w.txRates = append(w.txRates, txPerSec)
	w.rxRates = append(w.rxRates, rxPerSec)
	if len(w.txRates) > frozenSparkPoints {
		w.txRates = w.txRates[len(w.txRates)-frozenSparkPoints:]
	}
	if len(w.rxRates) > frozenSparkPoints {
		w.rxRates = w.rxRates[len(w.rxRates)-frozenSparkPoints:]
	}
	return txPerSec, rxPerSec, w.txEwma, w.rxEwma, append([]float64(nil), w.txRates...), append([]float64(nil), w.rxRates...)
}

func frozenPairEmit(w *frozenPairWindow, now time.Time, s rateSeriesStep) (tx, rx *gastrologv1.ThroughputRate) {
	txPerSec, rxPerSec, txEwma, rxEwma, txSpark, rxSpark := w.observe(now, s.sent, s.recv, s.membership, s.step)
	tx = &gastrologv1.ThroughputRate{
		InstantPerSec: txPerSec, Avg_1MPerSec: txEwma[0], Avg_5MPerSec: txEwma[1], Avg_15MPerSec: txEwma[2], Spark: txSpark,
	}
	rx = &gastrologv1.ThroughputRate{
		InstantPerSec: rxPerSec, Avg_1MPerSec: rxEwma[0], Avg_5MPerSec: rxEwma[1], Avg_15MPerSec: rxEwma[2], Spark: rxSpark,
	}
	return tx, rx
}

// newSplitEmit drives two independent rateSeries (the target: one honest
// quantity per series) and encodes each via emit().
func newSplitEmit(tx, rx *rateSeries, now time.Time, s rateSeriesStep) (*gastrologv1.ThroughputRate, *gastrologv1.ThroughputRate) {
	tx.observe(now, s.sent, s.membership, s.step)
	rx.observe(now, s.recv, s.membership, s.step)
	return tx.emit(), rx.emit()
}

func assertRateEqual(t *testing.T, label string, want, got *gastrologv1.ThroughputRate) {
	t.Helper()
	if want.InstantPerSec != got.InstantPerSec {
		t.Fatalf("%s instant: want=%v got=%v", label, want.InstantPerSec, got.InstantPerSec)
	}
	if want.Avg_1MPerSec != got.Avg_1MPerSec || want.Avg_5MPerSec != got.Avg_5MPerSec || want.Avg_15MPerSec != got.Avg_15MPerSec {
		t.Fatalf("%s ewma: want=[%v %v %v] got=[%v %v %v]", label,
			want.Avg_1MPerSec, want.Avg_5MPerSec, want.Avg_15MPerSec,
			got.Avg_1MPerSec, got.Avg_5MPerSec, got.Avg_15MPerSec)
	}
	if len(want.Spark) != len(got.Spark) {
		t.Fatalf("%s spark length: want=%d got=%d", label, len(want.Spark), len(got.Spark))
	}
	for i := range want.Spark {
		if want.Spark[i] != got.Spark[i] {
			t.Fatalf("%s spark[%d]: want=%v got=%v", label, i, want.Spark[i], got.Spark[i])
		}
	}
}

// diffSteps walks through every branch of the re-anchor logic: seed, normal
// ramp, between-tick read, dt<=0, counter reset, and summed-series membership
// change, then overflows the 20-point spark ring. Counters are chosen so tx and
// rx diverge in magnitude (they are independent honest quantities); resets and
// membership changes are driven on the pair together — that is how real
// cumulative counters move (process restart resets both; a contributor leaving a
// summed sum re-anchors both under one fingerprint).
func diffSteps() []rateSeriesStep {
	steps := []rateSeriesStep{
		{dt: 0, sent: 1000, recv: 5000, membership: "m1", step: true},                   // seed
		{dt: 5 * time.Second, sent: 2000, recv: 15000, membership: "m1", step: true},    // ramp
		{dt: 5 * time.Second, sent: 3200, recv: 30000, membership: "m1", step: true},    // ramp
		{dt: 5 * time.Second, sent: 4100, recv: 41000, membership: "m1", step: true},    // ramp
		{dt: 2 * time.Second, sent: 9999, recv: 99999, membership: "m1", step: false},   // between-tick read
		{dt: 0, sent: 5000, recv: 50000, membership: "m1", step: true},                  // dt<=0 read-only
		{dt: 5 * time.Second, sent: 5000, recv: 50000, membership: "m1", step: true},    // ramp
		{dt: 5 * time.Second, sent: 100, recv: 900, membership: "m1", step: true},       // counter reset (both)
		{dt: 5 * time.Second, sent: 700, recv: 5900, membership: "m1", step: true},      // post-reset ramp
		{dt: 5 * time.Second, sent: 1300, recv: 11000, membership: "m1", step: true},    // ramp
		{dt: 5 * time.Second, sent: 5300, recv: 60000, membership: "m2", step: true},    // membership change
		{dt: 5 * time.Second, sent: 5900, recv: 66000, membership: "m2", step: true},    // ramp under m2
		{dt: 1 * time.Second, sent: 0, recv: 0, membership: "m2", step: false},          // between-tick read
		{dt: 37 * time.Second, sent: 20000, recv: 200000, membership: "m2", step: true}, // long-dt decay
	}
	sent, recv := int64(20000), int64(200000)
	for i := 0; i < 30; i++ { // overflow the spark ring, confirm trimming matches
		sent += 1500 + int64(i*10)
		recv += 17000 + int64(i*100)
		steps = append(steps, rateSeriesStep{dt: 5 * time.Second, sent: sent, recv: recv, membership: "m2", step: true})
	}
	return steps
}

// TestRateSeries_MatchesOldPairKernel proves the split rateSeries emits
// byte-identical ThroughputRate protos to the frozen tx/rx-pair oracle across
// every branch of the re-anchor logic.
func TestRateSeries_MatchesOldPairKernel(t *testing.T) {
	t.Parallel()

	steps := diffSteps()
	oracle := &frozenPairWindow{}
	newTx, newRx := &rateSeries{}, &rateSeries{}

	now := time.Unix(1_700_000_000, 0)
	var sawSeed, sawNormal, sawRead, sawDtZero, sawReset, sawMembership bool
	prevSent, prevRecv := int64(0), int64(0)
	firstSeen := false

	for i, s := range steps {
		now = now.Add(s.dt)
		wantTx, wantRx := frozenPairEmit(oracle, now, s)
		gotTx, gotRx := newSplitEmit(newTx, newRx, now, s)
		assertRateEqual(t, "tx@"+itoa(i), wantTx, gotTx)
		assertRateEqual(t, "rx@"+itoa(i), wantRx, gotRx)

		switch {
		case !firstSeen:
			sawSeed = true
			firstSeen = true
		case !s.step:
			sawRead = true
		case s.dt <= 0:
			sawDtZero = true
		case i > 0 && steps[i-1].membership != s.membership:
			sawMembership = true
		case s.sent < prevSent || s.recv < prevRecv:
			sawReset = true
		default:
			sawNormal = true
		}
		if s.step && s.dt > 0 {
			prevSent, prevRecv = s.sent, s.recv
		}
	}

	if !(sawSeed && sawNormal && sawRead && sawDtZero && sawReset && sawMembership) {
		t.Fatalf("sequence under-exercised branches: seed=%v normal=%v read=%v dtZero=%v reset=%v membership=%v",
			sawSeed, sawNormal, sawRead, sawDtZero, sawReset, sawMembership)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
