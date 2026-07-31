package cluster

import (
	"math"
	"strings"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/sparkline"
)

// ewmaTaus are the Unix-load-average horizons for sustained rates.
var ewmaTaus = [3]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// rateSparkPoints is how many per-tick rate samples a series keeps for its
// spark history. Broadcast ticks are ~5s apart (StatsCollector.Interval), so
// 20 points is ~100s of burst shape — enough to read a spike or a stall
// without an unbounded buffer.
const rateSparkPoints = 20

// rateSeries is the rolling rate/spark window for ONE cumulative counter. It
// encapsulates the whole "cumulative counter in, per-second rate + sustained
// EWMAs + spark history out" transform and encodes itself to the wire via
// emit(). One series is one honest quantity: append_records and append_bytes
// are separate series, peer tx and peer rx are separate series, and so on.
//
// This replaces the tx/rx-pair window (peerConnStatsWindow) that crammed two
// counters into a single struct, the five hand-partitioned store maps, and the
// nine hand-assembled ThroughputRate literals that read fields off the pair's
// per-tick return value.
//
// The subtle re-anchor semantics (seed, between-tick reads, counter reset,
// summed-series membership change) live in observe() ONLY.
type rateSeries struct {
	seeded bool
	last   int64
	lastAt time.Time
	// membership fingerprints the contributor set behind a SUMMED series
	// (cluster totals from TTL-live peer broadcasts). A contributor whose
	// stats expired and later resumed rejoins the sum as a one-tick upward
	// jump indistinguishable from real traffic — the 5m EWMA read 138K/s from
	// a 40K/s source. Any fingerprint change re-anchors the
	// window exactly like a counter reset: no sample, EWMA + spark preserved.
	// Per-entity series pass a constant fingerprint and never trigger it.
	membership string
	// Unix-load-style EWMAs (one float per horizon, no history buffer): each
	// step folds the instantaneous rate in with e^(-dt/tau) decay, tau =
	// 1m/5m/15m.
	ewma [3]float64
	// ring is the per-tick rate history rendered as the wire spark. It is a
	// generic sparkline.Sparkline[float64] — the same domain-free bounded-history
	// primitive a gauge would compose — created lazily so the zero rateSeries is
	// still usable (seriesLocked never has to pre-size it).
	ring *sparkline.Sparkline[float64]
	// curInstant is the instantaneous per-second rate this series emits right
	// now. observe() recomputes it every call so emit()/instant() reproduce the
	// old fused observe-and-return exactly: a normal step emits the freshly
	// measured rate, a re-anchor (seed/membership/reset) emits 0 without a
	// sample, and a between-tick read emits the last stepped point.
	curInstant float64
}

// observe folds one cumulative-counter reading into the window. When step is
// false (reads between broadcast ticks) it does not advance the window; it only
// refreshes curInstant from the last stepped point so a between-tick emit()
// reflects the current window without skewing rate calculations.
func (s *rateSeries) observe(now time.Time, counter int64, membership string, step bool) {
	if !s.seeded {
		// First observation seeds the baseline and emits zero.
		s.seeded = true
		s.last = counter
		s.lastAt = now
		s.membership = membership
		s.curInstant = 0
		return
	}

	if !step {
		s.readOnly()
		return
	}

	dt := now.Sub(s.lastAt).Seconds()
	if dt <= 0 {
		s.readOnly()
		return
	}

	if membership != s.membership {
		// Contributor set changed under a summed series: this tick's delta
		// mixes real traffic with counters entering/leaving the sum, so it is
		// not a measurable sample. Re-anchor, preserve EWMAs and spark, emit no
		// sample.
		s.membership = membership
		s.last = counter
		s.lastAt = now
		s.curInstant = 0
		return
	}

	if counter < s.last {
		// Counter reset (process restart, peer expiry in a summed series):
		// re-anchor the counter but PRESERVE the EWMAs and spark — the
		// sustained-rate history is still true; only the delta baseline moved.
		// This tick has no measurable delta, so instant reads 0 and the EWMAs
		// are not updated (no sample, rather than a fake zero).
		s.last = counter
		s.lastAt = now
		s.curInstant = 0
		return
	}

	perSec := float64(counter-s.last) / dt
	for i, tau := range ewmaTaus {
		decay := math.Exp(-dt / tau.Seconds())
		s.ewma[i] = s.ewma[i]*decay + perSec*(1-decay)
	}
	s.last = counter
	s.lastAt = now
	if s.ring == nil {
		s.ring = sparkline.New[float64](rateSparkPoints)
	}
	s.ring.Push(perSec)
	s.curInstant = perSec
}

// readOnly refreshes curInstant from the last stepped spark point without
// advancing the window (the old snapshotRates instant semantics for reads
// between broadcast ticks).
func (s *rateSeries) readOnly() {
	if s.ring != nil {
		if last, ok := s.ring.Last(); ok {
			s.curInstant = last
			return
		}
	}
	s.curInstant = 0
}

// emit encodes the current window to the wire. This is the ONLY site that
// assembles a ThroughputRate. Safe to call between ticks: it returns the
// current window without advancing it. Spark is copied so callers cannot mutate
// the ring.
func (s *rateSeries) emit() *gastrologv1.ThroughputRate {
	var spark []float64
	if s.ring != nil {
		spark = s.ring.Values() // already a defensive copy
	}
	return &gastrologv1.ThroughputRate{
		InstantPerSec: s.curInstant,
		Avg_1MPerSec:  s.ewma[0],
		Avg_5MPerSec:  s.ewma[1],
		Avg_15MPerSec: s.ewma[2],
		Spark:         spark,
	}
}

// instant returns just the current per-second rate, for the non-ThroughputRate
// consumers (Raft WAL average latency and elections-per-minute).
func (s *rateSeries) instant() float64 {
	return s.curInstant
}

// --- Collector integration ---------------------------------------------------

// emitRate observes a per-entity counter series (constant membership) under the
// collector lock and returns its wire encoding. Get-or-create + observe + emit
// happen atomically, reproducing the old fused observe-and-return semantics.
func (c *StatsCollector) emitRate(now time.Time, key string, counter int64, step bool) *gastrologv1.ThroughputRate {
	return c.emitRateM(now, key, counter, "", step)
}

// emitRateM is emitRate with an explicit membership fingerprint for summed
// cluster series.
func (c *StatsCollector) emitRateM(now time.Time, key string, counter int64, membership string, step bool) *gastrologv1.ThroughputRate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seriesLocked(key, now, counter, membership, step).emit()
}

// observeRateInstant observes a series and returns just its instantaneous rate,
// for the consumers that do not encode a ThroughputRate (Raft WAL average
// latency, elections-per-minute).
func (c *StatsCollector) observeRateInstant(now time.Time, key string, counter int64, step bool) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seriesLocked(key, now, counter, "", step).instant()
}

// seriesLocked returns the series for key, creating it on first use, and folds
// in the observation. Caller must hold c.mu.
func (c *StatsCollector) seriesLocked(key string, now time.Time, counter int64, membership string, step bool) *rateSeries {
	s := c.rates[key]
	if s == nil {
		s = &rateSeries{}
		c.rates[key] = s
	}
	s.observe(now, counter, membership, step)
	return s
}

// --- Series keys -------------------------------------------------------------
//
// All series share one map, so keys are namespaced by role. Peer-scoped series
// embed the peer node ID so peer removal (Delete/ReconcilePeers) can find them.

const (
	rateKeyPeerConnTx  = "peerconn:tx:"
	rateKeyPeerConnRx  = "peerconn:rx:"
	rateKeyPeerTotalTx = "peertotal:tx:"
	rateKeyPeerTotalRx = "peertotal:rx:"
)

// rateSeriesPeerID extracts the peer node ID from a peer-scoped series key,
// reporting ok=false for series that are not peer-scoped (vault/route/raft).
func rateSeriesPeerID(key string) (peer string, ok bool) {
	switch {
	case strings.HasPrefix(key, rateKeyPeerConnTx):
		return connKeyPeer(key[len(rateKeyPeerConnTx):]), true
	case strings.HasPrefix(key, rateKeyPeerConnRx):
		return connKeyPeer(key[len(rateKeyPeerConnRx):]), true
	case strings.HasPrefix(key, rateKeyPeerTotalTx):
		return key[len(rateKeyPeerTotalTx):], true
	case strings.HasPrefix(key, rateKeyPeerTotalRx):
		return key[len(rateKeyPeerTotalRx):], true
	}
	return "", false
}

// connKeyPeer returns the peer node ID from a "peer\x00lane\x00group\x00pool"
// conn key body.
func connKeyPeer(body string) string {
	peer, _, _ := strings.Cut(body, "\x00")
	return peer
}
