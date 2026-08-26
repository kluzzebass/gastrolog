// Package limits holds the ceilings ingesters apply to untrusted wire input.
//
// Every network ingester accepts connections without authentication (RELP's
// optional mutual TLS aside), so every length, count and decompressed size an
// ingester reads is chosen by whoever can reach the port. A wire-supplied
// number that reaches an allocator unchecked is a one-frame node kill; a
// connection with no bound on how long it may hold a goroutine is a
// resource-hold. The ceilings here are where those numbers stop being
// trusted.
//
// They are fixed rather than operator-tunable on purpose. Each is set far
// above any value a conforming producer emits, so the only reason to change
// one would be to raise a safety ceiling — a knob whose correct setting is
// always "leave it alone" is a liability, not a feature. Protocol-specific
// framing that is genuinely narrower than these (Docker's 16 KB log frames)
// stays narrow at its own call site.
package limits

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// MaxFrameBytes bounds one framed message read off a socket: a RELP frame's
// DATALEN, a syslog octet count or newline-delimited line, a Docker log
// frame. Without it the sender's own length field sizes the allocation, so
// one frame claiming gigabytes exhausts the node's memory before a single
// payload byte arrives. Syslog messages run to a few kilobytes; 1 MiB leaves
// three orders of magnitude of headroom.
const MaxFrameBytes = 1 << 20

// MaxTokenBytes bounds a protocol header token — a RELP transaction number,
// command or length field. These are a handful of characters; a sender that
// never emits the delimiter would otherwise grow the token buffer for as
// long as it keeps writing.
const MaxTokenBytes = 64

// MaxDecompressedBytes bounds what a compressed request body may expand to.
// Bounding the compressed input alone is no bound at all: a few kilobytes of
// crafted zstd expand to gigabytes. Callers pass their own body limit where
// they have one; this is the ceiling for callers that do not.
const MaxDecompressedBytes = 100 << 20

// MaxConnections bounds concurrent connections on one listener. Each
// connection holds a goroutine, a socket and its read buffer, so an
// unbounded accept loop lets one peer exhaust file descriptors. A node
// fronting thousands of senders stays well under this.
const MaxConnections = 4096

// FrameTimeout bounds how long a sender may take to finish a frame it has
// started. It deliberately does not bound idle time between frames: a syslog
// relay on a quiet host legitimately holds an open connection for hours,
// while a sender that dribbles one byte at a time to hold a connection slot
// is finishing no frame at all.
const FrameTimeout = 30 * time.Second

// Attrs bounds the attribute map an ingester builds from one record.
// Attributes become index terms, so an unbounded map is a cardinality
// explosion in the vault as much as a memory cost at ingest.
type Attrs struct {
	// Count is the most producer-supplied attributes one record may carry.
	// An ingester's own identifying attributes — ingester_type, severity,
	// trace ids — are a fixed handful set outside this budget, so they are
	// never crowded out by a flood.
	Count int
	// KeyBytes and ValueBytes bound a single attribute's key and value.
	KeyBytes   int
	ValueBytes int
}

// Labels bounds label-shaped attribute models, where the producer chooses a
// small set of short indexing labels — the Loki push protocol's streams.
var Labels = Attrs{Count: 32, KeyBytes: 64, ValueBytes: 256}

// Records bounds protocols whose producers attach arbitrary structured data
// to each record: OTLP resource, scope and record attributes, Fluent record
// fields, Kafka headers. Real producers emit dozens of attributes and the
// occasional multi-kilobyte value (a stack trace), so the ceiling sits above
// those rather than at label scale.
var Records = Attrs{Count: 128, KeyBytes: 256, ValueBytes: 32 << 10}

// Add stores key and value in attrs, or returns an error naming the ceiling
// it would have crossed. Callers that speak a protocol with an error
// response reject the record; callers that cannot reject drop the one
// attribute and report how many they dropped, because an attribute silently
// missing from a record is its own bug.
func (a Attrs) Add(attrs map[string]string, key, value string) error {
	if _, replacing := attrs[key]; !replacing && len(attrs) >= a.Count {
		return fmt.Errorf("too many attributes (max %d)", a.Count)
	}
	if len(key) > a.KeyBytes {
		return fmt.Errorf("attribute key too long: %d > %d", len(key), a.KeyBytes)
	}
	if len(value) > a.ValueBytes {
		return fmt.Errorf("attribute value too long: %d > %d", len(value), a.ValueBytes)
	}
	attrs[key] = value
	return nil
}

// ConnLimiter caps how many connections one listener serves at once.
// The zero value is unusable; call NewConnLimiter.
type ConnLimiter struct {
	max  int64
	open atomic.Int64
}

// NewConnLimiter returns a limiter admitting at most limit concurrent
// connections.
func NewConnLimiter(limit int) *ConnLimiter {
	return &ConnLimiter{max: int64(limit)}
}

// Acquire reports whether another connection fits. Every true must be
// matched by exactly one Release.
func (l *ConnLimiter) Acquire() bool {
	if l.open.Add(1) > l.max {
		l.open.Add(-1)
		return false
	}
	return true
}

// Release returns a slot taken by Acquire.
func (l *ConnLimiter) Release() { l.open.Add(-1) }

// Open reports how many connections currently hold a slot.
func (l *ConnLimiter) Open() int { return int(l.open.Load()) }

// FrameConn bounds how long a partially-read frame may stay unfinished.
// The deadline is armed on the first read of a frame and cleared by Done
// once the frame is complete, so an idle connection is never disconnected
// but a sender that stops mid-frame is.
//
// It is a per-connection reader: one goroutine reads it at a time, as every
// connection handler does.
type FrameConn struct {
	net.Conn
	timeout time.Duration
	armed   bool
}

// NewFrameConn wraps conn so reads within a frame must complete inside
// timeout.
func NewFrameConn(conn net.Conn, timeout time.Duration) *FrameConn {
	return &FrameConn{Conn: conn, timeout: timeout}
}

func (c *FrameConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && !c.armed {
		// A frame has started. Everything still missing from it must
		// arrive inside the timeout.
		c.armed = true
		_ = c.SetReadDeadline(time.Now().Add(c.timeout))
	}
	return n, err
}

// Done marks the current frame complete, clearing the deadline so a quiet
// sender keeps its connection until it starts the next frame.
func (c *FrameConn) Done() {
	if !c.armed {
		return
	}
	c.armed = false
	_ = c.SetReadDeadline(time.Time{})
}
