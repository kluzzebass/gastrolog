package limits

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestAttrsRefusesTooManyAttributes(t *testing.T) {
	t.Parallel()
	lim := Attrs{Count: 2, KeyBytes: 16, ValueBytes: 16}
	attrs := map[string]string{}

	if err := lim.Add(attrs, "a", "1"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := lim.Add(attrs, "b", "2"); err != nil {
		t.Fatalf("second: %v", err)
	}
	err := lim.Add(attrs, "c", "3")
	if err == nil {
		t.Fatal("stored an attribute past the count ceiling")
	}
	if !strings.Contains(err.Error(), "too many") {
		t.Errorf("error does not name the ceiling: %v", err)
	}
	if len(attrs) != 2 {
		t.Errorf("map grew past the ceiling: %d entries", len(attrs))
	}
}

func TestAttrsAllowsReplacingAnExistingKeyAtTheCeiling(t *testing.T) {
	t.Parallel()
	lim := Attrs{Count: 1, KeyBytes: 16, ValueBytes: 16}
	attrs := map[string]string{"a": "1"}

	if err := lim.Add(attrs, "a", "2"); err != nil {
		t.Fatalf("replacing a key must not count against the budget: %v", err)
	}
	if attrs["a"] != "2" {
		t.Errorf("value not replaced: %q", attrs["a"])
	}
}

func TestAttrsRefusesOversizeKeysAndValues(t *testing.T) {
	t.Parallel()
	lim := Attrs{Count: 8, KeyBytes: 4, ValueBytes: 4}
	attrs := map[string]string{}

	if err := lim.Add(attrs, strings.Repeat("k", 5), "v"); err == nil {
		t.Error("stored an oversize key")
	}
	if err := lim.Add(attrs, "k", strings.Repeat("v", 5)); err == nil {
		t.Error("stored an oversize value")
	}
	if len(attrs) != 0 {
		t.Errorf("rejected attributes were stored anyway: %v", attrs)
	}
}

func TestConnLimiterRefusesPastItsCap(t *testing.T) {
	t.Parallel()
	lim := NewConnLimiter(2)

	if !lim.Acquire() || !lim.Acquire() {
		t.Fatal("limiter refused a connection inside its cap")
	}
	if lim.Acquire() {
		t.Fatal("limiter admitted a connection past its cap")
	}
	if lim.Open() != 2 {
		t.Errorf("a refused Acquire leaked a slot: open=%d", lim.Open())
	}

	lim.Release()
	if !lim.Acquire() {
		t.Error("a released slot was not reusable")
	}
}

// TestFrameConnDisconnectsASenderThatStopsMidFrame proves the deadline is
// armed once a frame starts, so a sender that dribbles bytes to hold a
// connection slot is dropped.
func TestFrameConnDisconnectsASenderThatStopsMidFrame(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	framed := NewFrameConn(server, 50*time.Millisecond)

	go func() {
		_, _ = client.Write([]byte("partial"))
		// Then nothing: the frame never finishes.
	}()

	buf := make([]byte, 64)
	if _, err := framed.Read(buf); err != nil {
		t.Fatalf("first read: %v", err)
	}

	if _, err := framed.Read(buf); err == nil {
		t.Fatal("a sender that stopped mid-frame kept its connection")
	} else if !isTimeout(err) {
		t.Fatalf("expected a deadline error, got %v", err)
	}
}

// TestFrameConnLeavesIdleSendersAlone proves Done clears the deadline, so a
// sender that finished its frame is not disconnected for being quiet.
func TestFrameConnLeavesIdleSendersAlone(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	framed := NewFrameConn(server, 50*time.Millisecond)

	go func() {
		_, _ = client.Write([]byte("frame one"))
		time.Sleep(150 * time.Millisecond) // longer than the frame timeout
		_, _ = client.Write([]byte("frame two"))
	}()

	buf := make([]byte, 64)
	if _, err := framed.Read(buf); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	framed.Done()

	if _, err := framed.Read(buf); err != nil {
		t.Fatalf("an idle sender was disconnected between frames: %v", err)
	}
}

func isTimeout(err error) bool {
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}
