package scatterbox

import (
	"context"
	"encoding/json"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/waittest"
	"testing"
	"time"
)

func TestEmitsSequentialRecords(t *testing.T) {
	ing, err := NewIngester(glid.New(), map[string]string{
		"interval": "1ms",
		"burst":    "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan ingestion.IngesterMessage, 100)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()

	<-done // Run exited — no more sends on out

	var lastSeq uint64
	count := 0
	for len(out) > 0 {
		msg := <-out
		count++
		var body struct {
			Seq         uint64 `json:"seq"`
			GeneratedAt string `json:"generated_at"`
			Ingester    string `json:"ingester"`
		}
		if err := json.Unmarshal(msg.Raw, &body); err != nil {
			t.Fatalf("record %d: invalid JSON: %v", count, err)
		}
		if body.Seq <= lastSeq && count > 1 {
			t.Errorf("record %d: seq %d <= previous %d", count, body.Seq, lastSeq)
		}
		lastSeq = body.Seq

		if msg.Attrs["ingester_type"] != "scatterbox" {
			t.Errorf("record %d: ingester_type = %q, want scatterbox", count, msg.Attrs["ingester_type"])
		}
		if msg.Attrs["seq"] == "" {
			t.Errorf("record %d: missing seq attr", count)
		}
	}

	if count == 0 {
		t.Error("no records emitted")
	}
}

// TestFactoryEmbedsNodeID pins the multi-node differentiator behavior: when
// constructed via NewFactory(nodeID), every emitted record must include the
// node ID in both the JSON body and the Attrs map so records from different
// cluster nodes running the same scatterbox config are trivially attributable.
func TestFactoryEmbedsNodeID(t *testing.T) {
	const nodeID = "node-kappa"
	factory := NewFactory(nodeID)
	ing, err := factory(glid.New(), map[string]string{"interval": "1ms", "burst": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	out := make(chan ingestion.IngesterMessage, 64)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()
	<-done

	if len(out) == 0 {
		t.Fatal("no records emitted")
	}
	for len(out) > 0 {
		msg := <-out
		if got := msg.Attrs["node"]; got != nodeID {
			t.Errorf("Attrs[node] = %q, want %q", got, nodeID)
		}
		var body struct {
			Node string `json:"node"`
		}
		if err := json.Unmarshal(msg.Raw, &body); err != nil {
			t.Fatalf("body JSON: %v", err)
		}
		if body.Node != nodeID {
			t.Errorf("body.node = %q, want %q", body.Node, nodeID)
		}
	}
}

func TestBurstMode(t *testing.T) {
	ing, err := NewIngester(glid.New(), map[string]string{
		"interval": "10ms",
		"burst":    "5",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan ingestion.IngesterMessage, 100)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()

	// Read one full burst, however long the scheduler takes to start the
	// ingester and fire its first tick. Bounding the run with a small
	// context instead measures the machine: the context dies mid-burst on a
	// busy box and the partial count reads as a burst failure. The failsafe
	// stands in for a hung ingester, not for the measurement.
	deadline := time.After(waittest.Failsafe)
	for got := 0; got < 5; got++ {
		select {
		case <-out:
		case <-deadline:
			t.Fatalf("got %d of 5 burst records before the failsafe deadline", got)
		}
	}

	cancel()
	<-done // Run exited — no more sends on out
}
