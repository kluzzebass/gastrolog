package callgroup

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeduplication(t *testing.T) {
	var g Group[int]
	var calls atomic.Int32
	started := make(chan struct{})

	fn := func() error {
		calls.Add(1)
		close(started)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)

	// First caller starts the work.
	wg.Go(func() {
		errs[0] = <-g.DoChan(1, fn)
	})

	// Wait for fn to start, then pile on.
	<-started
	for i := 1; i < n; i++ {
		wg.Go(func() {
			errs[i] = <-g.DoChan(1, fn)
		})
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d got error: %v", i, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fn called %d times, want 1", got)
	}
}

func TestIndependentKeys(t *testing.T) {
	var g Group[int]
	var calls atomic.Int32

	fn := func() error {
		calls.Add(1)
		return nil
	}

	var wg sync.WaitGroup
	for _, key := range []int{1, 2, 3} {
		wg.Go(func() {
			<-g.DoChan(key, fn)
		})
	}

	wg.Wait()

	if got := calls.Load(); got != 3 {
		t.Errorf("fn called %d times, want 3", got)
	}
}

func TestWaiterReceivesResult(t *testing.T) {
	var g Group[int]
	started := make(chan struct{})

	fn := func() error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	// First caller.
	ch1 := g.DoChan(1, fn)
	<-started

	// Second caller joins.
	ch2 := g.DoChan(1, func() error {
		t.Error("second fn should not execute")
		return errors.New("unexpected")
	})

	err1 := <-ch1
	err2 := <-ch2

	if err1 != nil {
		t.Errorf("caller 1 got error: %v", err1)
	}
	if err2 != nil {
		t.Errorf("caller 2 got error: %v", err2)
	}
}

func TestErrorPropagation(t *testing.T) {
	var g Group[int]
	sentinel := errors.New("failed")
	started := make(chan struct{})

	ch1 := g.DoChan(1, func() error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		return sentinel
	})
	<-started

	ch2 := g.DoChan(1, func() error {
		t.Error("should not execute")
		return nil
	})

	err1 := <-ch1
	err2 := <-ch2

	if !errors.Is(err1, sentinel) {
		t.Errorf("caller 1: got %v, want %v", err1, sentinel)
	}
	if !errors.Is(err2, sentinel) {
		t.Errorf("caller 2: got %v, want %v", err2, sentinel)
	}
}

func TestReuseAfterCompletion(t *testing.T) {
	var g Group[int]
	var calls atomic.Int32

	fn := func() error {
		calls.Add(1)
		return nil
	}

	// First call completes.
	if err := <-g.DoChan(1, fn); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Second call for same key should trigger a new execution.
	if err := <-g.DoChan(1, fn); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("fn called %d times, want 2", got)
	}
}
