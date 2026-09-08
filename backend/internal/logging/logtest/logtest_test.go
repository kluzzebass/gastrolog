package logtest

import (
	"testing"
	"time"
)

func TestRecorderContainsRequiresEverySubstringOnOneLine(t *testing.T) {
	t.Parallel()
	logger, rec := New()

	logger.Info("first", "node", "a")
	logger.Info("second", "vault", "b")

	if !rec.Contains("first", "node=a") {
		t.Errorf("expected the first line to match: %s", rec)
	}
	if rec.Contains("first", "vault=b") {
		t.Errorf("matched substrings spread across two lines: %s", rec)
	}
}

func TestPanickingLoggerFiresOnlyTheRequestedNumberOfTimes(t *testing.T) {
	t.Parallel()
	logger, rec := NewPanicking("boom", 1)

	panics := 0
	for range 3 {
		func() {
			defer func() {
				if recover() != nil {
					panics++
				}
			}()
			logger.Info("boom")
		}()
	}

	if panics != 1 {
		t.Errorf("expected exactly 1 panic, got %d", panics)
	}
	if !rec.Contains("boom") {
		t.Errorf("the triggering line was not recorded: %s", rec)
	}
}

func TestPanickingLoggerIgnoresOtherMessages(t *testing.T) {
	t.Parallel()
	logger, _ := NewPanicking("boom", 1)

	defer func() {
		if v := recover(); v != nil {
			t.Errorf("panicked on an unrelated message: %v", v)
		}
	}()
	logger.Info("something else")
}

func TestWaitReturnsFalseWhenNothingMatches(t *testing.T) {
	t.Parallel()
	_, rec := New()

	if rec.Wait(10*time.Millisecond, "never logged") {
		t.Error("Wait claimed a match that was never logged")
	}
}

func TestWaitReturnsOnceTheLineArrives(t *testing.T) {
	t.Parallel()
	logger, rec := New()

	go func() { logger.Warn("late line", "node", "a") }()

	if !rec.Wait(5*time.Second, "late line", "node=a") {
		t.Errorf("Wait missed a line written from another goroutine: %s", rec)
	}
}
