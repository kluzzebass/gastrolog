package orchestrator

import (
	"errors"
	"testing"

	"gastrolog/internal/orchestrator/pipeline"
)

// A direct submit turned away by a writer swap is retried once the swap is
// over, and only then: the barrier runs between the two attempts.
func TestSubmitAcrossReregistrationRetriesAfterTheSwap(t *testing.T) {
	var steps []string
	attempts := 0
	err := submitAcrossReregistration(func() error {
		attempts++
		steps = append(steps, "submit")
		if attempts == 1 {
			return pipeline.ErrVaultNotRegistered
		}
		return nil
	}, func() { steps = append(steps, "barrier") })
	if err != nil {
		t.Fatalf("submit after the swap: %v", err)
	}
	if got := len(steps); got != 3 || steps[0] != "submit" || steps[1] != "barrier" || steps[2] != "submit" {
		t.Fatalf("steps = %v, want submit, barrier, submit", steps)
	}
}

// A vault still unregistered after the swap is reported as such, and any other
// error is returned at once, untouched.
func TestSubmitAcrossReregistrationDoesNotHideOtherOutcomes(t *testing.T) {
	err := submitAcrossReregistration(func() error { return pipeline.ErrVaultNotRegistered }, func() {})
	if !errors.Is(err, pipeline.ErrVaultNotRegistered) {
		t.Fatalf("vault absent after the swap: got %v", err)
	}
	other := errors.New("admission refused")
	barriers := 0
	err = submitAcrossReregistration(func() error { return other }, func() { barriers++ })
	if !errors.Is(err, other) || barriers != 0 {
		t.Fatalf("other error: got %v after %d barriers, want it returned with no retry", err, barriers)
	}
}
