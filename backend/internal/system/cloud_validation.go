package system

import (
	"fmt"
	"time"
)

// S3 storage class ordering — transitions must move forward, not backward.
var s3ClassOrder = map[string]int{
	"STANDARD":            0,
	"STANDARD_IA":         1,
	"ONEZONE_IA":          1,
	"INTELLIGENT_TIERING": 2,
	"GLACIER_IR":          3,
	"GLACIER":             4,
	"DEEP_ARCHIVE":        5,
}

// Minimum storage durations before early deletion charges stop.
var minStorageDuration = map[string]time.Duration{
	// S3
	"STANDARD_IA":  30 * 24 * time.Hour,
	"ONEZONE_IA":   30 * 24 * time.Hour,
	"GLACIER_IR":   90 * 24 * time.Hour,
	"GLACIER":      90 * 24 * time.Hour,
	"DEEP_ARCHIVE": 180 * 24 * time.Hour,
	// Azure
	"Cool":    30 * 24 * time.Hour,
	"Cold":    90 * 24 * time.Hour,
	"Archive": 180 * 24 * time.Hour,
	// GCS
	"NEARLINE": 30 * 24 * time.Hour,
	"COLDLINE": 90 * 24 * time.Hour,
	"ARCHIVE":  365 * 24 * time.Hour,
}

// ValidateTransitions checks a transition chain for ordering violations and
// minimum duration warnings. Returns a list of human-readable warnings.
// These are advisory, not errors — the config is still saved.
func ValidateTransitions(provider string, transitions []CloudStorageTransition) []string {
	if len(transitions) == 0 {
		return nil
	}
	// Parse all durations upfront.
	durations := make([]time.Duration, len(transitions))
	var warnings []string
	for i, t := range transitions {
		d, err := ParseDuration(t.After)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("transition %d: invalid duration %q: %v", i+1, t.After, err))
			continue
		}
		durations[i] = d
	}

	warnings = append(warnings, validateDurationOrdering(transitions, durations)...)
	if provider == "s3" {
		warnings = append(warnings, validateS3ClassOrdering(transitions)...)
	}
	warnings = append(warnings, validateMinDurations(transitions, durations)...)
	warnings = append(warnings, validateExpiryAfterArchive(transitions, durations)...)
	return warnings
}

func validateDurationOrdering(transitions []CloudStorageTransition, durations []time.Duration) []string {
	var warnings []string
	for i := 1; i < len(transitions); i++ {
		if durations[i] <= durations[i-1] {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d (%s) must be after transition %d (%s)",
				i+1, transitions[i].After, i, transitions[i-1].After))
		}
	}
	return warnings
}

func validateS3ClassOrdering(transitions []CloudStorageTransition) []string {
	var warnings []string
	prevOrder := -1
	for i, t := range transitions {
		if t.StorageClass == "" {
			continue
		}
		order, known := s3ClassOrder[t.StorageClass]
		if !known {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d: unknown S3 storage class %q", i+1, t.StorageClass))
			continue
		}
		if order <= prevOrder {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d: S3 class %q cannot follow a higher-tier class (must move forward: IA → GLACIER_IR → GLACIER → DEEP_ARCHIVE)",
				i+1, t.StorageClass))
		}
		prevOrder = order
	}
	return warnings
}

func validateMinDurations(transitions []CloudStorageTransition, durations []time.Duration) []string {
	var warnings []string
	for i, t := range transitions {
		if t.StorageClass == "" || i+1 >= len(transitions) {
			continue
		}
		minDur, hasMin := minStorageDuration[t.StorageClass]
		if !hasMin {
			continue
		}
		durationInClass := durations[i+1] - durations[i]
		if durationInClass < minDur {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d: %q has a minimum storage duration of %s, but data will only stay %s before the next transition — early deletion charges apply",
				i+1, t.StorageClass, FormatDuration(minDur), FormatDuration(durationInClass)))
		}
	}
	return warnings
}

func validateExpiryAfterArchive(transitions []CloudStorageTransition, durations []time.Duration) []string {
	var warnings []string
	for i := 1; i < len(transitions); i++ {
		t := transitions[i]
		if t.StorageClass != "" {
			continue // not a delete step
		}
		prev := transitions[i-1]
		minDur, hasMin := minStorageDuration[prev.StorageClass]
		if !hasMin {
			continue
		}
		durationInPrev := durations[i] - durations[i-1]
		if durationInPrev < minDur {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d: deleting after %s in %q (minimum %s) — early deletion charges apply",
				i+1, FormatDuration(durationInPrev), prev.StorageClass, FormatDuration(minDur)))
		}
	}
	return warnings
}
