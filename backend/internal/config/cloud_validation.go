package config

import "fmt"

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

// Minimum storage durations (days) before early deletion charges stop.
var minStorageDays = map[string]uint32{
	// S3
	"STANDARD_IA":  30,
	"ONEZONE_IA":   30,
	"GLACIER_IR":   90,
	"GLACIER":      90,
	"DEEP_ARCHIVE": 180,
	// Azure
	"Cool":    30,
	"Cold":    90,
	"Archive": 180,
	// GCS
	"NEARLINE": 30,
	"COLDLINE": 90,
	"ARCHIVE":  365,
}

// ValidateTransitions checks a transition chain for ordering violations and
// minimum duration warnings. Returns a list of human-readable warnings.
// These are advisory, not errors — the config is still saved.
func ValidateTransitions(provider string, transitions []CloudStorageTransition) []string {
	if len(transitions) == 0 {
		return nil
	}
	var warnings []string
	warnings = append(warnings, validateDaysOrdering(transitions)...)
	if provider == "s3" {
		warnings = append(warnings, validateS3ClassOrdering(transitions)...)
	}
	warnings = append(warnings, validateMinDurations(transitions)...)
	warnings = append(warnings, validateExpiryAfterArchive(transitions)...)
	return warnings
}

func validateDaysOrdering(transitions []CloudStorageTransition) []string {
	var warnings []string
	for i := 1; i < len(transitions); i++ {
		if transitions[i].AfterDays <= transitions[i-1].AfterDays {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d (%d days) must be after transition %d (%d days)",
				i+1, transitions[i].AfterDays, i, transitions[i-1].AfterDays))
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

func validateMinDurations(transitions []CloudStorageTransition) []string {
	var warnings []string
	for i, t := range transitions {
		if t.StorageClass == "" || i+1 >= len(transitions) {
			continue
		}
		minDays, hasMin := minStorageDays[t.StorageClass]
		if !hasMin {
			continue
		}
		durationInClass := transitions[i+1].AfterDays - t.AfterDays
		if durationInClass < minDays {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d: %q has a %d-day minimum storage duration, but data will only stay %d days before the next transition — early deletion charges apply",
				i+1, t.StorageClass, minDays, durationInClass))
		}
	}
	return warnings
}

func validateExpiryAfterArchive(transitions []CloudStorageTransition) []string {
	var warnings []string
	for i := 1; i < len(transitions); i++ {
		t := transitions[i]
		if t.StorageClass != "" {
			continue // not a delete step
		}
		prev := transitions[i-1]
		minDays, hasMin := minStorageDays[prev.StorageClass]
		if !hasMin {
			continue
		}
		durationInPrev := t.AfterDays - prev.AfterDays
		if durationInPrev < minDays {
			warnings = append(warnings, fmt.Sprintf(
				"transition %d: deleting after %d days in %q (minimum %d days) — early deletion charges apply",
				i+1, durationInPrev, prev.StorageClass, minDays))
		}
	}
	return warnings
}
