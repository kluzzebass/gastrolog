package config

import (
	"strings"
	"testing"
)

func TestValidateTransitions_S3OrderingValid(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{After: "30d", StorageClass: "STANDARD_IA"},
		{After: "90d", StorageClass: "GLACIER"},
		{After: "365d", StorageClass: "DEEP_ARCHIVE"},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
}

func TestValidateTransitions_S3OrderingReversed(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{After: "30d", StorageClass: "DEEP_ARCHIVE"},
		{After: "90d", StorageClass: "GLACIER"},
	})
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "cannot follow") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected ordering warning, got %v", warnings)
	}
}

func TestValidateTransitions_MinDurationWarning(t *testing.T) {
	t.Parallel()
	// GLACIER has 90-day minimum, but data only stays 30 days.
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{After: "10d", StorageClass: "GLACIER"},
		{After: "40d", StorageClass: "DEEP_ARCHIVE"},
	})
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "minimum storage duration") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected minimum duration warning, got %v", warnings)
	}
}

func TestValidateTransitions_ExpiryAfterArchive(t *testing.T) {
	t.Parallel()
	// Delete after 30 days in GLACIER (minimum 90 days).
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{After: "60d", StorageClass: "GLACIER"},
		{After: "90d", StorageClass: ""},
	})
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "early deletion") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected early deletion warning, got %v", warnings)
	}
}

func TestValidateTransitions_AzureValid(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("azure", []CloudStorageTransition{
		{After: "30d", StorageClass: "Cool"},
		{After: "90d", StorageClass: "Cold"},
		{After: "365d", StorageClass: "Archive"},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings for valid Azure chain, got %v", warnings)
	}
}

func TestValidateTransitions_GCSArchiveMinDuration(t *testing.T) {
	t.Parallel()
	// GCS Archive has 365-day minimum.
	warnings := ValidateTransitions("gcs", []CloudStorageTransition{
		{After: "30d", StorageClass: "ARCHIVE"},
		{After: "100d", StorageClass: ""},
	})
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "52w1d") || strings.Contains(w, "minimum storage duration") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected minimum duration warning for GCS Archive, got %v", warnings)
	}
}

func TestValidateTransitions_DurationsNotIncreasing(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{After: "90d", StorageClass: "GLACIER"},
		{After: "30d", StorageClass: "DEEP_ARCHIVE"},
	})
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "must be after") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected ordering warning, got %v", warnings)
	}
}

func TestValidateTransitions_Empty(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", nil)
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for empty chain, got %v", warnings)
	}
}

func TestValidateTransitions_SingleStepNoWarning(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{After: "30d", StorageClass: "GLACIER"},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings for single step, got %v", warnings)
	}
}

func TestValidateTransitions_SubDayDurations(t *testing.T) {
	t.Parallel()
	// Memory provider testing with sub-day durations.
	warnings := ValidateTransitions("memory", []CloudStorageTransition{
		{After: "0s", StorageClass: "cold"},
		{After: "30s", StorageClass: "deep-freeze"},
		{After: "1m", StorageClass: ""},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings for memory sub-day chain, got %v", warnings)
	}
}
