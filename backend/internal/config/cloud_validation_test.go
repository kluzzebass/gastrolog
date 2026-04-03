package config

import (
	"strings"
	"testing"
)

func TestValidateTransitions_S3OrderingValid(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{AfterDays: 30, StorageClass: "STANDARD_IA"},
		{AfterDays: 90, StorageClass: "GLACIER"},
		{AfterDays: 365, StorageClass: "DEEP_ARCHIVE"},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
}

func TestValidateTransitions_S3OrderingReversed(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{AfterDays: 30, StorageClass: "DEEP_ARCHIVE"},
		{AfterDays: 90, StorageClass: "GLACIER"},
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
		{AfterDays: 10, StorageClass: "GLACIER"},
		{AfterDays: 40, StorageClass: "DEEP_ARCHIVE"},
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
		{AfterDays: 60, StorageClass: "GLACIER"},
		{AfterDays: 90, StorageClass: ""},
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
		{AfterDays: 30, StorageClass: "Cool"},
		{AfterDays: 90, StorageClass: "Cold"},
		{AfterDays: 365, StorageClass: "Archive"},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings for valid Azure chain, got %v", warnings)
	}
}

func TestValidateTransitions_GCSArchiveMinDuration(t *testing.T) {
	t.Parallel()
	// GCS Archive has 365-day minimum.
	warnings := ValidateTransitions("gcs", []CloudStorageTransition{
		{AfterDays: 30, StorageClass: "ARCHIVE"},
		{AfterDays: 100, StorageClass: ""},
	})
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "365") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 365-day warning for GCS Archive, got %v", warnings)
	}
}

func TestValidateTransitions_DaysNotIncreasing(t *testing.T) {
	t.Parallel()
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{AfterDays: 90, StorageClass: "GLACIER"},
		{AfterDays: 30, StorageClass: "DEEP_ARCHIVE"},
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
	// Single step with no next transition — duration is unbounded.
	warnings := ValidateTransitions("s3", []CloudStorageTransition{
		{AfterDays: 30, StorageClass: "GLACIER"},
	})
	if len(warnings) > 0 {
		t.Errorf("expected no warnings for single step, got %v", warnings)
	}
}
