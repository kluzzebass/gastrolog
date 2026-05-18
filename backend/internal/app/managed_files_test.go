package app

import (
	"context"
	"io"
	"log/slog"
	"testing"

	sysmem "gastrolog/internal/system/memory"
)

// TestStartManagedFilesReconcile_RegistersOperatorVisibleJob verifies
// the drift check ships as a proper scheduled job: name + cron set,
// non-empty Describe text so the inspector shows context to the
// operator, and the captured task drives a real tick without panic
// against an empty manifest (no transferrer call required).
func TestStartManagedFilesReconcile_RegistersOperatorVisibleJob(t *testing.T) {
	t.Parallel()
	mgr := &managedFileManager{
		cfgStore:   sysmem.NewStore(),
		fileExists: func(string) bool { return true },
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	sched := &fakeScheduler{}

	if err := startManagedFilesReconcile(context.Background(), sched, mgr); err != nil {
		t.Fatalf("startManagedFilesReconcile: %v", err)
	}
	if sched.addJobName != managedFilesReconcileJobName {
		t.Errorf("AddJob name: got %q, want %q", sched.addJobName, managedFilesReconcileJobName)
	}
	if sched.addJobCron != managedFilesReconcileSchedule {
		t.Errorf("AddJob cron: got %q, want %q", sched.addJobCron, managedFilesReconcileSchedule)
	}
	if sched.describeMessage == "" {
		t.Error("Describe message empty — operator inspector will show no context")
	}

	// Run the captured task — empty manifest means reconcileOnce
	// short-circuits before any transferrer call. Must not panic.
	if task, ok := sched.addJobTaskFn.(func()); ok {
		task()
	} else {
		t.Fatalf("expected captured task of type func(), got %T", sched.addJobTaskFn)
	}
}

// TestStartManagedFilesReconcile_PropagatesAddJobError verifies the
// caller sees an AddJob failure (e.g. duplicate name).
func TestStartManagedFilesReconcile_PropagatesAddJobError(t *testing.T) {
	t.Parallel()
	mgr := &managedFileManager{
		cfgStore:   sysmem.NewStore(),
		fileExists: func(string) bool { return true },
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	sched := &fakeScheduler{addJobErr: errFakeMember}

	if err := startManagedFilesReconcile(context.Background(), sched, mgr); err == nil {
		t.Fatal("expected AddJob error to propagate")
	}
}
