package main

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestEnvBoolDefaultTrue(t *testing.T) {
	tests := []struct {
		env  string
		want bool
	}{
		{"", true},
		{"true", true},
		{"1", true},
		{"yes", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"FALSE", false},
	}
	for _, tc := range tests {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("GLOG_SEGMENT_HOT_PATH_FSYNC", tc.env)
			if got := envBoolDefaultTrue("GLOG_SEGMENT_HOT_PATH_FSYNC"); got != tc.want {
				t.Fatalf("env %q: got %v want %v", tc.env, got, tc.want)
			}
		})
	}
}

func TestResolveSegmentHotPathFsync(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Bool("segment-hot-path-fsync", true, "")

	t.Setenv("GLOG_SEGMENT_HOT_PATH_FSYNC", "false")
	if got := resolveSegmentHotPathFsync(cmd); got {
		t.Fatal("expected env false when flag unchanged")
	}

	if err := cmd.Flags().Set("segment-hot-path-fsync", "true"); err != nil {
		t.Fatal(err)
	}
	if got := resolveSegmentHotPathFsync(cmd); !got {
		t.Fatal("expected explicit flag true to win over env false")
	}
}
