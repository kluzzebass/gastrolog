package main

import (
	"os"
	"strings"
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

// A secret on the command line is readable by every other process on the
// host, so these four arrive through the environment instead. The flags
// remain for anyone already using them.
func TestSecretFlagPrefersTheEnvironment(t *testing.T) {
	const env = "GASTROLOG_TEST_SECRET"

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().String("the-secret", "", "")
		return cmd
	}

	t.Run("environment when the flag is unset", func(t *testing.T) {
		t.Setenv(env, "from-env")
		if got := secretFlag(newCmd(), "the-secret", env); got != "from-env" {
			t.Errorf("got %q, want the environment value", got)
		}
	})

	t.Run("flag wins when explicitly set", func(t *testing.T) {
		t.Setenv(env, "from-env")
		cmd := newCmd()
		if err := cmd.Flags().Set("the-secret", "from-flag"); err != nil {
			t.Fatal(err)
		}
		if got := secretFlag(cmd, "the-secret", env); got != "from-flag" {
			t.Errorf("got %q, want the explicit flag to win", got)
		}
	})

	t.Run("empty environment is not a value", func(t *testing.T) {
		t.Setenv(env, "   ")
		cmd := newCmd()
		if err := cmd.Flags().Set("the-secret", "from-flag"); err != nil {
			t.Fatal(err)
		}
		if got := secretFlag(cmd, "the-secret", env); got != "from-flag" {
			t.Errorf("got %q, want the flag when the environment holds only blanks", got)
		}
	})

	t.Run("neither yields empty", func(t *testing.T) {
		t.Setenv(env, "")
		if got := secretFlag(newCmd(), "the-secret", env); got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})
}

// The entrypoint must not put any of the four on the command line, or the
// binary reading them from the environment buys nothing.
func TestEntrypointPassesNoSecretOnTheCommandLine(t *testing.T) {
	script, err := os.ReadFile("../../../docker-entrypoint.sh")
	if err != nil {
		t.Skipf("entrypoint not readable from here: %v", err)
	}
	// Guard the guard: a path change or an unreadable file would make every
	// assertion below pass for the wrong reason.
	if !strings.Contains(string(script), "--listen $") {
		t.Fatal("the entrypoint does not look like the entrypoint; the assertions below prove nothing")
	}
	for _, flag := range []string{
		"--join-token",
		"--bootstrap-token-serve-secret",
		"--bootstrap-token-secret",
		"--initial-admin-password",
	} {
		if strings.Contains(string(script), flag+" $") {
			t.Errorf("the entrypoint passes %s on the command line, where ps exposes it", flag)
		}
	}
}
