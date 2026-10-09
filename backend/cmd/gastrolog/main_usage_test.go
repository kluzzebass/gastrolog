package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

type cliRun struct {
	err    error
	output string
}

// runCLI executes the production root command in-process. It deliberately
// leaves SilenceUsage and SilenceErrors at their defaults: what is printed is
// exactly what an operator sees.
func runCLI(t *testing.T, args ...string) cliRun {
	t.Helper()
	t.Setenv("GASTROLOG_TOKEN", "")
	root := newRootCommand(slog.New(slog.DiscardHandler), nil, nil, nil)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append(args, "--home", t.TempDir()))
	err := root.Execute()
	return cliRun{err: err, output: out.String()}
}

func refusingServer(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "refused", http.StatusServiceUnavailable)
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}

func requireErrorPrintedOnce(t *testing.T, r cliRun) {
	t.Helper()
	if r.err == nil {
		t.Fatalf("expected an error, got none; output:\n%s", r.output)
	}
	if got := strings.Count(r.output, "Error: "); got != 1 {
		t.Fatalf("error line printed %d times, want 1; output:\n%s", got, r.output)
	}
	if !strings.Contains(r.output, r.err.Error()) {
		t.Fatalf("output does not carry the error %q; output:\n%s", r.err, r.output)
	}
}

func TestCLIRuntimeErrorPrintsErrorWithoutUsage(t *testing.T) {
	r := runCLI(t, "cluster", "remove-node", "node-1", "--addr", refusingServer(t))
	requireErrorPrintedOnce(t, r)
	if strings.Contains(r.output, "Usage:") {
		t.Fatalf("runtime error printed the usage block; output:\n%s", r.output)
	}
}

func TestCLIInvocationErrorsPrintUsage(t *testing.T) {
	addr := refusingServer(t)
	cases := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"cluster", "remove-node", "node-1", "--addr", addr, "--no-such-flag"}},
		{"bad flag value", []string{"cluster", "remove-node", "node-1", "--addr", addr, "--force=maybe"}},
		{"too few args", []string{"cluster", "remove-node", "--addr", addr}},
		{"too many args", []string{"cluster", "remove-node", "node-1", "node-2", "--addr", addr}},
		{"missing required flag", []string{"user", "create", "--username", "alice", "--addr", addr}},
		{"mutually exclusive flags", []string{"config", "import", "--merge", "--replace", "--addr", addr}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := runCLI(t, tc.args...)
			requireErrorPrintedOnce(t, r)
			if !strings.Contains(r.output, "Usage:") {
				t.Fatalf("invocation error did not print the usage block; output:\n%s", r.output)
			}
		})
	}
}

// Without EnableTraverseRunHooks cobra runs only the nearest persistent
// pre-run hook, so a subcommand defining its own would bypass the root's
// usage handling for its whole subtree.
func TestOnlyRootDefinesPersistentPreRun(t *testing.T) {
	root := newRootCommand(slog.New(slog.DiscardHandler), nil, nil, nil)
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		for _, sub := range c.Commands() {
			if sub.PersistentPreRun != nil || sub.PersistentPreRunE != nil {
				t.Errorf("%q defines a persistent pre-run hook that shadows the root's", sub.CommandPath())
			}
			walk(sub)
		}
	}
	walk(root)
}
