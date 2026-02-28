// Command multirun runs multiple shell commands concurrently with colored,
// line-prefixed output. Useful for running a multi-node cluster in one terminal.
//
// Usage:
//
//	go run ./cmd/multirun [--name a,b,c] [--grace 60s] "cmd1" "cmd2" "cmd3"
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// ANSI colors readable on dark terminals.
var colors = []string{
	"\033[36m", // cyan
	"\033[35m", // magenta
	"\033[33m", // yellow
	"\033[32m", // green
	"\033[34m", // blue
	"\033[31m", // red
}

const reset = "\033[0m"

// lineWriter serializes colored, prefixed line output across goroutines.
type lineWriter struct {
	mu sync.Mutex
}

func (lw *lineWriter) writeTo(w *os.File, prefix, color, line string) {
	lw.mu.Lock()
	_, _ = fmt.Fprintf(w, "%s[%s]%s %s\n", color, prefix, reset, line) //nolint:gosec // writing to stdout/stderr, not HTTP
	lw.mu.Unlock()
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "multirun [flags] \"cmd1\" \"cmd2\" ...",
		Short: "Run multiple commands concurrently with colored output",
		Long: `multirun runs multiple shell commands concurrently, prefixing each line
of output with a colored label. On SIGINT/SIGTERM, it forwards the signal
to all children and waits for them to exit. If any child exits non-zero,
the rest are signaled to stop.`,
		Args:              cobra.MinimumNArgs(1),
		RunE:              runMulti,
		SilenceUsage:      true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}

	rootCmd.Flags().String("name", "", "comma-separated process names (default: 1,2,3,...)")
	rootCmd.Flags().Duration("grace", 60*time.Second, "grace period before SIGKILL after SIGTERM")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runMulti(cmd *cobra.Command, args []string) error {
	nameFlag, _ := cmd.Flags().GetString("name")
	grace, _ := cmd.Flags().GetDuration("grace")

	var names []string
	if nameFlag != "" {
		names = strings.Split(nameFlag, ",")
	}
	if len(names) == 0 {
		for i := range args {
			names = append(names, strconv.Itoa(i+1))
		}
	}
	if len(names) != len(args) {
		return fmt.Errorf("%d names but %d commands", len(names), len(args))
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		lw       lineWriter
		wg       sync.WaitGroup
		procs    []*os.Process
		procsMu  sync.Mutex
		exitCode int
		exitMu   sync.Mutex
	)

	failFast := func(code int) {
		exitMu.Lock()
		if code != 0 && exitCode == 0 {
			exitCode = code
		}
		exitMu.Unlock()
		if code != 0 {
			stop()
		}
	}

	for i, cmdStr := range args {
		wg.Add(1)
		go func(idx int, cmdStr string) {
			defer wg.Done()
			proc := runChild(&lw, names[idx], colors[idx%len(colors)], cmdStr)

			procsMu.Lock()
			if proc != nil {
				procs = append(procs, proc)
			}
			procsMu.Unlock()

			failFast(waitChild(&lw, names[idx], colors[idx%len(colors)], proc, cmdStr))
		}(i, cmdStr)
	}

	go forwardSignals(ctx, &procsMu, &procs, grace)

	wg.Wait()

	exitMu.Lock()
	code := exitCode
	exitMu.Unlock()
	if code != 0 {
		// Return a non-zero exit directly from main, not from RunE,
		// so Cobra doesn't print its own error message.
		os.Exit(code) //nolint:gocritic // intentional exit-after-defer; defers are cleanup-only
	}
	return nil
}

// runChild starts a child process and wires up output scanners.
// Returns the process (nil on start failure) so the caller can track it.
func runChild(lw *lineWriter, name, color, cmdStr string) *os.Process {
	child := exec.Command("sh", "-c", cmdStr) //nolint:gosec // user-provided commands are the purpose of this tool
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	child.Stdout = outW
	child.Stderr = errW

	if err := child.Start(); err != nil {
		lw.writeTo(os.Stderr, name, color, fmt.Sprintf("error: %v", err))
		return nil
	}

	// Stream stdout and stderr in separate goroutines.
	var scanWg sync.WaitGroup
	scanWg.Add(2)
	go scanLines(lw, &scanWg, outR, os.Stdout, name, color)
	go scanLines(lw, &scanWg, errR, os.Stderr, name, color)

	// Wait in a goroutine: close pipe writers when done so scanners finish.
	go func() {
		_ = child.Wait()
		_ = outW.Close()
		_ = errW.Close()
		scanWg.Wait()
	}()

	return child.Process
}

// waitChild waits for a process to exit and returns its exit code.
func waitChild(lw *lineWriter, name, color string, proc *os.Process, _ string) int {
	if proc == nil {
		return 1
	}

	state, err := proc.Wait()
	if err == nil && state.Success() {
		lw.writeTo(os.Stderr, name, color, "exited")
		return 0
	}

	code := 1
	switch {
	case state != nil:
		code = state.ExitCode()
	default:
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		}
	}
	lw.writeTo(os.Stderr, name, color, fmt.Sprintf("exited with code %d", code))
	return code
}

func scanLines(lw *lineWriter, wg *sync.WaitGroup, r *io.PipeReader, dest *os.File, name, color string) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		lw.writeTo(dest, name, color, scanner.Text())
	}
}

func forwardSignals(ctx context.Context, procsMu *sync.Mutex, procs *[]*os.Process, grace time.Duration) {
	<-ctx.Done()

	procsMu.Lock()
	snapshot := make([]*os.Process, len(*procs))
	copy(snapshot, *procs)
	procsMu.Unlock()

	for _, p := range snapshot {
		_ = syscall.Kill(-p.Pid, syscall.SIGTERM)
	}

	time.AfterFunc(grace, func() {
		for _, p := range snapshot {
			_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
		}
	})
}
