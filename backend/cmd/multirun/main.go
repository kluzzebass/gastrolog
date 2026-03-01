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
	_, _ = fmt.Fprintf(w, "%s[%s]%s %s\n", color, prefix, reset, line)
	lw.mu.Unlock()
}

// childProc tracks a running child process alongside its display metadata
// and a channel that is closed once the process has exited and its output
// has been fully flushed.
type childProc struct {
	name     string
	color    string
	proc     *os.Process   // nil if start failed
	done     chan struct{}  // closed after exit + output flush
	exitCode int           // valid after done is closed
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "multirun [flags] \"cmd1\" \"cmd2\" ...",
		Short: "Run multiple commands concurrently with colored output",
		Long: `multirun runs multiple shell commands concurrently, prefixing each line
of output with a colored label. On SIGINT/SIGTERM, it signals children one
at a time in reverse order, waiting for each to exit before signaling the
next. This allows clustered services to shut down gracefully while quorum
is still available.`,
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

	// Start children sequentially so children[i] always corresponds to
	// names[i]. They all run concurrently once started.
	children := make([]*childProc, len(args))
	var wg sync.WaitGroup
	for i, cmdStr := range args {
		cp := startChild(&lw, names[i], colors[i%len(colors)], cmdStr)
		children[i] = cp
		wg.Add(1)
		go func(cp *childProc) {
			defer wg.Done()
			<-cp.done
			failFast(cp.exitCode)
		}(cp)
	}

	go forwardSignals(ctx, &lw, children, grace)

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

// startChild starts a child process, wires up output scanners, and returns
// a childProc whose done channel closes when the process exits and output
// is fully flushed. Only one goroutine calls Wait on the underlying process,
// eliminating the race that occurs when multiple callers reap the same child.
func startChild(lw *lineWriter, name, color, cmdStr string) *childProc {
	cp := &childProc{
		name:  name,
		color: color,
		done:  make(chan struct{}),
	}

	child := exec.Command("sh", "-c", cmdStr) //nolint:gosec // user-provided commands are the purpose of this tool
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	child.Stdout = outW
	child.Stderr = errW

	if err := child.Start(); err != nil {
		lw.writeTo(os.Stderr, name, color, fmt.Sprintf("error: %v", err))
		cp.exitCode = 1
		close(cp.done)
		return cp
	}
	cp.proc = child.Process

	// Stream stdout and stderr in separate goroutines.
	var scanWg sync.WaitGroup
	scanWg.Add(2)
	go scanLines(lw, &scanWg, outR, os.Stdout, name, color)
	go scanLines(lw, &scanWg, errR, os.Stderr, name, color)

	// Single Wait goroutine: reaps the process, closes pipes so scanners
	// finish, then signals completion via cp.done.
	go func() {
		err := child.Wait()
		_ = outW.Close()
		_ = errW.Close()
		scanWg.Wait()

		if err == nil {
			lw.writeTo(os.Stderr, name, color, "exited")
		} else {
			code := 1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				code = exitErr.ExitCode()
			}
			cp.exitCode = code
			lw.writeTo(os.Stderr, name, color, fmt.Sprintf("exited with code %d", code))
		}
		close(cp.done)
	}()

	return cp
}

func scanLines(lw *lineWriter, wg *sync.WaitGroup, r *io.PipeReader, dest *os.File, name, color string) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		lw.writeTo(dest, name, color, scanner.Text())
	}
}

// forwardSignals shuts down children one at a time in reverse order.
// This lets clustered services (e.g. Raft nodes) maintain quorum while
// peers shut down, allowing each to complete a clean snapshot/drain.
func forwardSignals(ctx context.Context, lw *lineWriter, children []*childProc, grace time.Duration) {
	<-ctx.Done()

	// Shut down in reverse order (last started → first stopped).
	for i := len(children) - 1; i >= 0; i-- {
		cp := children[i]
		if cp.proc == nil {
			continue
		}

		_ = syscall.Kill(-cp.proc.Pid, syscall.SIGTERM)

		select {
		case <-cp.done:
			lw.writeTo(os.Stderr, "multirun", "\033[90m", cp.name+" stopped, continuing shutdown...")
		case <-time.After(grace):
			_ = syscall.Kill(-cp.proc.Pid, syscall.SIGKILL)
			<-cp.done
		}
	}
}
