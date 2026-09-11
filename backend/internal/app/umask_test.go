package app

// Raft snapshots carry the whole replicated config — the JWT signing
// secret, TLS private keys, cloud credentials — and hashicorp/raft creates
// them with os.Create, which nothing in this tree can pass a mode to. The
// umask is what covers those, so it is what gets tested.

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestRestrictFileCreationMakesLibraryFilesOwnerOnly(t *testing.T) {
	// Not parallel: the umask is process-wide.
	previous := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(previous) })

	dir := t.TempDir()

	// What a library that cannot be passed a mode produces, before.
	before := filepath.Join(dir, "before.snap")
	f, err := os.Create(before) //nolint:gosec // G304: path is a test temp dir
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	beforeInfo, err := os.Stat(before)
	if err != nil {
		t.Fatal(err)
	}
	if beforeInfo.Mode().Perm()&0o077 == 0 {
		t.Skip("this environment's umask already restricts creation; the test cannot show the change")
	}

	restrictFileCreation()

	after := filepath.Join(dir, "after.snap")
	f, err = os.Create(after) //nolint:gosec // G304: path is a test temp dir
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	afterInfo, err := os.Stat(after)
	if err != nil {
		t.Fatal(err)
	}
	if mode := afterInfo.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("a file created after restrictFileCreation has mode %04o; "+
			"group and other must have nothing", mode)
	}
}
