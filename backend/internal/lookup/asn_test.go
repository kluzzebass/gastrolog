package lookup

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

func TestASN_Suffixes(t *testing.T) {
	a := NewASN()
	defer a.Close()

	got := a.Suffixes()
	want := []string{"asn", "as_org"}
	if len(got) != len(want) {
		t.Fatalf("Suffixes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Suffixes()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestASN_LookupNilReader(t *testing.T) {
	a := NewASN()
	defer a.Close()

	if got := a.Lookup(context.Background(), "1.2.3.4"); got != nil {
		t.Errorf("Lookup with nil reader = %v, want nil", got)
	}
}

func TestASN_LookupInvalidIP(t *testing.T) {
	a := NewASN()
	defer a.Close()

	if got := a.Lookup(context.Background(), ""); got != nil {
		t.Errorf("Lookup empty = %v, want nil", got)
	}
	if got := a.Lookup(context.Background(), "not-an-ip"); got != nil {
		t.Errorf("Lookup garbage = %v, want nil", got)
	}
}

func TestASN_LoadBadPath(t *testing.T) {
	a := NewASN()
	defer a.Close()

	if _, err := a.Load("/nonexistent/path.mmdb"); err == nil {
		t.Error("Load bad path: expected error, got nil")
	}
}

func TestASN_LoadBadFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "bad.mmdb")
	if err := os.WriteFile(tmp, []byte("not a valid mmdb"), 0o644); err != nil {
		t.Fatal(err)
	}

	a := NewASN()
	defer a.Close()

	if _, err := a.Load(tmp); err == nil {
		t.Error("Load bad file: expected error, got nil")
	}
}

// generateTestASNDB creates a minimal ASN MMDB file in a temp directory and returns
// the path. The database contains:
//   - 8.8.8.8/32: ASN 15169 / GOOGLE
//   - 1.1.1.1/32: ASN 13335 / CLOUDFLARE
func generateTestASNDB(t *testing.T) string {
	t.Helper()

	tree, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType:            "Test-ASN",
		RecordSize:              24,
		IncludeReservedNetworks: true,
	})
	if err != nil {
		t.Fatalf("mmdbwriter.New: %v", err)
	}

	_, net8, _ := net.ParseCIDR("8.8.8.8/32")
	if err := tree.Insert(net8, mmdbtype.Map{
		"autonomous_system_number":       mmdbtype.Uint32(15169),
		"autonomous_system_organization": mmdbtype.String("GOOGLE"),
	}); err != nil {
		t.Fatalf("Insert 8.8.8.8: %v", err)
	}

	_, net1, _ := net.ParseCIDR("1.1.1.1/32")
	if err := tree.Insert(net1, mmdbtype.Map{
		"autonomous_system_number":       mmdbtype.Uint32(13335),
		"autonomous_system_organization": mmdbtype.String("CLOUDFLARE"),
	}); err != nil {
		t.Fatalf("Insert 1.1.1.1: %v", err)
	}

	path := filepath.Join(t.TempDir(), "test-asn.mmdb")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	if _, err := tree.WriteTo(f); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return path
}

func TestASN_LoadAndLookup(t *testing.T) {
	path := generateTestASNDB(t)

	a := NewASN()
	defer a.Close()

	info, err := a.Load(path)
	if err != nil {
		t.Fatalf("Load(%q): %v", path, err)
	}

	if info.DatabaseType != "Test-ASN" {
		t.Errorf("DatabaseType = %q, want %q", info.DatabaseType, "Test-ASN")
	}
	if info.BuildTime.IsZero() {
		t.Error("BuildTime is zero")
	}

	got := a.Lookup(context.Background(), "8.8.8.8")
	if got == nil {
		t.Fatal("Lookup(8.8.8.8) = nil, want non-nil result")
	}
	if got["asn"] != "AS15169" {
		t.Errorf("asn = %q, want %q", got["asn"], "AS15169")
	}
	if got["as_org"] != "GOOGLE" {
		t.Errorf("as_org = %q, want %q", got["as_org"], "GOOGLE")
	}

	got = a.Lookup(context.Background(), "1.1.1.1")
	if got == nil {
		t.Fatal("Lookup(1.1.1.1) = nil, want non-nil result")
	}
	if got["asn"] != "AS13335" {
		t.Errorf("asn = %q, want %q", got["asn"], "AS13335")
	}
	if got["as_org"] != "CLOUDFLARE" {
		t.Errorf("as_org = %q, want %q", got["as_org"], "CLOUDFLARE")
	}
}

func TestASN_ReaderSwap(t *testing.T) {
	path := generateTestASNDB(t)

	a := NewASN()
	defer a.Close()

	// Load twice — the first reader should be closed without error.
	if _, err := a.Load(path); err != nil {
		t.Fatalf("first Load: %v", err)
	}
	if _, err := a.Load(path); err != nil {
		t.Fatalf("second Load: %v", err)
	}

	// Should still work after swap.
	got := a.Lookup(context.Background(), "8.8.8.8")
	if got == nil {
		t.Fatal("Lookup after swap = nil")
	}
}

func TestASN_Miss(t *testing.T) {
	path := generateTestASNDB(t)

	a := NewASN()
	defer a.Close()

	if _, err := a.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// 10.0.0.1 (private IP) — complete miss, should return nil.
	if got := a.Lookup(context.Background(), "10.0.0.1"); got != nil {
		t.Errorf("Lookup(10.0.0.1) = %v, want nil", got)
	}
}
