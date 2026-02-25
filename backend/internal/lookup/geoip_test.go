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

func TestGeoIP_Suffixes(t *testing.T) {
	g := NewGeoIP()
	defer g.Close()

	got := g.Suffixes()
	want := []string{"country", "city", "subdivision", "latitude", "longitude", "timezone", "accuracy_radius"}
	if len(got) != len(want) {
		t.Fatalf("Suffixes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Suffixes()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestGeoIP_LookupNilReader(t *testing.T) {
	g := NewGeoIP()
	defer g.Close()

	// Before any Load, Lookup should return nil.
	if got := g.Lookup(context.Background(), "1.2.3.4"); got != nil {
		t.Errorf("Lookup with nil reader = %v, want nil", got)
	}
}

func TestGeoIP_LookupInvalidIP(t *testing.T) {
	g := NewGeoIP()
	defer g.Close()

	if got := g.Lookup(context.Background(), ""); got != nil {
		t.Errorf("Lookup empty = %v, want nil", got)
	}
	if got := g.Lookup(context.Background(), "not-an-ip"); got != nil {
		t.Errorf("Lookup garbage = %v, want nil", got)
	}
}

func TestGeoIP_LoadBadPath(t *testing.T) {
	g := NewGeoIP()
	defer g.Close()

	if _, err := g.Load("/nonexistent/path.mmdb"); err == nil {
		t.Error("Load bad path: expected error, got nil")
	}
}

func TestGeoIP_LoadBadFile(t *testing.T) {
	// Create a temp file that is not a valid MMDB.
	tmp := filepath.Join(t.TempDir(), "bad.mmdb")
	if err := os.WriteFile(tmp, []byte("not a valid mmdb"), 0o644); err != nil {
		t.Fatal(err)
	}

	g := NewGeoIP()
	defer g.Close()

	if _, err := g.Load(tmp); err == nil {
		t.Error("Load bad file: expected error, got nil")
	}
}

// generateTestMMDB creates a minimal MMDB file in a temp directory and returns
// the path. The database contains:
//   - 8.8.8.8/32: full record (country, city, subdivision, location)
//   - 1.1.1.1/32: country=AU only (tests partial data)
func generateTestMMDB(t *testing.T) string {
	t.Helper()

	tree, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType:            "Test-GeoIP",
		RecordSize:              24,
		IncludeReservedNetworks: true,
	})
	if err != nil {
		t.Fatalf("mmdbwriter.New: %v", err)
	}

	// 8.8.8.8/32 — full record: country + city + subdivision + location.
	_, net8, _ := net.ParseCIDR("8.8.8.8/32")
	if err := tree.Insert(net8, mmdbtype.Map{
		"country": mmdbtype.Map{
			"iso_code": mmdbtype.String("US"),
		},
		"city": mmdbtype.Map{
			"names": mmdbtype.Map{
				"en": mmdbtype.String("Mountain View"),
			},
		},
		"subdivisions": mmdbtype.Slice{
			mmdbtype.Map{
				"iso_code": mmdbtype.String("CA"),
				"names": mmdbtype.Map{
					"en": mmdbtype.String("California"),
				},
			},
		},
		"location": mmdbtype.Map{
			"latitude":        mmdbtype.Float64(37.386),
			"longitude":       mmdbtype.Float64(-122.0838),
			"time_zone":       mmdbtype.String("America/Los_Angeles"),
			"accuracy_radius": mmdbtype.Uint16(1000),
		},
	}); err != nil {
		t.Fatalf("Insert 8.8.8.8: %v", err)
	}

	// 1.1.1.1/32 — partial record: country only.
	_, net1, _ := net.ParseCIDR("1.1.1.1/32")
	if err := tree.Insert(net1, mmdbtype.Map{
		"country": mmdbtype.Map{
			"iso_code": mmdbtype.String("AU"),
		},
	}); err != nil {
		t.Fatalf("Insert 1.1.1.1: %v", err)
	}

	path := filepath.Join(t.TempDir(), "test.mmdb")
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

func TestGeoIP_LoadAndLookup(t *testing.T) {
	path := generateTestMMDB(t)

	g := NewGeoIP()
	defer g.Close()

	info, err := g.Load(path)
	if err != nil {
		t.Fatalf("Load(%q): %v", path, err)
	}

	if info.DatabaseType != "Test-GeoIP" {
		t.Errorf("DatabaseType = %q, want %q", info.DatabaseType, "Test-GeoIP")
	}
	if info.BuildTime.IsZero() {
		t.Error("BuildTime is zero")
	}

	got := g.Lookup(context.Background(), "8.8.8.8")
	if got == nil {
		t.Fatal("Lookup(8.8.8.8) = nil, want non-nil result")
	}
	if got["country"] != "US" {
		t.Errorf("country = %q, want %q", got["country"], "US")
	}
	if got["city"] != "Mountain View" {
		t.Errorf("city = %q, want %q", got["city"], "Mountain View")
	}
	if got["subdivision"] != "California" {
		t.Errorf("subdivision = %q, want %q", got["subdivision"], "California")
	}
	if got["latitude"] != "37.3860" {
		t.Errorf("latitude = %q, want %q", got["latitude"], "37.3860")
	}
	if got["longitude"] != "-122.0838" {
		t.Errorf("longitude = %q, want %q", got["longitude"], "-122.0838")
	}
	if got["timezone"] != "America/Los_Angeles" {
		t.Errorf("timezone = %q, want %q", got["timezone"], "America/Los_Angeles")
	}
	if got["accuracy_radius"] != "1000" {
		t.Errorf("accuracy_radius = %q, want %q", got["accuracy_radius"], "1000")
	}
}

func TestGeoIP_ReaderSwap(t *testing.T) {
	path := generateTestMMDB(t)

	g := NewGeoIP()
	defer g.Close()

	// Load twice — the first reader should be closed without error.
	if _, err := g.Load(path); err != nil {
		t.Fatalf("first Load: %v", err)
	}
	if _, err := g.Load(path); err != nil {
		t.Fatalf("second Load: %v", err)
	}

	// Should still work after swap.
	got := g.Lookup(context.Background(), "8.8.8.8")
	if got == nil {
		t.Fatal("Lookup after swap = nil")
	}
}

func TestGeoIP_PartialAndMiss(t *testing.T) {
	path := generateTestMMDB(t)

	g := NewGeoIP()
	defer g.Close()

	if _, err := g.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// 1.1.1.1 has country only — no city.
	got := g.Lookup(context.Background(), "1.1.1.1")
	if got == nil {
		t.Fatal("Lookup(1.1.1.1) = nil, want non-nil")
	}
	if got["country"] != "AU" {
		t.Errorf("country = %q, want %q", got["country"], "AU")
	}
	if _, ok := got["city"]; ok {
		t.Errorf("unexpected city key: %q", got["city"])
	}

	// 10.0.0.1 (private IP) — complete miss, should return nil.
	if got := g.Lookup(context.Background(), "10.0.0.1"); got != nil {
		t.Errorf("Lookup(10.0.0.1) = %v, want nil", got)
	}
}
