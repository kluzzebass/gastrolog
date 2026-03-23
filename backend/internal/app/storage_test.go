package app

import (
	"testing"
)

func TestParseStorageFlags(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		areas, err := parseStorageFlags([]string{"/data/nvme:class=1:label=NVMe"})
		if err != nil {
			t.Fatal(err)
		}
		if len(areas) != 1 {
			t.Fatalf("got %d areas, want 1", len(areas))
		}
		a := areas[0]
		if a.Path != "/data/nvme" {
			t.Errorf("path = %q, want /data/nvme", a.Path)
		}
		if a.StorageClass != 1 {
			t.Errorf("class = %d, want 1", a.StorageClass)
		}
		if a.Label != "NVMe" {
			t.Errorf("label = %q, want NVMe", a.Label)
		}
		if a.ID.String() == "" {
			t.Error("expected non-empty UUID")
		}
	})

	t.Run("default label from basename", func(t *testing.T) {
		areas, err := parseStorageFlags([]string{"/data/nvme:class=1"})
		if err != nil {
			t.Fatal(err)
		}
		if areas[0].Label != "nvme" {
			t.Errorf("label = %q, want nvme", areas[0].Label)
		}
	})

	t.Run("missing class errors", func(t *testing.T) {
		_, err := parseStorageFlags([]string{"/data/nvme:label=NVMe"})
		if err == nil {
			t.Fatal("expected error for missing class")
		}
	})

	t.Run("multiple flags", func(t *testing.T) {
		areas, err := parseStorageFlags([]string{
			"/data/nvme:class=1:label=NVMe",
			"/data/hdd:class=3:label=HDD",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(areas) != 2 {
			t.Fatalf("got %d areas, want 2", len(areas))
		}
		// Each should have a unique UUID.
		if areas[0].ID == areas[1].ID {
			t.Error("expected unique UUIDs for each area")
		}
	})

	t.Run("no colon in path works", func(t *testing.T) {
		areas, err := parseStorageFlags([]string{"/data/nvme:class=1"})
		if err != nil {
			t.Fatal(err)
		}
		if areas[0].Path != "/data/nvme" {
			t.Errorf("path = %q, want /data/nvme", areas[0].Path)
		}
	})

	t.Run("too few segments errors", func(t *testing.T) {
		_, err := parseStorageFlags([]string{"/data/nvme"})
		if err == nil {
			t.Fatal("expected error for single segment")
		}
	})

	t.Run("unknown key errors", func(t *testing.T) {
		_, err := parseStorageFlags([]string{"/data/nvme:class=1:bogus=yes"})
		if err == nil {
			t.Fatal("expected error for unknown key")
		}
	})
}

