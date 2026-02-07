package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestLoadMigrations(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("expected at least one migration")
	}
	if migrations[0].Version != 1 {
		t.Errorf("expected first migration version 1, got %d", migrations[0].Version)
	}
}

func TestRunMigrationsFreshDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := runMigrations(db); err != nil {
		t.Fatalf("runMigrations: %v", err)
	}

	// Verify schema_migrations recorded the version.
	var version int
	if err := db.QueryRow("SELECT version FROM schema_migrations").Scan(&version); err != nil {
		t.Fatalf("query version: %v", err)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}
}

func TestRunMigrationsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := runMigrations(db); err != nil {
		t.Fatalf("first run: %v", err)
	}

	if err := runMigrations(db); err != nil {
		t.Fatalf("second run: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 migration record, got %d", count)
	}
}
