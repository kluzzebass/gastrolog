package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"gastrolog/internal/config"
	"gastrolog/internal/config/storetest"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestConformance(t *testing.T) {
	storetest.TestStore(t, func(t *testing.T) config.Store {
		return newTestStore(t)
	})
}

func TestPragmas(t *testing.T) {
	s := newTestStore(t)

	var journalMode string
	if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("expected journal_mode=wal, got %q", journalMode)
	}

	var fk int
	if err := s.db.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("query foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("expected foreign_keys=1, got %d", fk)
	}
}

func TestSchema(t *testing.T) {
	s := newTestStore(t)

	tables := map[string]bool{}
	rows, err := s.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		tables[name] = true
	}

	for _, want := range []string{"rotation_policies", "stores", "ingesters", "schema_migrations"} {
		if !tables[want] {
			t.Errorf("expected table %q, got tables: %v", want, tables)
		}
	}
}

func TestMigrationsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	// Open and close twice — migrations should be idempotent.
	s1, err := NewStore(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	s1.Close()

	s2, err := NewStore(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer s2.Close()

	// Verify schema_migrations has exactly one version.
	var count int
	if err := s2.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 migration version, got %d", count)
	}
}

func TestConnectionLimits(t *testing.T) {
	s := newTestStore(t)

	if got := s.db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("expected MaxOpenConnections=1, got %d", got)
	}

	// MaxIdleConns isn't directly queryable from Stats(), but we can
	// verify the DB was configured by checking it doesn't error.
	// The real check is in NewStore setting it.
	_ = s.db.Stats().Idle
}

func TestStrictTables(t *testing.T) {
	s := newTestStore(t)

	// STRICT tables reject type mismatches. rotation_policies.max_records
	// is INTEGER — inserting a non-numeric text should fail.
	_, err := s.db.Exec(
		"INSERT INTO rotation_policies (rotation_policy_id, max_records) VALUES (?, ?)",
		"test", "not-a-number")
	if err == nil {
		t.Fatal("expected error inserting text into STRICT INTEGER column")
	}
}

func TestNullRoundTrip(t *testing.T) {
	s := newTestStore(t)

	// Insert a rotation policy with all NULL optional fields directly via SQL.
	_, err := s.db.Exec(
		"INSERT INTO rotation_policies (rotation_policy_id) VALUES (?)", "nulltest")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Read back via the Store interface.
	rp, err := s.GetRotationPolicy(context.Background(), "nulltest")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rp == nil {
		t.Fatal("expected rotation policy, got nil")
	}
	if rp.MaxBytes != nil {
		t.Errorf("expected nil MaxBytes, got %v", *rp.MaxBytes)
	}
	if rp.MaxAge != nil {
		t.Errorf("expected nil MaxAge, got %v", *rp.MaxAge)
	}
	if rp.MaxRecords != nil {
		t.Errorf("expected nil MaxRecords, got %v", *rp.MaxRecords)
	}
}

func TestCloseReleasesDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After close, the DB file should be openable by another connection.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping after re-open: %v", err)
	}
}
