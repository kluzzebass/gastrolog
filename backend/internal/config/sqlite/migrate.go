package sqlite

import (
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type migration struct {
	Version int
	SQL     string
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	var migrations []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}

		// Parse version from filename: 001_init.sql -> 1
		parts := strings.SplitN(e.Name(), "_", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid migration filename: %s", e.Name())
		}
		version, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid migration version in %s: %w", e.Name(), err)
		}

		data, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}

		migrations = append(migrations, migration{Version: version, SQL: string(data)})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func runMigrations(db *sql.DB) error {
	// Create schema_migrations table if not exists.
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY
	) STRICT`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	// Query applied versions.
	applied := map[int]bool{}
	rows, err := db.Query("SELECT version FROM schema_migrations")
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("scan migration version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate migration versions: %w", err)
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}

		// Disable foreign keys for migration (SQLite requires this outside a transaction).
		if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
			return fmt.Errorf("disable foreign keys for migration %d: %w", m.Version, err)
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.Version, err)
		}

		if _, err := tx.Exec(m.SQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("execute migration %d: %w", m.Version, err)
		}

		if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES (?)", m.Version); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %d: %w", m.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}

		// Re-enable foreign keys and check for violations.
		if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
			return fmt.Errorf("enable foreign keys after migration %d: %w", m.Version, err)
		}
		rows, err := db.Query("PRAGMA foreign_key_check")
		if err != nil {
			return fmt.Errorf("foreign key check after migration %d: %w", m.Version, err)
		}
		if rows.Next() {
			rows.Close()
			return fmt.Errorf("foreign key violations after migration %d", m.Version)
		}
		rows.Close()
	}

	return nil
}
