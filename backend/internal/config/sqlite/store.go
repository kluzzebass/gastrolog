// Package sqlite provides a SQLite-based ConfigStore implementation.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"

	"gastrolog/internal/config"
)

// Store is a SQLite-based ConfigStore implementation.
type Store struct {
	db   *sql.DB
	path string
}

var _ config.Store = (*Store)(nil)

// NewStore opens a SQLite database at path and runs migrations.
func NewStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Set pragmas.
	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set journal_mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set foreign_keys: %w", err)
	}

	if err := runMigrations(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return &Store{db: db, path: path}, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// Load reads the full configuration. Returns nil if all tables are empty.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	// Check if anything exists.
	var count int
	err := s.db.QueryRowContext(ctx, `
		SELECT (SELECT count(*) FROM rotation_policies)
		     + (SELECT count(*) FROM stores)
		     + (SELECT count(*) FROM ingesters)
	`).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("count entities: %w", err)
	}
	if count == 0 {
		return nil, nil
	}

	policies, err := s.ListRotationPolicies(ctx)
	if err != nil {
		return nil, err
	}
	stores, err := s.ListStores(ctx)
	if err != nil {
		return nil, err
	}
	ingesters, err := s.ListIngesters(ctx)
	if err != nil {
		return nil, err
	}

	return &config.Config{
		RotationPolicies: policies,
		Stores:           stores,
		Ingesters:        ingesters,
	}, nil
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id string) (*config.RotationPolicyConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT max_bytes, max_age, max_records FROM rotation_policies WHERE rotation_policy_id = ?", id)

	var rp config.RotationPolicyConfig
	err := row.Scan(&rp.MaxBytes, &rp.MaxAge, &rp.MaxRecords)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rotation policy %q: %w", id, err)
	}
	return &rp, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) (map[string]config.RotationPolicyConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT rotation_policy_id, max_bytes, max_age, max_records FROM rotation_policies")
	if err != nil {
		return nil, fmt.Errorf("list rotation policies: %w", err)
	}
	defer rows.Close()

	result := make(map[string]config.RotationPolicyConfig)
	for rows.Next() {
		var id string
		var rp config.RotationPolicyConfig
		if err := rows.Scan(&id, &rp.MaxBytes, &rp.MaxAge, &rp.MaxRecords); err != nil {
			return nil, fmt.Errorf("scan rotation policy: %w", err)
		}
		result[id] = rp
	}
	return result, rows.Err()
}

func (s *Store) PutRotationPolicy(ctx context.Context, id string, rp config.RotationPolicyConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO rotation_policies (rotation_policy_id, max_bytes, max_age, max_records)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(rotation_policy_id) DO UPDATE SET
			max_bytes = excluded.max_bytes,
			max_age = excluded.max_age,
			max_records = excluded.max_records
	`, id, rp.MaxBytes, rp.MaxAge, rp.MaxRecords)
	if err != nil {
		return fmt.Errorf("put rotation policy %q: %w", id, err)
	}
	return nil
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM rotation_policies WHERE rotation_policy_id = ?", id)
	if err != nil {
		return fmt.Errorf("delete rotation policy %q: %w", id, err)
	}
	return nil
}

// Stores

func (s *Store) GetStore(ctx context.Context, id string) (*config.StoreConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT store_id, type, filter, policy, params FROM stores WHERE store_id = ?", id)

	var st config.StoreConfig
	var paramsJSON *string
	err := row.Scan(&st.ID, &st.Type, &st.Filter, &st.Policy, &paramsJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get store %q: %w", id, err)
	}
	if paramsJSON != nil {
		if err := json.Unmarshal([]byte(*paramsJSON), &st.Params); err != nil {
			return nil, fmt.Errorf("unmarshal store %q params: %w", id, err)
		}
	}
	return &st, nil
}

func (s *Store) ListStores(ctx context.Context) ([]config.StoreConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT store_id, type, filter, policy, params FROM stores")
	if err != nil {
		return nil, fmt.Errorf("list stores: %w", err)
	}
	defer rows.Close()

	var result []config.StoreConfig
	for rows.Next() {
		var st config.StoreConfig
		var paramsJSON *string
		if err := rows.Scan(&st.ID, &st.Type, &st.Filter, &st.Policy, &paramsJSON); err != nil {
			return nil, fmt.Errorf("scan store: %w", err)
		}
		if paramsJSON != nil {
			if err := json.Unmarshal([]byte(*paramsJSON), &st.Params); err != nil {
				return nil, fmt.Errorf("unmarshal store params: %w", err)
			}
		}
		result = append(result, st)
	}
	return result, rows.Err()
}

func (s *Store) PutStore(ctx context.Context, st config.StoreConfig) error {
	var paramsJSON *string
	if st.Params != nil {
		data, err := json.Marshal(st.Params)
		if err != nil {
			return fmt.Errorf("marshal store %q params: %w", st.ID, err)
		}
		s := string(data)
		paramsJSON = &s
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO stores (store_id, type, filter, policy, params)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(store_id) DO UPDATE SET
			type = excluded.type,
			filter = excluded.filter,
			policy = excluded.policy,
			params = excluded.params
	`, st.ID, st.Type, st.Filter, st.Policy, paramsJSON)
	if err != nil {
		return fmt.Errorf("put store %q: %w", st.ID, err)
	}
	return nil
}

func (s *Store) DeleteStore(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM stores WHERE store_id = ?", id)
	if err != nil {
		return fmt.Errorf("delete store %q: %w", id, err)
	}
	return nil
}

// Ingesters

func (s *Store) GetIngester(ctx context.Context, id string) (*config.IngesterConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT ingester_id, type, params FROM ingesters WHERE ingester_id = ?", id)

	var ing config.IngesterConfig
	var paramsJSON *string
	err := row.Scan(&ing.ID, &ing.Type, &paramsJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ingester %q: %w", id, err)
	}
	if paramsJSON != nil {
		if err := json.Unmarshal([]byte(*paramsJSON), &ing.Params); err != nil {
			return nil, fmt.Errorf("unmarshal ingester %q params: %w", id, err)
		}
	}
	return &ing, nil
}

func (s *Store) ListIngesters(ctx context.Context) ([]config.IngesterConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT ingester_id, type, params FROM ingesters")
	if err != nil {
		return nil, fmt.Errorf("list ingesters: %w", err)
	}
	defer rows.Close()

	var result []config.IngesterConfig
	for rows.Next() {
		var ing config.IngesterConfig
		var paramsJSON *string
		if err := rows.Scan(&ing.ID, &ing.Type, &paramsJSON); err != nil {
			return nil, fmt.Errorf("scan ingester: %w", err)
		}
		if paramsJSON != nil {
			if err := json.Unmarshal([]byte(*paramsJSON), &ing.Params); err != nil {
				return nil, fmt.Errorf("unmarshal ingester params: %w", err)
			}
		}
		result = append(result, ing)
	}
	return result, rows.Err()
}

func (s *Store) PutIngester(ctx context.Context, ing config.IngesterConfig) error {
	var paramsJSON *string
	if ing.Params != nil {
		data, err := json.Marshal(ing.Params)
		if err != nil {
			return fmt.Errorf("marshal ingester %q params: %w", ing.ID, err)
		}
		s := string(data)
		paramsJSON = &s
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ingesters (ingester_id, type, params)
		VALUES (?, ?, ?)
		ON CONFLICT(ingester_id) DO UPDATE SET
			type = excluded.type,
			params = excluded.params
	`, ing.ID, ing.Type, paramsJSON)
	if err != nil {
		return fmt.Errorf("put ingester %q: %w", ing.ID, err)
	}
	return nil
}

func (s *Store) DeleteIngester(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM ingesters WHERE ingester_id = ?", id)
	if err != nil {
		return fmt.Errorf("delete ingester %q: %w", id, err)
	}
	return nil
}
