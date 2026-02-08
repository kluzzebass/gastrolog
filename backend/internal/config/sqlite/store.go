// Package sqlite provides a SQLite-based ConfigStore implementation.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"gastrolog/internal/config"
)

const timeFormat = time.RFC3339

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
		SELECT (SELECT count(*) FROM filters)
		     + (SELECT count(*) FROM rotation_policies)
		     + (SELECT count(*) FROM retention_policies)
		     + (SELECT count(*) FROM stores)
		     + (SELECT count(*) FROM ingesters)
		     + (SELECT count(*) FROM settings)
	`).Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("count entities: %w", err)
	}
	if count == 0 {
		return nil, nil
	}

	filters, err := s.ListFilters(ctx)
	if err != nil {
		return nil, err
	}
	rotationPolicies, err := s.ListRotationPolicies(ctx)
	if err != nil {
		return nil, err
	}
	retentionPolicies, err := s.ListRetentionPolicies(ctx)
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

	settings, err := s.listSettings(ctx)
	if err != nil {
		return nil, err
	}

	return &config.Config{
		Filters:           filters,
		RotationPolicies:  rotationPolicies,
		RetentionPolicies: retentionPolicies,
		Stores:            stores,
		Ingesters:         ingesters,
		Settings:          settings,
	}, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id string) (*config.FilterConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT expression FROM filters WHERE filter_id = ?", id)

	var fc config.FilterConfig
	err := row.Scan(&fc.Expression)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get filter %q: %w", id, err)
	}
	return &fc, nil
}

func (s *Store) ListFilters(ctx context.Context) (map[string]config.FilterConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT filter_id, expression FROM filters")
	if err != nil {
		return nil, fmt.Errorf("list filters: %w", err)
	}
	defer rows.Close()

	result := make(map[string]config.FilterConfig)
	for rows.Next() {
		var id string
		var fc config.FilterConfig
		if err := rows.Scan(&id, &fc.Expression); err != nil {
			return nil, fmt.Errorf("scan filter: %w", err)
		}
		result[id] = fc
	}
	return result, rows.Err()
}

func (s *Store) PutFilter(ctx context.Context, id string, fc config.FilterConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO filters (filter_id, expression)
		VALUES (?, ?)
		ON CONFLICT(filter_id) DO UPDATE SET
			expression = excluded.expression
	`, id, fc.Expression)
	if err != nil {
		return fmt.Errorf("put filter %q: %w", id, err)
	}
	return nil
}

func (s *Store) DeleteFilter(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM filters WHERE filter_id = ?", id)
	if err != nil {
		return fmt.Errorf("delete filter %q: %w", id, err)
	}
	return nil
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id string) (*config.RotationPolicyConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT max_bytes, max_age, max_records, cron FROM rotation_policies WHERE rotation_policy_id = ?", id)

	var rp config.RotationPolicyConfig
	err := row.Scan(&rp.MaxBytes, &rp.MaxAge, &rp.MaxRecords, &rp.Cron)
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
		"SELECT rotation_policy_id, max_bytes, max_age, max_records, cron FROM rotation_policies")
	if err != nil {
		return nil, fmt.Errorf("list rotation policies: %w", err)
	}
	defer rows.Close()

	result := make(map[string]config.RotationPolicyConfig)
	for rows.Next() {
		var id string
		var rp config.RotationPolicyConfig
		if err := rows.Scan(&id, &rp.MaxBytes, &rp.MaxAge, &rp.MaxRecords, &rp.Cron); err != nil {
			return nil, fmt.Errorf("scan rotation policy: %w", err)
		}
		result[id] = rp
	}
	return result, rows.Err()
}

func (s *Store) PutRotationPolicy(ctx context.Context, id string, rp config.RotationPolicyConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO rotation_policies (rotation_policy_id, max_bytes, max_age, max_records, cron)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(rotation_policy_id) DO UPDATE SET
			max_bytes = excluded.max_bytes,
			max_age = excluded.max_age,
			max_records = excluded.max_records,
			cron = excluded.cron
	`, id, rp.MaxBytes, rp.MaxAge, rp.MaxRecords, rp.Cron)
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

// Retention policies

func (s *Store) GetRetentionPolicy(ctx context.Context, id string) (*config.RetentionPolicyConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT max_age, max_bytes, max_chunks FROM retention_policies WHERE retention_policy_id = ?", id)

	var rp config.RetentionPolicyConfig
	err := row.Scan(&rp.MaxAge, &rp.MaxBytes, &rp.MaxChunks)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get retention policy %q: %w", id, err)
	}
	return &rp, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) (map[string]config.RetentionPolicyConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT retention_policy_id, max_age, max_bytes, max_chunks FROM retention_policies")
	if err != nil {
		return nil, fmt.Errorf("list retention policies: %w", err)
	}
	defer rows.Close()

	result := make(map[string]config.RetentionPolicyConfig)
	for rows.Next() {
		var id string
		var rp config.RetentionPolicyConfig
		if err := rows.Scan(&id, &rp.MaxAge, &rp.MaxBytes, &rp.MaxChunks); err != nil {
			return nil, fmt.Errorf("scan retention policy: %w", err)
		}
		result[id] = rp
	}
	return result, rows.Err()
}

func (s *Store) PutRetentionPolicy(ctx context.Context, id string, rp config.RetentionPolicyConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO retention_policies (retention_policy_id, max_age, max_bytes, max_chunks)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(retention_policy_id) DO UPDATE SET
			max_age = excluded.max_age,
			max_bytes = excluded.max_bytes,
			max_chunks = excluded.max_chunks
	`, id, rp.MaxAge, rp.MaxBytes, rp.MaxChunks)
	if err != nil {
		return fmt.Errorf("put retention policy %q: %w", id, err)
	}
	return nil
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM retention_policies WHERE retention_policy_id = ?", id)
	if err != nil {
		return fmt.Errorf("delete retention policy %q: %w", id, err)
	}
	return nil
}

// Stores

func (s *Store) GetStore(ctx context.Context, id string) (*config.StoreConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT store_id, type, filter, policy, retention, params FROM stores WHERE store_id = ?", id)

	var st config.StoreConfig
	var paramsJSON *string
	err := row.Scan(&st.ID, &st.Type, &st.Filter, &st.Policy, &st.Retention, &paramsJSON)
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
		"SELECT store_id, type, filter, policy, retention, params FROM stores")
	if err != nil {
		return nil, fmt.Errorf("list stores: %w", err)
	}
	defer rows.Close()

	var result []config.StoreConfig
	for rows.Next() {
		var st config.StoreConfig
		var paramsJSON *string
		if err := rows.Scan(&st.ID, &st.Type, &st.Filter, &st.Policy, &st.Retention, &paramsJSON); err != nil {
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
		INSERT INTO stores (store_id, type, filter, policy, retention, params)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(store_id) DO UPDATE SET
			type = excluded.type,
			filter = excluded.filter,
			policy = excluded.policy,
			retention = excluded.retention,
			params = excluded.params
	`, st.ID, st.Type, st.Filter, st.Policy, st.Retention, paramsJSON)
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

// Settings

func (s *Store) listSettings(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT key, value FROM settings")
	if err != nil {
		return nil, fmt.Errorf("list settings: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("scan setting: %w", err)
		}
		result[key] = value
	}
	return result, rows.Err()
}

func (s *Store) GetSetting(ctx context.Context, key string) (*string, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT value FROM settings WHERE key = ?", key)

	var value string
	err := row.Scan(&value)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get setting %q: %w", key, err)
	}
	return &value, nil
}

func (s *Store) PutSetting(ctx context.Context, key string, value string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO settings (key, value)
		VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value
	`, key, value)
	if err != nil {
		return fmt.Errorf("put setting %q: %w", key, err)
	}
	return nil
}

func (s *Store) DeleteSetting(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM settings WHERE key = ?", key)
	if err != nil {
		return fmt.Errorf("delete setting %q: %w", key, err)
	}
	return nil
}

// Users

func (s *Store) CreateUser(ctx context.Context, user config.User) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (username, password_hash, role, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, user.Username, user.PasswordHash, user.Role,
		user.CreatedAt.Format(timeFormat), user.UpdatedAt.Format(timeFormat))
	if err != nil {
		return fmt.Errorf("create user %q: %w", user.Username, err)
	}
	return nil
}

func (s *Store) GetUser(ctx context.Context, username string) (*config.User, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT username, password_hash, role, created_at, updated_at FROM users WHERE username = ?", username)

	var u config.User
	var createdAt, updatedAt string
	err := row.Scan(&u.Username, &u.PasswordHash, &u.Role, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user %q: %w", username, err)
	}
	u.CreatedAt, _ = time.Parse(timeFormat, createdAt)
	u.UpdatedAt, _ = time.Parse(timeFormat, updatedAt)
	return &u, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]config.User, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT username, password_hash, role, created_at, updated_at FROM users ORDER BY created_at")
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []config.User
	for rows.Next() {
		var u config.User
		var createdAt, updatedAt string
		if err := rows.Scan(&u.Username, &u.PasswordHash, &u.Role, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		u.CreatedAt, _ = time.Parse(timeFormat, createdAt)
		u.UpdatedAt, _ = time.Parse(timeFormat, updatedAt)
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *Store) UpdatePassword(ctx context.Context, username string, passwordHash string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET password_hash = ?, updated_at = ? WHERE username = ?
	`, passwordHash, time.Now().UTC().Format(timeFormat), username)
	if err != nil {
		return fmt.Errorf("update password for %q: %w", username, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update password for %q: %w", username, err)
	}
	if n == 0 {
		return fmt.Errorf("user %q not found", username)
	}
	return nil
}

func (s *Store) UpdateUserRole(ctx context.Context, username string, role string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET role = ?, updated_at = ? WHERE username = ?
	`, role, time.Now().UTC().Format(timeFormat), username)
	if err != nil {
		return fmt.Errorf("update role for %q: %w", username, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update role for %q: %w", username, err)
	}
	if n == 0 {
		return fmt.Errorf("user %q not found", username)
	}
	return nil
}

func (s *Store) DeleteUser(ctx context.Context, username string) error {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM users WHERE username = ?", username)
	if err != nil {
		return fmt.Errorf("delete user %q: %w", username, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete user %q: %w", username, err)
	}
	if n == 0 {
		return fmt.Errorf("user %q not found", username)
	}
	return nil
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return count, nil
}
