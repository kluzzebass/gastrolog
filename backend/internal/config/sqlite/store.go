// Package sqlite provides a SQLite-based ConfigStore implementation.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create config directory: %w", err)
		}
	}
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
	certs, err := s.ListCertificates(ctx)
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
		Certs:             certs,
	}, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id string) (*config.FilterConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, expression FROM filters WHERE id = ?", id)

	var fc config.FilterConfig
	err := row.Scan(&fc.ID, &fc.Name, &fc.Expression)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get filter %q: %w", id, err)
	}
	return &fc, nil
}

func (s *Store) ListFilters(ctx context.Context) ([]config.FilterConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, expression FROM filters")
	if err != nil {
		return nil, fmt.Errorf("list filters: %w", err)
	}
	defer rows.Close()

	var result []config.FilterConfig
	for rows.Next() {
		var fc config.FilterConfig
		if err := rows.Scan(&fc.ID, &fc.Name, &fc.Expression); err != nil {
			return nil, fmt.Errorf("scan filter: %w", err)
		}
		result = append(result, fc)
	}
	return result, rows.Err()
}

func (s *Store) PutFilter(ctx context.Context, fc config.FilterConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO filters (id, name, expression)
		VALUES (?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			expression = excluded.expression
	`, fc.ID, fc.Name, fc.Expression)
	if err != nil {
		return fmt.Errorf("put filter %q: %w", fc.ID, err)
	}
	return nil
}

func (s *Store) DeleteFilter(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM filters WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete filter %q: %w", id, err)
	}
	return nil
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id string) (*config.RotationPolicyConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, max_bytes, max_age, max_records, cron FROM rotation_policies WHERE id = ?", id)

	var rp config.RotationPolicyConfig
	err := row.Scan(&rp.ID, &rp.Name, &rp.MaxBytes, &rp.MaxAge, &rp.MaxRecords, &rp.Cron)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rotation policy %q: %w", id, err)
	}
	return &rp, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) ([]config.RotationPolicyConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, max_bytes, max_age, max_records, cron FROM rotation_policies")
	if err != nil {
		return nil, fmt.Errorf("list rotation policies: %w", err)
	}
	defer rows.Close()

	var result []config.RotationPolicyConfig
	for rows.Next() {
		var rp config.RotationPolicyConfig
		if err := rows.Scan(&rp.ID, &rp.Name, &rp.MaxBytes, &rp.MaxAge, &rp.MaxRecords, &rp.Cron); err != nil {
			return nil, fmt.Errorf("scan rotation policy: %w", err)
		}
		result = append(result, rp)
	}
	return result, rows.Err()
}

func (s *Store) PutRotationPolicy(ctx context.Context, rp config.RotationPolicyConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO rotation_policies (id, name, max_bytes, max_age, max_records, cron)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			max_bytes = excluded.max_bytes,
			max_age = excluded.max_age,
			max_records = excluded.max_records,
			cron = excluded.cron
	`, rp.ID, rp.Name, rp.MaxBytes, rp.MaxAge, rp.MaxRecords, rp.Cron)
	if err != nil {
		return fmt.Errorf("put rotation policy %q: %w", rp.ID, err)
	}
	return nil
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM rotation_policies WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete rotation policy %q: %w", id, err)
	}
	return nil
}

// Retention policies

func (s *Store) GetRetentionPolicy(ctx context.Context, id string) (*config.RetentionPolicyConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, max_age, max_bytes, max_chunks FROM retention_policies WHERE id = ?", id)

	var rp config.RetentionPolicyConfig
	err := row.Scan(&rp.ID, &rp.Name, &rp.MaxAge, &rp.MaxBytes, &rp.MaxChunks)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get retention policy %q: %w", id, err)
	}
	return &rp, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) ([]config.RetentionPolicyConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, max_age, max_bytes, max_chunks FROM retention_policies")
	if err != nil {
		return nil, fmt.Errorf("list retention policies: %w", err)
	}
	defer rows.Close()

	var result []config.RetentionPolicyConfig
	for rows.Next() {
		var rp config.RetentionPolicyConfig
		if err := rows.Scan(&rp.ID, &rp.Name, &rp.MaxAge, &rp.MaxBytes, &rp.MaxChunks); err != nil {
			return nil, fmt.Errorf("scan retention policy: %w", err)
		}
		result = append(result, rp)
	}
	return result, rows.Err()
}

func (s *Store) PutRetentionPolicy(ctx context.Context, rp config.RetentionPolicyConfig) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO retention_policies (id, name, max_age, max_bytes, max_chunks)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			max_age = excluded.max_age,
			max_bytes = excluded.max_bytes,
			max_chunks = excluded.max_chunks
	`, rp.ID, rp.Name, rp.MaxAge, rp.MaxBytes, rp.MaxChunks)
	if err != nil {
		return fmt.Errorf("put retention policy %q: %w", rp.ID, err)
	}
	return nil
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM retention_policies WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete retention policy %q: %w", id, err)
	}
	return nil
}

// Stores

func (s *Store) GetStore(ctx context.Context, id string) (*config.StoreConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, type, filter, policy, retention, params, enabled FROM stores WHERE id = ?", id)

	var st config.StoreConfig
	var paramsJSON *string
	err := row.Scan(&st.ID, &st.Name, &st.Type, &st.Filter, &st.Policy, &st.Retention, &paramsJSON, &st.Enabled)
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
		"SELECT id, name, type, filter, policy, retention, params, enabled FROM stores")
	if err != nil {
		return nil, fmt.Errorf("list stores: %w", err)
	}
	defer rows.Close()

	var result []config.StoreConfig
	for rows.Next() {
		var st config.StoreConfig
		var paramsJSON *string
		if err := rows.Scan(&st.ID, &st.Name, &st.Type, &st.Filter, &st.Policy, &st.Retention, &paramsJSON, &st.Enabled); err != nil {
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
		v := string(data)
		paramsJSON = &v
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO stores (id, name, type, filter, policy, retention, params, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			type = excluded.type,
			filter = excluded.filter,
			policy = excluded.policy,
			retention = excluded.retention,
			params = excluded.params,
			enabled = excluded.enabled
	`, st.ID, st.Name, st.Type, st.Filter, st.Policy, st.Retention, paramsJSON, st.Enabled)
	if err != nil {
		return fmt.Errorf("put store %q: %w", st.ID, err)
	}
	return nil
}

func (s *Store) DeleteStore(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM stores WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete store %q: %w", id, err)
	}
	return nil
}

// Ingesters

func (s *Store) GetIngester(ctx context.Context, id string) (*config.IngesterConfig, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, type, params, enabled FROM ingesters WHERE id = ?", id)

	var ing config.IngesterConfig
	var paramsJSON *string
	err := row.Scan(&ing.ID, &ing.Name, &ing.Type, &paramsJSON, &ing.Enabled)
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
		"SELECT id, name, type, params, enabled FROM ingesters")
	if err != nil {
		return nil, fmt.Errorf("list ingesters: %w", err)
	}
	defer rows.Close()

	var result []config.IngesterConfig
	for rows.Next() {
		var ing config.IngesterConfig
		var paramsJSON *string
		if err := rows.Scan(&ing.ID, &ing.Name, &ing.Type, &paramsJSON, &ing.Enabled); err != nil {
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
		v := string(data)
		paramsJSON = &v
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ingesters (id, name, type, params, enabled)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			type = excluded.type,
			params = excluded.params,
			enabled = excluded.enabled
	`, ing.ID, ing.Name, ing.Type, paramsJSON, ing.Enabled)
	if err != nil {
		return fmt.Errorf("put ingester %q: %w", ing.ID, err)
	}
	return nil
}

func (s *Store) DeleteIngester(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM ingesters WHERE id = ?", id)
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

// Certificates

func (s *Store) ListCertificates(ctx context.Context) ([]config.CertPEM, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, cert_pem, key_pem, cert_file, key_file FROM tls_certificates")
	if err != nil {
		return nil, fmt.Errorf("list certificates: %w", err)
	}
	defer rows.Close()

	var result []config.CertPEM
	for rows.Next() {
		var cert config.CertPEM
		if err := rows.Scan(&cert.ID, &cert.Name, &cert.CertPEM, &cert.KeyPEM, &cert.CertFile, &cert.KeyFile); err != nil {
			return nil, fmt.Errorf("scan certificate: %w", err)
		}
		result = append(result, cert)
	}
	return result, rows.Err()
}

func (s *Store) GetCertificate(ctx context.Context, id string) (*config.CertPEM, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, cert_pem, key_pem, cert_file, key_file FROM tls_certificates WHERE id = ?", id)

	var cert config.CertPEM
	err := row.Scan(&cert.ID, &cert.Name, &cert.CertPEM, &cert.KeyPEM, &cert.CertFile, &cert.KeyFile)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get certificate %q: %w", id, err)
	}
	return &cert, nil
}

func (s *Store) PutCertificate(ctx context.Context, cert config.CertPEM) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tls_certificates (id, name, cert_pem, key_pem, cert_file, key_file)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			cert_pem = excluded.cert_pem,
			key_pem = excluded.key_pem,
			cert_file = excluded.cert_file,
			key_file = excluded.key_file
	`, cert.ID, cert.Name, cert.CertPEM, cert.KeyPEM, cert.CertFile, cert.KeyFile)
	if err != nil {
		return fmt.Errorf("put certificate %q: %w", cert.ID, err)
	}
	return nil
}

func (s *Store) DeleteCertificate(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM tls_certificates WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete certificate %q: %w", id, err)
	}
	return nil
}

// Users

func (s *Store) CreateUser(ctx context.Context, user config.User) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO users (id, username, password_hash, role, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, user.ID, user.Username, user.PasswordHash, user.Role,
		user.CreatedAt.Format(timeFormat), user.UpdatedAt.Format(timeFormat))
	if err != nil {
		return fmt.Errorf("create user %q: %w", user.Username, err)
	}
	return nil
}

func (s *Store) GetUser(ctx context.Context, id string) (*config.User, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, username, password_hash, role, created_at, updated_at FROM users WHERE id = ?", id)

	var u config.User
	var createdAt, updatedAt string
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user %q: %w", id, err)
	}
	u.CreatedAt, _ = time.Parse(timeFormat, createdAt)
	u.UpdatedAt, _ = time.Parse(timeFormat, updatedAt)
	return &u, nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*config.User, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, username, password_hash, role, created_at, updated_at FROM users WHERE username = ?", username)

	var u config.User
	var createdAt, updatedAt string
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get user by username %q: %w", username, err)
	}
	u.CreatedAt, _ = time.Parse(timeFormat, createdAt)
	u.UpdatedAt, _ = time.Parse(timeFormat, updatedAt)
	return &u, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]config.User, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, username, password_hash, role, created_at, updated_at FROM users ORDER BY created_at")
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []config.User
	for rows.Next() {
		var u config.User
		var createdAt, updatedAt string
		if err := rows.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Role, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		u.CreatedAt, _ = time.Parse(timeFormat, createdAt)
		u.UpdatedAt, _ = time.Parse(timeFormat, updatedAt)
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *Store) UpdatePassword(ctx context.Context, id string, passwordHash string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?
	`, passwordHash, time.Now().UTC().Format(timeFormat), id)
	if err != nil {
		return fmt.Errorf("update password for %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update password for %q: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("user %q not found", id)
	}
	return nil
}

func (s *Store) UpdateUserRole(ctx context.Context, id string, role string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE users SET role = ?, updated_at = ? WHERE id = ?
	`, role, time.Now().UTC().Format(timeFormat), id)
	if err != nil {
		return fmt.Errorf("update role for %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update role for %q: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("user %q not found", id)
	}
	return nil
}

func (s *Store) DeleteUser(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM users WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("delete user %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete user %q: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("user %q not found", id)
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
