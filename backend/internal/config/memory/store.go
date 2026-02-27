// Package memory provides an in-memory ConfigStore implementation.
// Intended for testing. Configuration is not persisted across restarts.
package memory

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"gastrolog/internal/config"

	"github.com/google/uuid"
)

// serverSettings holds the typed server-level config fields.
type serverSettings struct {
	auth             config.AuthConfig
	query            config.QueryConfig
	scheduler        config.SchedulerConfig
	tls              config.TLSConfig
	lookup           config.LookupConfig
	setupDismissed   bool
	hasServerSettings bool // true once SaveServerSettings has been called at least once
}

// Store is an in-memory ConfigStore implementation.
type Store struct {
	mu                sync.RWMutex
	filters           map[uuid.UUID]config.FilterConfig
	rotationPolicies  map[uuid.UUID]config.RotationPolicyConfig
	retentionPolicies map[uuid.UUID]config.RetentionPolicyConfig
	vaults            map[uuid.UUID]config.VaultConfig
	ingesters         map[uuid.UUID]config.IngesterConfig
	ss                serverSettings
	certs             map[uuid.UUID]config.CertPEM
	users         map[uuid.UUID]config.User         // keyed by ID (UUID)
	refreshTokens map[uuid.UUID]config.RefreshToken // keyed by token ID
	nodes         map[uuid.UUID]config.NodeConfig    // keyed by node ID
	clusterTLS    *config.ClusterTLS
}

var _ config.Store = (*Store)(nil)

// NewStore creates a new in-memory ConfigStore.
func NewStore() *Store {
	return &Store{
		filters:           make(map[uuid.UUID]config.FilterConfig),
		rotationPolicies:  make(map[uuid.UUID]config.RotationPolicyConfig),
		retentionPolicies: make(map[uuid.UUID]config.RetentionPolicyConfig),
		vaults:            make(map[uuid.UUID]config.VaultConfig),
		ingesters:         make(map[uuid.UUID]config.IngesterConfig),
		certs:             make(map[uuid.UUID]config.CertPEM),
		users:         make(map[uuid.UUID]config.User),
		refreshTokens: make(map[uuid.UUID]config.RefreshToken),
		nodes:         make(map[uuid.UUID]config.NodeConfig),
	}
}

// Load returns the full configuration.
// Returns nil if no entities exist.
func (s *Store) Load(ctx context.Context) (*config.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.filters) == 0 && len(s.rotationPolicies) == 0 && len(s.retentionPolicies) == 0 && len(s.vaults) == 0 && len(s.ingesters) == 0 && !s.ss.hasServerSettings && s.clusterTLS == nil {
		return nil, nil
	}

	cfg := &config.Config{}

	if len(s.filters) > 0 {
		cfg.Filters = make([]config.FilterConfig, 0, len(s.filters))
		for _, fc := range s.filters {
			cfg.Filters = append(cfg.Filters, copyFilterConfig(fc))
		}
	}

	if len(s.rotationPolicies) > 0 {
		cfg.RotationPolicies = make([]config.RotationPolicyConfig, 0, len(s.rotationPolicies))
		for _, rp := range s.rotationPolicies {
			cfg.RotationPolicies = append(cfg.RotationPolicies, copyRotationPolicy(rp))
		}
	}

	if len(s.retentionPolicies) > 0 {
		cfg.RetentionPolicies = make([]config.RetentionPolicyConfig, 0, len(s.retentionPolicies))
		for _, rp := range s.retentionPolicies {
			cfg.RetentionPolicies = append(cfg.RetentionPolicies, copyRetentionPolicy(rp))
		}
	}

	if len(s.vaults) > 0 {
		cfg.Vaults = make([]config.VaultConfig, 0, len(s.vaults))
		for _, st := range s.vaults {
			cfg.Vaults = append(cfg.Vaults, copyVaultConfig(st))
		}
	}

	if len(s.ingesters) > 0 {
		cfg.Ingesters = make([]config.IngesterConfig, 0, len(s.ingesters))
		for _, ing := range s.ingesters {
			cfg.Ingesters = append(cfg.Ingesters, copyIngesterConfig(ing))
		}
	}

	if len(s.certs) > 0 {
		cfg.Certs = make([]config.CertPEM, 0, len(s.certs))
		for _, cert := range s.certs {
			cfg.Certs = append(cfg.Certs, copyCertPEM(cert))
		}
	}

	if len(s.nodes) > 0 {
		cfg.Nodes = make([]config.NodeConfig, 0, len(s.nodes))
		for _, n := range s.nodes {
			cfg.Nodes = append(cfg.Nodes, n)
		}
	}

	// Populate server settings on Config.
	if s.ss.hasServerSettings {
		cfg.Auth = s.ss.auth
		cfg.Query = s.ss.query
		cfg.Scheduler = s.ss.scheduler
		cfg.TLS = s.ss.tls
		cfg.Lookup = s.ss.lookup
		cfg.SetupWizardDismissed = s.ss.setupDismissed
	}

	// Include ClusterTLS if loaded (cluster mode).
	if s.clusterTLS != nil {
		c := *s.clusterTLS
		cfg.ClusterTLS = &c
	}

	return cfg, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id uuid.UUID) (*config.FilterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fc, ok := s.filters[id]
	if !ok {
		return nil, nil
	}
	c := copyFilterConfig(fc)
	return &c, nil
}

func (s *Store) ListFilters(ctx context.Context) ([]config.FilterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.FilterConfig, 0, len(s.filters))
	for _, fc := range s.filters {
		result = append(result, copyFilterConfig(fc))
	}
	return result, nil
}

func (s *Store) PutFilter(ctx context.Context, cfg config.FilterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.filters[cfg.ID] = copyFilterConfig(cfg)
	return nil
}

func (s *Store) DeleteFilter(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.filters, id)
	return nil
}

// Rotation policies

func (s *Store) GetRotationPolicy(ctx context.Context, id uuid.UUID) (*config.RotationPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rp, ok := s.rotationPolicies[id]
	if !ok {
		return nil, nil
	}
	c := copyRotationPolicy(rp)
	return &c, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) ([]config.RotationPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.RotationPolicyConfig, 0, len(s.rotationPolicies))
	for _, rp := range s.rotationPolicies {
		result = append(result, copyRotationPolicy(rp))
	}
	return result, nil
}

func (s *Store) PutRotationPolicy(ctx context.Context, cfg config.RotationPolicyConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rotationPolicies[cfg.ID] = copyRotationPolicy(cfg)
	return nil
}

func (s *Store) DeleteRotationPolicy(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.rotationPolicies, id)
	return nil
}

// Retention policies

func (s *Store) GetRetentionPolicy(ctx context.Context, id uuid.UUID) (*config.RetentionPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rp, ok := s.retentionPolicies[id]
	if !ok {
		return nil, nil
	}
	c := copyRetentionPolicy(rp)
	return &c, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) ([]config.RetentionPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.RetentionPolicyConfig, 0, len(s.retentionPolicies))
	for _, rp := range s.retentionPolicies {
		result = append(result, copyRetentionPolicy(rp))
	}
	return result, nil
}

func (s *Store) PutRetentionPolicy(ctx context.Context, cfg config.RetentionPolicyConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retentionPolicies[cfg.ID] = copyRetentionPolicy(cfg)
	return nil
}

func (s *Store) DeleteRetentionPolicy(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.retentionPolicies, id)
	return nil
}

// Vaults

func (s *Store) GetVault(ctx context.Context, id uuid.UUID) (*config.VaultConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st, ok := s.vaults[id]
	if !ok {
		return nil, nil
	}
	c := copyVaultConfig(st)
	return &c, nil
}

func (s *Store) ListVaults(ctx context.Context) ([]config.VaultConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.VaultConfig, 0, len(s.vaults))
	for _, st := range s.vaults {
		result = append(result, copyVaultConfig(st))
	}
	return result, nil
}

func (s *Store) PutVault(ctx context.Context, cfg config.VaultConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.vaults[cfg.ID] = copyVaultConfig(cfg)
	return nil
}

func (s *Store) DeleteVault(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.vaults, id)
	return nil
}

// Ingesters

func (s *Store) GetIngester(ctx context.Context, id uuid.UUID) (*config.IngesterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ing, ok := s.ingesters[id]
	if !ok {
		return nil, nil
	}
	c := copyIngesterConfig(ing)
	return &c, nil
}

func (s *Store) ListIngesters(ctx context.Context) ([]config.IngesterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.IngesterConfig, 0, len(s.ingesters))
	for _, ing := range s.ingesters {
		result = append(result, copyIngesterConfig(ing))
	}
	return result, nil
}

func (s *Store) PutIngester(ctx context.Context, cfg config.IngesterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ingesters[cfg.ID] = copyIngesterConfig(cfg)
	return nil
}

func (s *Store) DeleteIngester(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.ingesters, id)
	return nil
}

// Server settings

func (s *Store) LoadServerSettings(ctx context.Context) (config.AuthConfig, config.QueryConfig, config.SchedulerConfig, config.TLSConfig, config.LookupConfig, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.ss.auth, s.ss.query, s.ss.scheduler, s.ss.tls, s.ss.lookup, s.ss.setupDismissed, nil
}

func (s *Store) SaveServerSettings(ctx context.Context, auth config.AuthConfig, query config.QueryConfig, sched config.SchedulerConfig, tls config.TLSConfig, lookup config.LookupConfig, setupDismissed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ss = serverSettings{
		auth:              auth,
		query:             query,
		scheduler:         sched,
		tls:               tls,
		lookup:            lookup,
		setupDismissed:    setupDismissed,
		hasServerSettings: true,
	}
	return nil
}

// Nodes

func (s *Store) GetNode(ctx context.Context, id uuid.UUID) (*config.NodeConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.nodes[id]
	if !ok {
		return nil, nil
	}
	return &n, nil
}

func (s *Store) ListNodes(ctx context.Context) ([]config.NodeConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.NodeConfig, 0, len(s.nodes))
	for _, n := range s.nodes {
		result = append(result, n)
	}
	return result, nil
}

func (s *Store) PutNode(ctx context.Context, node config.NodeConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodes[node.ID] = node
	return nil
}

func (s *Store) DeleteNode(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.nodes, id)
	return nil
}

// Cluster TLS

func (s *Store) PutClusterTLS(ctx context.Context, tls config.ClusterTLS) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusterTLS = &tls
	return nil
}

// Certificates

func (s *Store) ListCertificates(ctx context.Context) ([]config.CertPEM, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]config.CertPEM, 0, len(s.certs))
	for _, cert := range s.certs {
		result = append(result, copyCertPEM(cert))
	}
	return result, nil
}

func (s *Store) GetCertificate(ctx context.Context, id uuid.UUID) (*config.CertPEM, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pem, ok := s.certs[id]
	if !ok {
		return nil, nil
	}
	c := copyCertPEM(pem)
	return &c, nil
}

func (s *Store) PutCertificate(ctx context.Context, cert config.CertPEM) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.certs[cert.ID] = copyCertPEM(cert)
	return nil
}

func (s *Store) DeleteCertificate(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.certs, id)
	return nil
}

// Users

func (s *Store) CreateUser(ctx context.Context, user config.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[user.ID]; ok {
		return fmt.Errorf("user %q already exists", user.ID)
	}
	// Also check for duplicate username.
	for _, u := range s.users {
		if u.Username == user.Username {
			return fmt.Errorf("username %q already exists", user.Username)
		}
	}
	s.users[user.ID] = user
	return nil
}

func (s *Store) GetUser(ctx context.Context, id uuid.UUID) (*config.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	u, ok := s.users[id]
	if !ok {
		return nil, nil
	}
	return &u, nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*config.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, u := range s.users {
		if u.Username == username {
			return &u, nil
		}
	}
	return nil, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]config.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]config.User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return users, nil
}

func (s *Store) UpdatePassword(ctx context.Context, id uuid.UUID, passwordHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.PasswordHash = passwordHash
	u.UpdatedAt = time.Now().UTC()
	s.users[id] = u
	return nil
}

func (s *Store) UpdateUserRole(ctx context.Context, id uuid.UUID, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.Role = role
	u.UpdatedAt = time.Now().UTC()
	s.users[id] = u
	return nil
}

func (s *Store) UpdateUsername(ctx context.Context, id uuid.UUID, username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	// Check uniqueness.
	for uid, other := range s.users {
		if uid != id && other.Username == username {
			return fmt.Errorf("username %q is already taken", username)
		}
	}
	u.Username = username
	u.UpdatedAt = time.Now().UTC()
	s.users[id] = u
	return nil
}

func (s *Store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[id]; !ok {
		return fmt.Errorf("user %q not found", id)
	}
	delete(s.users, id)
	return nil
}

func (s *Store) InvalidateTokens(ctx context.Context, id uuid.UUID, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.TokenInvalidatedAt = at
	u.UpdatedAt = time.Now().UTC()
	s.users[id] = u
	return nil
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.users), nil
}

func (s *Store) GetUserPreferences(ctx context.Context, id uuid.UUID) (*string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	u, ok := s.users[id]
	if !ok {
		return nil, nil
	}
	if u.Preferences == "" {
		return nil, nil
	}
	return &u.Preferences, nil
}

func (s *Store) PutUserPreferences(ctx context.Context, id uuid.UUID, prefs string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[id]
	if !ok {
		return fmt.Errorf("user %q not found", id)
	}
	u.Preferences = prefs
	u.UpdatedAt = time.Now().UTC()
	s.users[id] = u
	return nil
}

// Refresh tokens

func (s *Store) CreateRefreshToken(ctx context.Context, token config.RefreshToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refreshTokens[token.ID] = token
	return nil
}

func (s *Store) GetRefreshTokenByHash(ctx context.Context, tokenHash string) (*config.RefreshToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, rt := range s.refreshTokens {
		if rt.TokenHash == tokenHash {
			return &rt, nil
		}
	}
	return nil, nil
}

func (s *Store) ListRefreshTokens(ctx context.Context) ([]config.RefreshToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tokens := make([]config.RefreshToken, 0, len(s.refreshTokens))
	for _, rt := range s.refreshTokens {
		tokens = append(tokens, rt)
	}
	return tokens, nil
}

func (s *Store) DeleteRefreshToken(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.refreshTokens, id)
	return nil
}

func (s *Store) DeleteUserRefreshTokens(ctx context.Context, userID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, rt := range s.refreshTokens {
		if rt.UserID == userID {
			delete(s.refreshTokens, id)
		}
	}
	return nil
}

// Deep copy helpers

func copyFilterConfig(fc config.FilterConfig) config.FilterConfig {
	return config.FilterConfig{
		ID:         fc.ID,
		Name:       fc.Name,
		Expression: fc.Expression,
	}
}

func copyRotationPolicy(rp config.RotationPolicyConfig) config.RotationPolicyConfig {
	c := config.RotationPolicyConfig{
		ID:   rp.ID,
		Name: rp.Name,
	}
	if rp.MaxBytes != nil {
		c.MaxBytes = new(*rp.MaxBytes)
	}
	if rp.MaxAge != nil {
		c.MaxAge = new(*rp.MaxAge)
	}
	if rp.MaxRecords != nil {
		c.MaxRecords = new(*rp.MaxRecords)
	}
	if rp.Cron != nil {
		c.Cron = new(*rp.Cron)
	}
	return c
}

func copyRetentionPolicy(rp config.RetentionPolicyConfig) config.RetentionPolicyConfig {
	c := config.RetentionPolicyConfig{
		ID:   rp.ID,
		Name: rp.Name,
	}
	if rp.MaxAge != nil {
		c.MaxAge = new(*rp.MaxAge)
	}
	if rp.MaxBytes != nil {
		c.MaxBytes = new(*rp.MaxBytes)
	}
	if rp.MaxChunks != nil {
		c.MaxChunks = new(*rp.MaxChunks)
	}
	return c
}

func copyVaultConfig(st config.VaultConfig) config.VaultConfig {
	c := config.VaultConfig{
		ID:      st.ID,
		Name:    st.Name,
		Type:    st.Type,
		Params:  copyParams(st.Params),
		Enabled: st.Enabled,
	}
	if st.Filter != nil {
		c.Filter = new(*st.Filter)
	}
	if st.Policy != nil {
		c.Policy = new(*st.Policy)
	}
	if len(st.RetentionRules) > 0 {
		c.RetentionRules = make([]config.RetentionRule, len(st.RetentionRules))
		for i, b := range st.RetentionRules {
			c.RetentionRules[i] = config.RetentionRule{
				RetentionPolicyID: b.RetentionPolicyID,
				Action:            b.Action,
			}
			if b.Destination != nil {
				c.RetentionRules[i].Destination = new(*b.Destination)
			}
		}
	}
	return c
}

func copyIngesterConfig(ing config.IngesterConfig) config.IngesterConfig {
	return config.IngesterConfig{
		ID:      ing.ID,
		Name:    ing.Name,
		Type:    ing.Type,
		Enabled: ing.Enabled,
		Params:  copyParams(ing.Params),
	}
}

func copyCertPEM(cert config.CertPEM) config.CertPEM {
	return config.CertPEM{
		ID:       cert.ID,
		Name:     cert.Name,
		CertPEM:  cert.CertPEM,
		KeyPEM:   cert.KeyPEM,
		CertFile: cert.CertFile,
		KeyFile:  cert.KeyFile,
	}
}

func copyParams(params map[string]string) map[string]string {
	if params == nil {
		return nil
	}
	cp := make(map[string]string, len(params))
	maps.Copy(cp, params)
	return cp
}
