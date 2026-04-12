// Package memory provides an in-memory ConfigStore implementation.
// Intended for testing. Configuration is not persisted across restarts.
package memory

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"gastrolog/internal/system"

	"github.com/google/uuid"
)

// cmpUUID compares two UUIDs lexicographically. Since gastrolog uses UUIDv7,
// byte order equals creation order.
func cmpUUID(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) }

// collectAndSort copies values from a map, applies a per-element transform,
// and sorts the result. Used by Load to deep-copy + sort each config entity type.
func collectAndSort[K comparable, V any](m map[K]V, transform func(V) V, less func(V, V) int) []V {
	if len(m) == 0 {
		return nil
	}
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, transform(v))
	}
	slices.SortFunc(out, less)
	return out
}

// serverSettings holds the typed server-level config fields.
type serverSettings struct {
	ss                system.ServerSettings
	hasServerSettings bool // true once SaveServerSettings has been called at least once
}

// Store is an in-memory ConfigStore implementation.
type Store struct {
	mu                sync.RWMutex
	filters           map[uuid.UUID]system.FilterConfig
	rotationPolicies  map[uuid.UUID]system.RotationPolicyConfig
	retentionPolicies map[uuid.UUID]system.RetentionPolicyConfig
	vaults            map[uuid.UUID]system.VaultConfig
	ingesters         map[uuid.UUID]system.IngesterConfig
	routes            map[uuid.UUID]system.RouteConfig
	ss                serverSettings
	certs             map[uuid.UUID]system.CertPEM
	users         map[uuid.UUID]system.User         // keyed by ID (UUID)
	refreshTokens map[uuid.UUID]system.RefreshToken // keyed by token ID
	nodes         map[uuid.UUID]system.NodeConfig    // keyed by node ID
	managedFiles       map[uuid.UUID]system.ManagedFileConfig
	cloudServices      map[uuid.UUID]system.CloudService
	tiers              map[uuid.UUID]system.TierConfig
	nodeStorageConfigs map[string]system.NodeStorageConfig // keyed by nodeID
	clusterTLS         *system.ClusterTLS
}

var _ system.Store = (*Store)(nil)

// NewStore creates a new in-memory ConfigStore.
func NewStore() *Store {
	return &Store{
		filters:           make(map[uuid.UUID]system.FilterConfig),
		rotationPolicies:  make(map[uuid.UUID]system.RotationPolicyConfig),
		retentionPolicies: make(map[uuid.UUID]system.RetentionPolicyConfig),
		vaults:            make(map[uuid.UUID]system.VaultConfig),
		ingesters:         make(map[uuid.UUID]system.IngesterConfig),
		routes:            make(map[uuid.UUID]system.RouteConfig),
		certs:             make(map[uuid.UUID]system.CertPEM),
		users:         make(map[uuid.UUID]system.User),
		refreshTokens: make(map[uuid.UUID]system.RefreshToken),
		nodes:         make(map[uuid.UUID]system.NodeConfig),
		managedFiles:       make(map[uuid.UUID]system.ManagedFileConfig),
		cloudServices:      make(map[uuid.UUID]system.CloudService),
		tiers:              make(map[uuid.UUID]system.TierConfig),
		nodeStorageConfigs: make(map[string]system.NodeStorageConfig),
	}
}

// isEmpty reports whether the store has any entities at all.
func (s *Store) isEmpty() bool {
	return len(s.filters) == 0 && len(s.rotationPolicies) == 0 &&
		len(s.retentionPolicies) == 0 && len(s.vaults) == 0 &&
		len(s.ingesters) == 0 && len(s.routes) == 0 &&
		len(s.managedFiles) == 0 && len(s.cloudServices) == 0 &&
		len(s.tiers) == 0 && len(s.nodeStorageConfigs) == 0 &&
		!s.ss.hasServerSettings && s.clusterTLS == nil
}

// Load returns the full configuration.
// Returns nil if no entities exist.
func (s *Store) Load(ctx context.Context) (*system.Config, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isEmpty() {
		return nil, nil
	}

	cfg := &system.Config{}

	cfg.Filters = collectAndSort(s.filters, copyFilterConfig, func(a, b system.FilterConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.RotationPolicies = collectAndSort(s.rotationPolicies, copyRotationPolicy, func(a, b system.RotationPolicyConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.RetentionPolicies = collectAndSort(s.retentionPolicies, copyRetentionPolicy, func(a, b system.RetentionPolicyConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.Vaults = collectAndSort(s.vaults, copyVaultConfig, func(a, b system.VaultConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.Ingesters = collectAndSort(s.ingesters, copyIngesterConfig, func(a, b system.IngesterConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.Routes = collectAndSort(s.routes, copyRouteConfig, func(a, b system.RouteConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.ManagedFiles = collectAndSort(s.managedFiles, func(v system.ManagedFileConfig) system.ManagedFileConfig { return v }, func(a, b system.ManagedFileConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.CloudServices = collectAndSort(s.cloudServices, copyCloudService, func(a, b system.CloudService) int { return cmpUUID(a.ID, b.ID) })
	cfg.Tiers = collectAndSort(s.tiers, copyTierConfig, func(a, b system.TierConfig) int { return cmpUUID(a.ID, b.ID) })
	cfg.NodeStorageConfigs = collectAndSort(s.nodeStorageConfigs, copyNodeStorageConfig, func(a, b system.NodeStorageConfig) int { return cmp.Compare(a.NodeID, b.NodeID) })
	cfg.Certs = collectAndSort(s.certs, copyCertPEM, func(a, b system.CertPEM) int { return cmpUUID(a.ID, b.ID) })
	cfg.Nodes = collectAndSort(s.nodes, func(v system.NodeConfig) system.NodeConfig { return v }, func(a, b system.NodeConfig) int { return cmpUUID(a.ID, b.ID) })

	// Populate server settings on Config.
	if s.ss.hasServerSettings {
		cfg.Auth = s.ss.ss.Auth
		cfg.Query = s.ss.ss.Query
		cfg.Scheduler = s.ss.ss.Scheduler
		cfg.TLS = s.ss.ss.TLS
		cfg.Lookup = s.ss.ss.Lookup
		cfg.Cluster = s.ss.ss.Cluster
		cfg.SetupWizardDismissed = s.ss.ss.SetupWizardDismissed
	}

	// Include ClusterTLS if loaded (cluster mode).
	if s.clusterTLS != nil {
		c := *s.clusterTLS
		cfg.ClusterTLS = &c
	}

	return cfg, nil
}

// Filters

func (s *Store) GetFilter(ctx context.Context, id uuid.UUID) (*system.FilterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fc, ok := s.filters[id]
	if !ok {
		return nil, nil
	}
	c := copyFilterConfig(fc)
	return &c, nil
}

func (s *Store) ListFilters(ctx context.Context) ([]system.FilterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.FilterConfig, 0, len(s.filters))
	for _, fc := range s.filters {
		result = append(result, copyFilterConfig(fc))
	}
	slices.SortFunc(result, func(a, b system.FilterConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutFilter(ctx context.Context, cfg system.FilterConfig) error {
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

func (s *Store) GetRotationPolicy(ctx context.Context, id uuid.UUID) (*system.RotationPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rp, ok := s.rotationPolicies[id]
	if !ok {
		return nil, nil
	}
	c := copyRotationPolicy(rp)
	return &c, nil
}

func (s *Store) ListRotationPolicies(ctx context.Context) ([]system.RotationPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.RotationPolicyConfig, 0, len(s.rotationPolicies))
	for _, rp := range s.rotationPolicies {
		result = append(result, copyRotationPolicy(rp))
	}
	slices.SortFunc(result, func(a, b system.RotationPolicyConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutRotationPolicy(ctx context.Context, cfg system.RotationPolicyConfig) error {
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

func (s *Store) GetRetentionPolicy(ctx context.Context, id uuid.UUID) (*system.RetentionPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rp, ok := s.retentionPolicies[id]
	if !ok {
		return nil, nil
	}
	c := copyRetentionPolicy(rp)
	return &c, nil
}

func (s *Store) ListRetentionPolicies(ctx context.Context) ([]system.RetentionPolicyConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.RetentionPolicyConfig, 0, len(s.retentionPolicies))
	for _, rp := range s.retentionPolicies {
		result = append(result, copyRetentionPolicy(rp))
	}
	slices.SortFunc(result, func(a, b system.RetentionPolicyConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutRetentionPolicy(ctx context.Context, cfg system.RetentionPolicyConfig) error {
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

func (s *Store) GetVault(ctx context.Context, id uuid.UUID) (*system.VaultConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st, ok := s.vaults[id]
	if !ok {
		return nil, nil
	}
	c := copyVaultConfig(st)
	return &c, nil
}

func (s *Store) ListVaults(ctx context.Context) ([]system.VaultConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.VaultConfig, 0, len(s.vaults))
	for _, st := range s.vaults {
		result = append(result, copyVaultConfig(st))
	}
	slices.SortFunc(result, func(a, b system.VaultConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutVault(ctx context.Context, cfg system.VaultConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.vaults[cfg.ID] = copyVaultConfig(cfg)
	return nil
}

func (s *Store) DeleteVault(ctx context.Context, id uuid.UUID, _ bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.vaults, id)
	return nil
}

// Ingesters

func (s *Store) GetIngester(ctx context.Context, id uuid.UUID) (*system.IngesterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ing, ok := s.ingesters[id]
	if !ok {
		return nil, nil
	}
	c := copyIngesterConfig(ing)
	return &c, nil
}

func (s *Store) ListIngesters(ctx context.Context) ([]system.IngesterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.IngesterConfig, 0, len(s.ingesters))
	for _, ing := range s.ingesters {
		result = append(result, copyIngesterConfig(ing))
	}
	slices.SortFunc(result, func(a, b system.IngesterConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutIngester(ctx context.Context, cfg system.IngesterConfig) error {
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

// Routes

func (s *Store) GetRoute(ctx context.Context, id uuid.UUID) (*system.RouteConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rt, ok := s.routes[id]
	if !ok {
		return nil, nil
	}
	c := copyRouteConfig(rt)
	return &c, nil
}

func (s *Store) ListRoutes(ctx context.Context) ([]system.RouteConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.RouteConfig, 0, len(s.routes))
	for _, rt := range s.routes {
		result = append(result, copyRouteConfig(rt))
	}
	slices.SortFunc(result, func(a, b system.RouteConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutRoute(ctx context.Context, cfg system.RouteConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.routes[cfg.ID] = copyRouteConfig(cfg)
	return nil
}

func (s *Store) DeleteRoute(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.routes, id)
	return nil
}

// Managed files

func (s *Store) GetManagedFile(ctx context.Context, id uuid.UUID) (*system.ManagedFileConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lf, ok := s.managedFiles[id]
	if !ok {
		return nil, nil
	}
	return &lf, nil
}

func (s *Store) ListManagedFiles(ctx context.Context) ([]system.ManagedFileConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.ManagedFileConfig, 0, len(s.managedFiles))
	for _, lf := range s.managedFiles {
		result = append(result, lf)
	}
	slices.SortFunc(result, func(a, b system.ManagedFileConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutManagedFile(ctx context.Context, cfg system.ManagedFileConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.managedFiles[cfg.ID] = cfg
	return nil
}

func (s *Store) DeleteManagedFile(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.managedFiles, id)
	return nil
}

// Server settings

func (s *Store) LoadServerSettings(ctx context.Context) (system.ServerSettings, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.ss.ss, nil
}

func (s *Store) SaveServerSettings(ctx context.Context, ss system.ServerSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ss = serverSettings{
		ss:                ss,
		hasServerSettings: true,
	}
	return nil
}

// Nodes

func (s *Store) GetNode(ctx context.Context, id uuid.UUID) (*system.NodeConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.nodes[id]
	if !ok {
		return nil, nil
	}
	return &n, nil
}

func (s *Store) ListNodes(ctx context.Context) ([]system.NodeConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.NodeConfig, 0, len(s.nodes))
	for _, n := range s.nodes {
		result = append(result, n)
	}
	slices.SortFunc(result, func(a, b system.NodeConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutNode(ctx context.Context, node system.NodeConfig) error {
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

func (s *Store) PutClusterTLS(ctx context.Context, tls system.ClusterTLS) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clusterTLS = &tls
	return nil
}

// Cloud services

func (s *Store) GetCloudService(ctx context.Context, id uuid.UUID) (*system.CloudService, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cs, ok := s.cloudServices[id]
	if !ok {
		return nil, nil
	}
	return &cs, nil
}

func (s *Store) ListCloudServices(ctx context.Context) ([]system.CloudService, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.CloudService, 0, len(s.cloudServices))
	for _, cs := range s.cloudServices {
		result = append(result, cs)
	}
	slices.SortFunc(result, func(a, b system.CloudService) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutCloudService(ctx context.Context, svc system.CloudService) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cloudServices[svc.ID] = svc
	return nil
}

func (s *Store) DeleteCloudService(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cloudServices, id)
	return nil
}

// Tiers

func (s *Store) GetTier(ctx context.Context, id uuid.UUID) (*system.TierConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tier, ok := s.tiers[id]
	if !ok {
		return nil, nil
	}
	return &tier, nil
}

func (s *Store) ListTiers(ctx context.Context) ([]system.TierConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.TierConfig, 0, len(s.tiers))
	for _, tier := range s.tiers {
		result = append(result, tier)
	}
	slices.SortFunc(result, func(a, b system.TierConfig) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) PutTier(ctx context.Context, tier system.TierConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tiers[tier.ID] = tier
	return nil
}

func (s *Store) DeleteTier(ctx context.Context, id uuid.UUID, _ bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.tiers, id)
	return nil
}

// Node storage configs

func (s *Store) GetNodeStorageConfig(ctx context.Context, nodeID string) (*system.NodeStorageConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nsc, ok := s.nodeStorageConfigs[nodeID]
	if !ok {
		return nil, nil
	}
	c := copyNodeStorageConfig(nsc)
	return &c, nil
}

func (s *Store) ListNodeStorageConfigs(ctx context.Context) ([]system.NodeStorageConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.NodeStorageConfig, 0, len(s.nodeStorageConfigs))
	for _, nsc := range s.nodeStorageConfigs {
		result = append(result, copyNodeStorageConfig(nsc))
	}
	slices.SortFunc(result, func(a, b system.NodeStorageConfig) int {
		return cmp.Compare(a.NodeID, b.NodeID)
	})
	return result, nil
}

func (s *Store) SetNodeStorageConfig(ctx context.Context, cfg system.NodeStorageConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodeStorageConfigs[cfg.NodeID] = copyNodeStorageConfig(cfg)
	return nil
}

// Certificates

func (s *Store) ListCertificates(ctx context.Context) ([]system.CertPEM, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]system.CertPEM, 0, len(s.certs))
	for _, cert := range s.certs {
		result = append(result, copyCertPEM(cert))
	}
	slices.SortFunc(result, func(a, b system.CertPEM) int { return cmpUUID(a.ID, b.ID) })
	return result, nil
}

func (s *Store) GetCertificate(ctx context.Context, id uuid.UUID) (*system.CertPEM, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pem, ok := s.certs[id]
	if !ok {
		return nil, nil
	}
	c := copyCertPEM(pem)
	return &c, nil
}

func (s *Store) PutCertificate(ctx context.Context, cert system.CertPEM) error {
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

func (s *Store) CreateUser(ctx context.Context, user system.User) error {
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

func (s *Store) GetUser(ctx context.Context, id uuid.UUID) (*system.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	u, ok := s.users[id]
	if !ok {
		return nil, nil
	}
	return &u, nil
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (*system.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, u := range s.users {
		if u.Username == username {
			return &u, nil
		}
	}
	return nil, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]system.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]system.User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	slices.SortFunc(users, func(a, b system.User) int { return cmpUUID(a.ID, b.ID) })
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

func (s *Store) CreateRefreshToken(ctx context.Context, token system.RefreshToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refreshTokens[token.ID] = token
	return nil
}

func (s *Store) GetRefreshTokenByHash(ctx context.Context, tokenHash string) (*system.RefreshToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, rt := range s.refreshTokens {
		if rt.TokenHash == tokenHash {
			return &rt, nil
		}
	}
	return nil, nil
}

func (s *Store) ListRefreshTokens(ctx context.Context) ([]system.RefreshToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tokens := make([]system.RefreshToken, 0, len(s.refreshTokens))
	for _, rt := range s.refreshTokens {
		tokens = append(tokens, rt)
	}
	slices.SortFunc(tokens, func(a, b system.RefreshToken) int { return cmpUUID(a.ID, b.ID) })
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

func copyFilterConfig(fc system.FilterConfig) system.FilterConfig {
	return system.FilterConfig{
		ID:         fc.ID,
		Name:       fc.Name,
		Expression: fc.Expression,
	}
}

func copyRotationPolicy(rp system.RotationPolicyConfig) system.RotationPolicyConfig {
	c := system.RotationPolicyConfig{
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

func copyRetentionPolicy(rp system.RetentionPolicyConfig) system.RetentionPolicyConfig {
	c := system.RetentionPolicyConfig{
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

func copyVaultConfig(st system.VaultConfig) system.VaultConfig {
	return system.VaultConfig{
		ID:      st.ID,
		Name:    st.Name,
		Enabled: st.Enabled,
	}
}

func copyIngesterConfig(ing system.IngesterConfig) system.IngesterConfig {
	return system.IngesterConfig{
		ID:      ing.ID,
		Name:    ing.Name,
		Type:    ing.Type,
		Enabled: ing.Enabled,
		Params:  copyParams(ing.Params),
		NodeID:  ing.NodeID,
	}
}

func copyRouteConfig(rt system.RouteConfig) system.RouteConfig {
	c := system.RouteConfig{
		ID:           rt.ID,
		Name:         rt.Name,
		Distribution: rt.Distribution,
		Enabled:      rt.Enabled,
		EjectOnly:    rt.EjectOnly,
	}
	if rt.FilterID != nil {
		c.FilterID = new(*rt.FilterID)
	}
	if len(rt.Destinations) > 0 {
		c.Destinations = make([]uuid.UUID, len(rt.Destinations))
		copy(c.Destinations, rt.Destinations)
	}
	return c
}

func copyCertPEM(cert system.CertPEM) system.CertPEM {
	return system.CertPEM{
		ID:       cert.ID,
		Name:     cert.Name,
		CertPEM:  cert.CertPEM,
		KeyPEM:   cert.KeyPEM,
		CertFile: cert.CertFile,
		KeyFile:  cert.KeyFile,
	}
}

func copyNodeStorageConfig(nsc system.NodeStorageConfig) system.NodeStorageConfig {
	c := system.NodeStorageConfig{
		NodeID: nsc.NodeID,
	}
	if len(nsc.FileStorages) > 0 {
		c.FileStorages = make([]system.FileStorage, len(nsc.FileStorages))
		copy(c.FileStorages, nsc.FileStorages)
	}
	return c
}

func copyTierConfig(tc system.TierConfig) system.TierConfig {
	c := tc
	if tc.RotationPolicyID != nil {
		id := *tc.RotationPolicyID
		c.RotationPolicyID = &id
	}
	if tc.CloudServiceID != nil {
		id := *tc.CloudServiceID
		c.CloudServiceID = &id
	}
	if len(tc.RetentionRules) > 0 {
		c.RetentionRules = make([]system.RetentionRule, len(tc.RetentionRules))
		copy(c.RetentionRules, tc.RetentionRules)
	}
	if len(tc.Placements) > 0 {
		c.Placements = make([]system.TierPlacement, len(tc.Placements))
		copy(c.Placements, tc.Placements)
	}
	return c
}

func copyCloudService(cs system.CloudService) system.CloudService {
	c := cs
	if len(cs.Transitions) > 0 {
		c.Transitions = make([]system.CloudStorageTransition, len(cs.Transitions))
		copy(c.Transitions, cs.Transitions)
	}
	return c
}

func copyParams(params map[string]string) map[string]string {
	if params == nil {
		return nil
	}
	cp := make(map[string]string, len(params))
	maps.Copy(cp, params)
	return cp
}
