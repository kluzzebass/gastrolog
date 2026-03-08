package config

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrJoining is returned by the proxy when a cluster join is in progress.
// During the join, the underlying store is being replaced and cannot serve requests.
var ErrJoining = errors.New("cluster join in progress, please retry shortly")

// StoreProxy is a mutex-guarded wrapper implementing Store + io.Closer that
// delegates to an inner store and allows atomic swap. All consumers hold a
// reference to the proxy; on join, only the proxy's inner pointer changes.
type StoreProxy struct {
	mu      sync.RWMutex
	inner   Store
	joining bool
}

// NewStoreProxy creates a StoreProxy delegating to inner.
func NewStoreProxy(inner Store) *StoreProxy {
	return &StoreProxy{inner: inner}
}

// Swap replaces the inner store and returns the old one. Clears the joining flag.
func (p *StoreProxy) Swap(newInner Store) Store {
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.inner
	p.inner = newInner
	p.joining = false
	return old
}

// SetJoining sets the joining flag. While set, all methods return ErrJoining.
func (p *StoreProxy) SetJoining() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.joining = true
}

// ClearJoining clears the joining flag without swapping the store.
func (p *StoreProxy) ClearJoining() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.joining = false
}

// Inner returns the current inner store.
func (p *StoreProxy) Inner() Store {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.inner
}

func (p *StoreProxy) check() error {
	if p.joining {
		return ErrJoining
	}
	return nil
}

// Close delegates to the inner store if it implements io.Closer.
func (p *StoreProxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	type closer interface{ Close() error }
	if c, ok := p.inner.(closer); ok {
		return c.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Store interface delegation — all methods: RLock → check joining → delegate
// ---------------------------------------------------------------------------

func (p *StoreProxy) Load(ctx context.Context) (*Config, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.Load(ctx)
}

func (p *StoreProxy) GetFilter(ctx context.Context, id uuid.UUID) (*FilterConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetFilter(ctx, id)
}

func (p *StoreProxy) ListFilters(ctx context.Context) ([]FilterConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListFilters(ctx)
}

func (p *StoreProxy) PutFilter(ctx context.Context, cfg FilterConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutFilter(ctx, cfg)
}

func (p *StoreProxy) DeleteFilter(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteFilter(ctx, id)
}

func (p *StoreProxy) GetRotationPolicy(ctx context.Context, id uuid.UUID) (*RotationPolicyConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetRotationPolicy(ctx, id)
}

func (p *StoreProxy) ListRotationPolicies(ctx context.Context) ([]RotationPolicyConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListRotationPolicies(ctx)
}

func (p *StoreProxy) PutRotationPolicy(ctx context.Context, cfg RotationPolicyConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutRotationPolicy(ctx, cfg)
}

func (p *StoreProxy) DeleteRotationPolicy(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteRotationPolicy(ctx, id)
}

func (p *StoreProxy) GetRetentionPolicy(ctx context.Context, id uuid.UUID) (*RetentionPolicyConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetRetentionPolicy(ctx, id)
}

func (p *StoreProxy) ListRetentionPolicies(ctx context.Context) ([]RetentionPolicyConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListRetentionPolicies(ctx)
}

func (p *StoreProxy) PutRetentionPolicy(ctx context.Context, cfg RetentionPolicyConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutRetentionPolicy(ctx, cfg)
}

func (p *StoreProxy) DeleteRetentionPolicy(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteRetentionPolicy(ctx, id)
}

func (p *StoreProxy) GetVault(ctx context.Context, id uuid.UUID) (*VaultConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetVault(ctx, id)
}

func (p *StoreProxy) ListVaults(ctx context.Context) ([]VaultConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListVaults(ctx)
}

func (p *StoreProxy) PutVault(ctx context.Context, cfg VaultConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutVault(ctx, cfg)
}

func (p *StoreProxy) DeleteVault(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteVault(ctx, id)
}

func (p *StoreProxy) GetIngester(ctx context.Context, id uuid.UUID) (*IngesterConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetIngester(ctx, id)
}

func (p *StoreProxy) ListIngesters(ctx context.Context) ([]IngesterConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListIngesters(ctx)
}

func (p *StoreProxy) PutIngester(ctx context.Context, cfg IngesterConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutIngester(ctx, cfg)
}

func (p *StoreProxy) DeleteIngester(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteIngester(ctx, id)
}

func (p *StoreProxy) GetRoute(ctx context.Context, id uuid.UUID) (*RouteConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetRoute(ctx, id)
}

func (p *StoreProxy) ListRoutes(ctx context.Context) ([]RouteConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListRoutes(ctx)
}

func (p *StoreProxy) PutRoute(ctx context.Context, cfg RouteConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutRoute(ctx, cfg)
}

func (p *StoreProxy) DeleteRoute(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteRoute(ctx, id)
}

func (p *StoreProxy) GetLookupFile(ctx context.Context, id uuid.UUID) (*LookupFileConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetLookupFile(ctx, id)
}

func (p *StoreProxy) ListLookupFiles(ctx context.Context) ([]LookupFileConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListLookupFiles(ctx)
}

func (p *StoreProxy) PutLookupFile(ctx context.Context, cfg LookupFileConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutLookupFile(ctx, cfg)
}

func (p *StoreProxy) DeleteLookupFile(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteLookupFile(ctx, id)
}

func (p *StoreProxy) LoadServerSettings(ctx context.Context) (ServerSettings, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return ServerSettings{}, err
	}
	return p.inner.LoadServerSettings(ctx)
}

func (p *StoreProxy) SaveServerSettings(ctx context.Context, ss ServerSettings) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.SaveServerSettings(ctx, ss)
}

func (p *StoreProxy) GetNode(ctx context.Context, id uuid.UUID) (*NodeConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetNode(ctx, id)
}

func (p *StoreProxy) ListNodes(ctx context.Context) ([]NodeConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListNodes(ctx)
}

func (p *StoreProxy) PutNode(ctx context.Context, node NodeConfig) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutNode(ctx, node)
}

func (p *StoreProxy) DeleteNode(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteNode(ctx, id)
}

func (p *StoreProxy) PutClusterTLS(ctx context.Context, tls ClusterTLS) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutClusterTLS(ctx, tls)
}

func (p *StoreProxy) ListCertificates(ctx context.Context) ([]CertPEM, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListCertificates(ctx)
}

func (p *StoreProxy) GetCertificate(ctx context.Context, id uuid.UUID) (*CertPEM, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetCertificate(ctx, id)
}

func (p *StoreProxy) PutCertificate(ctx context.Context, cert CertPEM) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutCertificate(ctx, cert)
}

func (p *StoreProxy) DeleteCertificate(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteCertificate(ctx, id)
}

func (p *StoreProxy) CreateUser(ctx context.Context, user User) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.CreateUser(ctx, user)
}

func (p *StoreProxy) GetUser(ctx context.Context, id uuid.UUID) (*User, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetUser(ctx, id)
}

func (p *StoreProxy) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetUserByUsername(ctx, username)
}

func (p *StoreProxy) ListUsers(ctx context.Context) ([]User, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListUsers(ctx)
}

func (p *StoreProxy) UpdatePassword(ctx context.Context, id uuid.UUID, passwordHash string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.UpdatePassword(ctx, id, passwordHash)
}

func (p *StoreProxy) UpdateUserRole(ctx context.Context, id uuid.UUID, role string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.UpdateUserRole(ctx, id, role)
}

func (p *StoreProxy) UpdateUsername(ctx context.Context, id uuid.UUID, username string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.UpdateUsername(ctx, id, username)
}

func (p *StoreProxy) DeleteUser(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteUser(ctx, id)
}

func (p *StoreProxy) InvalidateTokens(ctx context.Context, id uuid.UUID, at time.Time) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.InvalidateTokens(ctx, id, at)
}

func (p *StoreProxy) CountUsers(ctx context.Context) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return 0, err
	}
	return p.inner.CountUsers(ctx)
}

func (p *StoreProxy) GetUserPreferences(ctx context.Context, id uuid.UUID) (*string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetUserPreferences(ctx, id)
}

func (p *StoreProxy) PutUserPreferences(ctx context.Context, id uuid.UUID, prefs string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.PutUserPreferences(ctx, id, prefs)
}

func (p *StoreProxy) CreateRefreshToken(ctx context.Context, token RefreshToken) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.CreateRefreshToken(ctx, token)
}

func (p *StoreProxy) GetRefreshTokenByHash(ctx context.Context, tokenHash string) (*RefreshToken, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.GetRefreshTokenByHash(ctx, tokenHash)
}

func (p *StoreProxy) ListRefreshTokens(ctx context.Context) ([]RefreshToken, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.inner.ListRefreshTokens(ctx)
}

func (p *StoreProxy) DeleteRefreshToken(ctx context.Context, id uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteRefreshToken(ctx, id)
}

func (p *StoreProxy) DeleteUserRefreshTokens(ctx context.Context, userID uuid.UUID) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.check(); err != nil {
		return err
	}
	return p.inner.DeleteUserRefreshTokens(ctx, userID)
}
