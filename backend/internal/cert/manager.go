// Package cert provides TLS certificate loading from PEM content stored in config.
package cert

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"gastrolog/internal/logging"
)

// CertSource holds certificate content: PEM or file paths. File paths take precedence when set.
type CertSource struct {
	CertPEM, KeyPEM   string
	CertFile, KeyFile string
}

// Manager loads and holds PEM certificate/key pairs from config store.
// Safe for concurrent use. Certs are identified by name (e.g. "server", "ingester.http").
// When CertFile/KeyFile are set, the manager watches those files and reloads on change.
type Manager struct {
	logger *slog.Logger

	mu sync.RWMutex
	// certs maps name -> certEntry with loaded tls.Certificate.
	certs map[string]*certEntry

	// defaultName is used when GetCertificate is called without SNI (e.g. single-cert server).
	defaultName string

	// fileSources: certs loaded from files, for reload on change.
	fileSources map[string]CertSource
	watcher    *fsnotify.Watcher
	watcherStop chan struct{}
}

// certEntry holds a loaded cert.
type certEntry struct {
	cert atomic.Pointer[tls.Certificate]
}

// Config holds Manager configuration.
type Config struct {
	Logger *slog.Logger
}

// New creates a new Manager.
func New(cfg Config) *Manager {
	return &Manager{
		logger:      logging.Default(cfg.Logger).With("component", "cert"),
		certs:       make(map[string]*certEntry),
		defaultName: "",
	}
}

// AddFromPEM registers a certificate from PEM content. Overwrites any existing entry with the same name.
func (m *Manager) AddFromPEM(name, certPEM, keyPEM string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		return err
	}

	entry := &certEntry{}
	entry.cert.Store(&cert)
	m.certs[name] = entry
	return nil
}

// Remove removes a certificate by name.
func (m *Manager) Remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.certs, name)
	if m.defaultName == name {
		m.defaultName = ""
	}
}

// SetDefault sets the cert name used when GetCertificate is called without SNI.
// Used for single-cert servers that don't do SNI.
func (m *Manager) SetDefault(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultName = name
}

// LoadFromConfig replaces all certs with the given config. Used at startup and when config changes.
// When CertFile/KeyFile are set, loads from disk and watches for changes; otherwise uses CertPEM/KeyPEM.
func (m *Manager) LoadFromConfig(defaultCert string, certs map[string]CertSource) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopWatcher()

	m.certs = make(map[string]*certEntry)
	m.defaultName = defaultCert
	m.fileSources = make(map[string]CertSource)

	for name, src := range certs {
		certPEM, keyPEM := src.CertPEM, src.KeyPEM
		if src.CertFile != "" && src.KeyFile != "" {
			m.fileSources[name] = src
			var err error
			certPEM, keyPEM, err = m.loadFromFiles(src.CertFile, src.KeyFile)
			if err != nil {
				m.logger.Warn("load cert from files failed", "name", name, "error", err)
				continue
			}
		}
		if certPEM == "" || keyPEM == "" {
			continue
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			m.logger.Warn("load cert failed", "name", name, "error", err)
			continue
		}
		entry := &certEntry{}
		entry.cert.Store(&cert)
		m.certs[name] = entry
	}

	if len(m.fileSources) > 0 {
		m.startWatcher()
	}
	return nil
}

// stopWatcher stops the file watcher. Caller must hold m.mu.
func (m *Manager) stopWatcher() {
	if m.watcherStop != nil {
		close(m.watcherStop)
		m.watcherStop = nil
	}
	if m.watcher != nil {
		m.watcher.Close()
		m.watcher = nil
	}
}

// startWatcher starts watching file-based certs. Caller must hold m.mu.
func (m *Manager) startWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		m.logger.Warn("fsnotify start failed", "error", err)
		return
	}
	m.watcher = watcher
	m.watcherStop = make(chan struct{})

	// path -> cert name for reload
	pathToName := make(map[string]string)
	for name, src := range m.fileSources {
		pathToName[src.CertFile] = name
		pathToName[src.KeyFile] = name
		if err := watcher.Add(src.CertFile); err != nil {
			m.logger.Warn("watch cert file", "file", src.CertFile, "error", err)
		}
		if err := watcher.Add(src.KeyFile); err != nil {
			m.logger.Warn("watch key file", "file", src.KeyFile, "error", err)
		}
	}

	go func() {
		defer watcher.Close()
		for {
			select {
			case <-m.watcherStop:
				return
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				m.logger.Warn("watcher error", "error", err)
			case ev, ok := <-watcher.Events:
				if !ok {
					return
				}
				if ev.Op&(fsnotify.Write|fsnotify.Create) == 0 {
					continue
				}
				name, ok := pathToName[ev.Name]
				if !ok {
					continue
				}
				m.reloadFileCert(name)
			}
		}
	}()
}

// reloadFileCert reloads a cert from its file paths. Called from watcher goroutine.
func (m *Manager) reloadFileCert(name string) {
	m.mu.RLock()
	src, ok := m.fileSources[name]
	m.mu.RUnlock()
	if !ok {
		return
	}
	certPEM, keyPEM, err := m.loadFromFiles(src.CertFile, src.KeyFile)
	if err != nil {
		m.logger.Warn("reload cert from files failed", "name", name, "error", err)
		return
	}
	cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		m.logger.Warn("reload cert parse failed", "name", name, "error", err)
		return
	}
	m.mu.Lock()
	if entry, ok := m.certs[name]; ok {
		entry.cert.Store(&cert)
	}
	m.mu.Unlock()
}

// loadFromFiles reads cert and key from disk.
func (m *Manager) loadFromFiles(certFile, keyFile string) (certPEM, keyPEM string, err error) {
	certB, err := os.ReadFile(certFile)
	if err != nil {
		return "", "", fmt.Errorf("read cert: %w", err)
	}
	keyB, err := os.ReadFile(keyFile)
	if err != nil {
		return "", "", fmt.Errorf("read key: %w", err)
	}
	return string(certB), string(keyB), nil
}

// GetCertificate returns a tls.Config.GetCertificate callback for server-side TLS.
// Uses default cert when clientHello.ServerName is empty. Otherwise looks up by SNI.
func (m *Manager) GetCertificate(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := clientHello.ServerName
	if name == "" {
		m.mu.RLock()
		name = m.defaultName
		m.mu.RUnlock()
	}
	if name == "" {
		return nil, nil
	}

	m.mu.RLock()
	entry, ok := m.certs[name]
	m.mu.RUnlock()

	if !ok {
		return nil, nil
	}
	c := entry.cert.Load()
	if c == nil {
		return nil, nil
	}
	return c, nil
}

// Certificate returns the current certificate for the given name, or nil if not found.
func (m *Manager) Certificate(name string) *tls.Certificate {
	m.mu.RLock()
	entry, ok := m.certs[name]
	m.mu.RUnlock()

	if !ok {
		return nil
	}
	return entry.cert.Load()
}

// TLSConfig returns a tls.Config that uses this manager for GetCertificate.
// Caller may set additional fields (MinVersion, CipherSuites, etc.).
func (m *Manager) TLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: m.GetCertificate,
	}
}
