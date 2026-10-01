package cert

import (
	"context"
	"fmt"

	"gastrolog/internal/system"
)

// ReloadFromStore mirrors the config store's certificates into the manager —
// the ONE fill path, used at boot, by the certificate-change dispatcher leg
// on every node, and nowhere else. Certificates are keyed by name, falling
// back to the ID for unnamed ones; names are what every consumer resolves
// (ingester tls_cert/tls_ca params, the default serving certificate), and
// the boot path keying by ID while the old RPC-local reload keyed by name is
// exactly how restarts broke name lookups.
func ReloadFromStore(ctx context.Context, mgr *Manager, store system.Store) error {
	certList, err := store.ListCertificates(ctx)
	if err != nil {
		return fmt.Errorf("list certificates: %w", err)
	}
	certs := make(map[string]CertSource, len(certList))
	for _, c := range certList {
		key := c.Name
		if key == "" {
			key = c.ID.String()
		}
		certs[key] = CertSource{CertPEM: c.CertPEM, KeyPEM: c.KeyPEM, CertFile: c.CertFile, KeyFile: c.KeyFile}
	}
	ss, err := store.LoadServerSettings(ctx)
	if err != nil {
		return fmt.Errorf("load server settings for TLS: %w", err)
	}
	if err := mgr.LoadFromConfig(ss.TLS.DefaultCert, certs); err != nil {
		return fmt.Errorf("load certs: %w", err)
	}
	return nil
}
