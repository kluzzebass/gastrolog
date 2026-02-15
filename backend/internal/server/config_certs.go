package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cert"
	"gastrolog/internal/config"
)

// reloadCertManager lists all certs from the store and loads them into the cert manager.
func (s *ConfigServer) reloadCertManager(ctx context.Context) error {
	if s.certManager == nil {
		return nil
	}
	sc, err := config.LoadServerConfig(ctx, s.cfgStore)
	if err != nil {
		return fmt.Errorf("load server config: %w", err)
	}
	certList, err := s.cfgStore.ListCertificates(ctx)
	if err != nil {
		return fmt.Errorf("list certificates: %w", err)
	}
	certs := make(map[string]cert.CertSource, len(certList))
	for _, c := range certList {
		certs[c.Name] = cert.CertSource{CertPEM: c.CertPEM, KeyPEM: c.KeyPEM, CertFile: c.CertFile, KeyFile: c.KeyFile}
	}
	return s.certManager.LoadFromConfig(sc.TLS.DefaultCert, certs)
}

// ListCertificates returns all certificate names.
func (s *ConfigServer) ListCertificates(
	ctx context.Context,
	req *connect.Request[apiv1.ListCertificatesRequest],
) (*connect.Response[apiv1.ListCertificatesResponse], error) {
	certs, err := s.cfgStore.ListCertificates(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	infos := make([]*apiv1.CertificateInfo, len(certs))
	for i, c := range certs {
		infos[i] = &apiv1.CertificateInfo{Id: c.ID.String(), Name: c.Name}
	}
	return connect.NewResponse(&apiv1.ListCertificatesResponse{Certificates: infos}), nil
}

// findCertByName returns the certificate with the given name, or nil if not found.
func (s *ConfigServer) findCertByName(ctx context.Context, name string) (*config.CertPEM, error) {
	certs, err := s.cfgStore.ListCertificates(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range certs {
		if c.Name == name {
			return &c, nil
		}
	}
	return nil, nil
}

// GetCertificate returns a certificate by ID.
func (s *ConfigServer) GetCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.GetCertificateRequest],
) (*connect.Response[apiv1.GetCertificateResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	pem, err := s.cfgStore.GetCertificate(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if pem == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("certificate not found"))
	}
	return connect.NewResponse(&apiv1.GetCertificateResponse{
		Id:       pem.ID.String(),
		Name:     pem.Name,
		CertPem:  pem.CertPEM,
		KeyPem:   "", // Never expose private keys via API
		CertFile: pem.CertFile,
		KeyFile:  pem.KeyFile,
	}), nil
}

// PutCertificate creates or updates a certificate.
func (s *ConfigServer) PutCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.PutCertificateRequest],
) (*connect.Response[apiv1.PutCertificateResponse], error) {
	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name required"))
	}

	// Load existing cert by ID (if given) or by name for key-reuse logic.
	var existing *config.CertPEM
	var err error
	if req.Msg.Id != "" {
		reqID, connErr := parseUUID(req.Msg.Id)
		if connErr != nil {
			return nil, connErr
		}
		existing, err = s.cfgStore.GetCertificate(ctx, reqID)
	} else {
		existing, err = s.findCertByName(ctx, req.Msg.Name)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if existing == nil {
		existing = &config.CertPEM{}
	}

	hasPEM := req.Msg.CertPem != "" && req.Msg.KeyPem != ""
	hasFiles := req.Msg.CertFile != "" && req.Msg.KeyFile != ""
	// Update PEM cert: certPem + empty keyPem means keep existing key
	hasPEMUpdate := req.Msg.CertPem != "" && (req.Msg.KeyPem != "" || (existing.KeyPEM != "" && existing.CertFile == ""))
	if !hasPEM && !hasFiles && !hasPEMUpdate {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("provide either cert_pem+key_pem or cert_file+key_file"))
	}

	keyPEM := req.Msg.KeyPem
	if req.Msg.CertPem != "" && keyPEM == "" && existing.KeyPEM != "" {
		keyPEM = existing.KeyPEM
	}

	// Validate PEM before storing to avoid enabling HTTPS with invalid certs
	if req.Msg.CertPem != "" && keyPEM != "" {
		if _, err := tls.X509KeyPair([]byte(req.Msg.CertPem), []byte(keyPEM)); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid certificate or key PEM: %w", err))
		}
	}
	if hasFiles {
		keyPath := req.Msg.KeyFile
		if keyPath == "" {
			keyPath = existing.KeyFile
		}
		if keyPath != "" {
			if _, err := tls.LoadX509KeyPair(req.Msg.CertFile, keyPath); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid certificate or key file: %w", err))
			}
		}
	}

	// Reuse existing ID, or generate new UUID.
	certID := existing.ID
	if certID == uuid.Nil {
		if req.Msg.Id != "" {
			// Already validated above.
			certID, _ = uuid.Parse(req.Msg.Id)
		} else {
			certID = uuid.Must(uuid.NewV7())
		}
	}

	newCert := config.CertPEM{
		ID:       certID,
		Name:     req.Msg.Name,
		CertPEM:  req.Msg.CertPem,
		KeyPEM:   keyPEM,
		CertFile: req.Msg.CertFile,
		KeyFile:  req.Msg.KeyFile,
	}
	if err := s.cfgStore.PutCertificate(ctx, newCert); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Update default cert in server config if requested.
	if req.Msg.SetAsDefault {
		sc, err := config.LoadServerConfig(ctx, s.cfgStore)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		sc.TLS.DefaultCert = req.Msg.Name
		if err := config.SaveServerConfig(ctx, s.cfgStore, sc); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	if err := s.reloadCertManager(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload certs: %w", err))
	}
	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}
	return connect.NewResponse(&apiv1.PutCertificateResponse{}), nil
}

// DeleteCertificate removes a certificate.
func (s *ConfigServer) DeleteCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteCertificateRequest],
) (*connect.Response[apiv1.DeleteCertificateResponse], error) {
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id required"))
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	pem, err := s.cfgStore.GetCertificate(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if pem == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("certificate not found"))
	}
	if err := s.cfgStore.DeleteCertificate(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Clear default and disable TLS if the deleted cert was the default.
	sc, err := config.LoadServerConfig(ctx, s.cfgStore)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if sc.TLS.DefaultCert == pem.Name {
		sc.TLS.DefaultCert = ""
		sc.TLS.TLSEnabled = false
		sc.TLS.HTTPToHTTPSRedirect = false
		if err := config.SaveServerConfig(ctx, s.cfgStore, sc); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	if err := s.reloadCertManager(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload certs: %w", err))
	}
	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}
	return connect.NewResponse(&apiv1.DeleteCertificateResponse{}), nil
}
