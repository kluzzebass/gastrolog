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

	existing, err := s.loadExistingCert(ctx, req.Msg.Id, req.Msg.Name)
	if err != nil {
		return nil, err
	}

	if err := validateCertInput(req.Msg, existing); err != nil {
		return nil, err
	}

	keyPEM := req.Msg.KeyPem
	if req.Msg.CertPem != "" && keyPEM == "" && existing.KeyPEM != "" {
		keyPEM = existing.KeyPEM
	}

	if err := validateCertMaterial(req.Msg, keyPEM, existing); err != nil {
		return nil, err
	}

	certID := resolveCertID(existing.ID, req.Msg.Id)

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

	if req.Msg.SetAsDefault {
		if err := s.setDefaultCert(ctx, req.Msg.Name); err != nil {
			return nil, err
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

func (s *ConfigServer) loadExistingCert(ctx context.Context, id, name string) (config.CertPEM, error) {
	var existing *config.CertPEM
	var err error
	if id != "" {
		reqID, connErr := parseUUID(id)
		if connErr != nil {
			return config.CertPEM{}, connErr
		}
		existing, err = s.cfgStore.GetCertificate(ctx, reqID)
	} else {
		existing, err = s.findCertByName(ctx, name)
	}
	if err != nil {
		return config.CertPEM{}, connect.NewError(connect.CodeInternal, err)
	}
	if existing == nil {
		return config.CertPEM{}, nil
	}
	return *existing, nil
}

func validateCertInput(msg *apiv1.PutCertificateRequest, existing config.CertPEM) *connect.Error {
	hasPEM := msg.CertPem != "" && msg.KeyPem != ""
	hasFiles := msg.CertFile != "" && msg.KeyFile != ""
	hasPEMUpdate := msg.CertPem != "" && (msg.KeyPem != "" || (existing.KeyPEM != "" && existing.CertFile == ""))
	if !hasPEM && !hasFiles && !hasPEMUpdate {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("provide either cert_pem+key_pem or cert_file+key_file"))
	}
	return nil
}

func validateCertMaterial(msg *apiv1.PutCertificateRequest, keyPEM string, existing config.CertPEM) *connect.Error {
	if msg.CertPem != "" && keyPEM != "" {
		if _, err := tls.X509KeyPair([]byte(msg.CertPem), []byte(keyPEM)); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid certificate or key PEM: %w", err))
		}
	}
	hasFiles := msg.CertFile != "" && msg.KeyFile != ""
	if !hasFiles {
		return nil
	}
	keyPath := msg.KeyFile
	if keyPath == "" {
		keyPath = existing.KeyFile
	}
	if keyPath == "" {
		return nil
	}
	if _, err := tls.LoadX509KeyPair(msg.CertFile, keyPath); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid certificate or key file: %w", err))
	}
	return nil
}

func resolveCertID(existingID uuid.UUID, reqID string) uuid.UUID {
	if existingID != uuid.Nil {
		return existingID
	}
	if reqID != "" {
		id, _ := uuid.Parse(reqID)
		return id
	}
	return uuid.Must(uuid.NewV7())
}

func (s *ConfigServer) setDefaultCert(ctx context.Context, name string) error {
	sc, err := config.LoadServerConfig(ctx, s.cfgStore)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	sc.TLS.DefaultCert = name
	if err := config.SaveServerConfig(ctx, s.cfgStore, sc); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	return nil
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
