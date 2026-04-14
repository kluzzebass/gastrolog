package server

import (
	"gastrolog/internal/glid"
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cert"
	"gastrolog/internal/system"
)

// reloadCertManager lists all certs from the vault and loads them into the cert manager.
func (s *SystemServer) reloadCertManager(ctx context.Context) error {
	if s.certManager == nil {
		return nil
	}
	ss, err := s.sysStore.LoadServerSettings(ctx)
	if err != nil {
		return fmt.Errorf("load server settings: %w", err)
	}
	tlsCfg := ss.TLS
	certList, err := s.sysStore.ListCertificates(ctx)
	if err != nil {
		return fmt.Errorf("list certificates: %w", err)
	}
	certs := make(map[string]cert.CertSource, len(certList))
	for _, c := range certList {
		certs[c.Name] = cert.CertSource{CertPEM: c.CertPEM, KeyPEM: c.KeyPEM, CertFile: c.CertFile, KeyFile: c.KeyFile}
	}
	return s.certManager.LoadFromConfig(tlsCfg.DefaultCert, certs)
}

// ListCertificates returns all certificate names.
func (s *SystemServer) ListCertificates(
	ctx context.Context,
	req *connect.Request[apiv1.ListCertificatesRequest],
) (*connect.Response[apiv1.ListCertificatesResponse], error) {
	certs, err := s.sysStore.ListCertificates(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	infos := make([]*apiv1.CertificateInfo, len(certs))
	for i, c := range certs {
		infos[i] = &apiv1.CertificateInfo{Id: c.ID.String(), Name: c.Name}
	}
	return connect.NewResponse(&apiv1.ListCertificatesResponse{Certificates: infos}), nil
}

// findCertByName returns the certificate with the given name, or nil if not found.
func (s *SystemServer) findCertByName(ctx context.Context, name string) (*system.CertPEM, error) {
	certs, err := s.sysStore.ListCertificates(ctx)
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
func (s *SystemServer) GetCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.GetCertificateRequest],
) (*connect.Response[apiv1.GetCertificateResponse], error) {
	if req.Msg.Id == "" {
		return nil, errRequired("id")
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	pem, err := s.sysStore.GetCertificate(ctx, id)
	if err != nil {
		return nil, errInternal(err)
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
func (s *SystemServer) PutCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.PutCertificateRequest],
) (*connect.Response[apiv1.PutCertificateResponse], error) {
	if req.Msg.Name == "" {
		return nil, errRequired("name")
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

	certID, err := resolveCertID(existing.ID, req.Msg.Id)
	if err != nil {
		return nil, errInvalidArg(err)
	}

	// Reject duplicate names.
	certs, err := s.sysStore.ListCertificates(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if connErr := checkNameConflict("certificate", certID, req.Msg.Name, certs, func(c system.CertPEM) (glid.GLID, string) { return c.ID, c.Name }); connErr != nil {
		return nil, connErr
	}

	newCert := system.CertPEM{
		ID:       certID,
		Name:     req.Msg.Name,
		CertPEM:  req.Msg.CertPem,
		KeyPEM:   keyPEM,
		CertFile: req.Msg.CertFile,
		KeyFile:  req.Msg.KeyFile,
	}
	if err := s.sysStore.PutCertificate(ctx, newCert); err != nil {
		return nil, errInternal(err)
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
	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.PutCertificateResponse{System: cfg}), nil
}

func (s *SystemServer) loadExistingCert(ctx context.Context, id, name string) (system.CertPEM, error) {
	var existing *system.CertPEM
	var err error
	if id != "" {
		reqID, connErr := parseUUID(id)
		if connErr != nil {
			return system.CertPEM{}, connErr
		}
		existing, err = s.sysStore.GetCertificate(ctx, reqID)
	} else {
		existing, err = s.findCertByName(ctx, name)
	}
	if err != nil {
		return system.CertPEM{}, errInternal(err)
	}
	if existing == nil {
		return system.CertPEM{}, nil
	}
	return *existing, nil
}

func validateCertInput(msg *apiv1.PutCertificateRequest, existing system.CertPEM) *connect.Error {
	hasPEM := msg.CertPem != "" && msg.KeyPem != ""
	hasFiles := msg.CertFile != "" && msg.KeyFile != ""
	hasPEMUpdate := msg.CertPem != "" && (msg.KeyPem != "" || (existing.KeyPEM != "" && existing.CertFile == ""))
	if !hasPEM && !hasFiles && !hasPEMUpdate {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("provide either cert_pem+key_pem or cert_file+key_file"))
	}
	return nil
}

func validateCertMaterial(msg *apiv1.PutCertificateRequest, keyPEM string, existing system.CertPEM) *connect.Error {
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

func resolveCertID(existingID glid.GLID, reqID string) (glid.GLID, error) {
	if existingID != glid.Nil {
		return existingID, nil
	}
	if reqID != "" {
		id, err := glid.ParseUUID(reqID)
		if err != nil {
			return glid.Nil, fmt.Errorf("invalid certificate id %q: %w", reqID, err)
		}
		return id, nil
	}
	return glid.New(), nil
}

func (s *SystemServer) setDefaultCert(ctx context.Context, name string) error {
	ss, err := s.sysStore.LoadServerSettings(ctx)
	if err != nil {
		return errInternal(err)
	}
	ss.TLS.DefaultCert = name
	if err := s.sysStore.SaveServerSettings(ctx, ss); err != nil {
		return errInternal(err)
	}
	return nil
}

// DeleteCertificate removes a certificate.
func (s *SystemServer) DeleteCertificate(
	ctx context.Context,
	req *connect.Request[apiv1.DeleteCertificateRequest],
) (*connect.Response[apiv1.DeleteCertificateResponse], error) {
	if req.Msg.Id == "" {
		return nil, errRequired("id")
	}

	id, connErr := parseUUID(req.Msg.Id)
	if connErr != nil {
		return nil, connErr
	}

	pem, err := s.sysStore.GetCertificate(ctx, id)
	if err != nil {
		return nil, errInternal(err)
	}
	if pem == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("certificate not found"))
	}
	if err := s.sysStore.DeleteCertificate(ctx, id); err != nil {
		return nil, errInternal(err)
	}

	// Clear default and disable TLS if the deleted cert was the default.
	ss, err := s.sysStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	if ss.TLS.DefaultCert == pem.Name {
		ss.TLS.DefaultCert = ""
		ss.TLS.TLSEnabled = false
		ss.TLS.HTTPToHTTPSRedirect = false
		if err := s.sysStore.SaveServerSettings(ctx, ss); err != nil {
			return nil, errInternal(err)
		}
	}

	if err := s.reloadCertManager(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("reload certs: %w", err))
	}
	if s.onTLSConfigChange != nil {
		s.onTLSConfigChange()
	}
	cfg, err := s.buildFullSystem(ctx)
	if err != nil {
		return nil, errInternal(err)
	}
	return connect.NewResponse(&apiv1.DeleteCertificateResponse{System: cfg}), nil
}
