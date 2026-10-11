package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func TestResolveCertID_ExistingID(t *testing.T) {
	t.Parallel()
	existing := glid.New()
	got, err := resolveCertID(existing, "anything")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != existing {
		t.Fatalf("expected existing ID %s, got %s", existing, got)
	}
}

func TestResolveCertID_ValidReqID(t *testing.T) {
	t.Parallel()
	reqID := glid.New()
	got, err := resolveCertID(glid.Nil, reqID.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != reqID {
		t.Fatalf("expected %s, got %s", reqID, got)
	}
}

func TestResolveCertID_MalformedReqID(t *testing.T) {
	t.Parallel()
	_, err := resolveCertID(glid.Nil, "not-a-uuid")
	if err == nil {
		t.Fatal("expected error for malformed UUID, got nil")
	}
}

func TestResolveCertID_EmptyReqID(t *testing.T) {
	t.Parallel()
	got, err := resolveCertID(glid.Nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == glid.Nil {
		t.Fatal("expected a new UUIDv7, got nil UUID")
	}
}

// ListCertificates tells the caller where each certificate's key material
// lives: from_files for a certificate watched on disk, otherwise PEM held
// in the configuration. The settings card's kind badge reads this bit.
func TestListCertificates_FromFiles(t *testing.T) {
	t.Parallel()
	store, _ := nodeStore(t, map[string]system.NodeState{})
	ctx := context.Background()
	for _, c := range []system.CertPEM{
		{ID: glid.New(), Name: "pasted", CertPEM: "cert", KeyPEM: "key"},
		{ID: glid.New(), Name: "watched", CertFile: "/etc/ssl/cert.pem", KeyFile: "/etc/ssl/key.pem"},
	} {
		if err := store.PutCertificate(ctx, c); err != nil {
			t.Fatalf("PutCertificate %s: %v", c.Name, err)
		}
	}
	s := &SystemServer{sysStore: store}
	resp, err := s.ListCertificates(ctx, connect.NewRequest(&apiv1.ListCertificatesRequest{}))
	if err != nil {
		t.Fatalf("ListCertificates: %v", err)
	}
	fromFiles := map[string]bool{}
	for _, info := range resp.Msg.Certificates {
		fromFiles[info.Name] = info.FromFiles
	}
	if fromFiles["pasted"] {
		t.Fatal("pasted PEM certificate reported from_files")
	}
	if !fromFiles["watched"] {
		t.Fatal("file-backed certificate not reported from_files")
	}
}
