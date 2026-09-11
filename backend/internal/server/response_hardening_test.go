package server_test

// What a response tells a caller, beyond what they asked for: the headers
// that constrain a browser, the host a redirect sends them to, and how much
// of this server's internals an error carries back.

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// HSTS claims every future visit to this host will be over TLS. A browser
// ignores it on a plain HTTP response, so sending it there would be a claim
// the connection carrying it cannot support.
func TestStrictTransportSecurityOnlyOverTLS(t *testing.T) {
	t.Parallel()
	handler := newChainTestHandler(t)

	plain := httptest.NewRecorder()
	handler.ServeHTTP(plain, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := plain.Header().Get("Strict-Transport-Security"); got != "" {
		t.Errorf("Strict-Transport-Security = %q on a plain HTTP response, want none", got)
	}

	secureReq := httptest.NewRequest(http.MethodGet, "/", nil)
	secureReq.TLS = &tls.ConnectionState{}
	secure := httptest.NewRecorder()
	handler.ServeHTTP(secure, secureReq)
	got := secure.Header().Get("Strict-Transport-Security")
	if got == "" {
		t.Fatal("Strict-Transport-Security is absent over TLS")
	}
	if !strings.Contains(got, "max-age=") {
		t.Errorf("Strict-Transport-Security = %q, want a max-age", got)
	}
	if strings.Contains(got, "preload") {
		t.Errorf("Strict-Transport-Security = %q; preload is an irreversible submission "+
			"and not this server's decision to make for its domain", got)
	}
}

// An internal failure is a statement about this server, and its text carries
// whatever the failing layer happened to say. The caller gets a reference
// instead, and the operator finds the detail in the log under it.
func TestInternalErrorTextDoesNotReachTheCaller(t *testing.T) {
	t.Parallel()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	failing := &vaultListFailsStore{Store: cfgStore, err: errors.New("dial tcp 10.1.2.3:5432: connection refused")}
	srv := server.New(orch, failing, orchestrator.Factories{VaultsDir: t.TempDir()}, nil,
		server.Config{NoAuth: true})
	client := gastrologv1connect.NewSystemServiceClient(
		&http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}, "http://embedded")

	_, err = client.GetSystem(t.Context(), connect.NewRequest(&gastrologv1.GetSystemRequest{}))
	if err == nil {
		t.Fatal("premise: the injected failure must surface as an error")
	}
	if connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("code = %v, want %v", connect.CodeOf(err), connect.CodeInternal)
	}
	if strings.Contains(err.Error(), "10.1.2.3") || strings.Contains(err.Error(), "connection refused") {
		t.Errorf("the internal detail reached the caller: %v", err)
	}
	if !strings.Contains(err.Error(), "ref ") {
		t.Errorf("error = %v, want a reference the operator can find in the log", err)
	}
}

// Every other code is a statement about the request, so it goes back as
// written — redacting those would leave a caller unable to tell a bad
// argument from a missing vault.
func TestNonInternalErrorTextStillReachesTheCaller(t *testing.T) {
	t.Parallel()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	srv := server.New(orch, cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil,
		server.Config{NoAuth: true})
	client := gastrologv1connect.NewVaultServiceClient(
		&http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}, "http://embedded")

	_, err = client.GetIndexes(t.Context(), connect.NewRequest(&gastrologv1.GetIndexesRequest{}))
	if err == nil {
		t.Fatal("a request with no vault must be refused")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want %v", connect.CodeOf(err), connect.CodeInvalidArgument)
	}
	if !strings.Contains(err.Error(), "vault") {
		t.Errorf("error = %v, want it to name what was wrong with the request", err)
	}
}

// vaultListFailsStore is a config store whose vault listing fails, which is
// the first thing GetSystem reads. It stands in for any internal dependency
// failing with a message that names infrastructure.
type vaultListFailsStore struct {
	system.Store
	err error
}

func (s *vaultListFailsStore) ListVaults(context.Context) ([]system.VaultConfig, error) {
	return nil, s.err
}
