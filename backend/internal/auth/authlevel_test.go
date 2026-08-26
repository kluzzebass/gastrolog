package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// rangeAPIMethods calls fn for every method of every service defined in the
// gastrolog.v1 schema, reading the generated descriptors rather than any list
// kept by hand: an RPC added to a proto shows up here on the next build.
func rangeAPIMethods(fn func(svc protoreflect.ServiceDescriptor, method protoreflect.MethodDescriptor)) {
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if fd.Package() != "gastrolog.v1" {
			return true
		}
		services := fd.Services()
		for i := range services.Len() {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := range methods.Len() {
				fn(svc, methods.Get(j))
			}
		}
		return true
	})
}

func TestEveryProcedureDeclaresAnAuthLevel(t *testing.T) {
	t.Parallel()
	levels := procedureLevels()

	seen := map[apiv1.AuthLevel]int{}
	total := 0
	rangeAPIMethods(func(svc protoreflect.ServiceDescriptor, method protoreflect.MethodDescriptor) {
		total++
		procedure := procedureName(svc, method)
		level, declared := levels[procedure]
		if !declared {
			t.Errorf("%s declares no authorization level: add `option (auth_level) = AUTH_LEVEL_…;` to its rpc", procedure)
			return
		}
		seen[level]++
	})

	// Without these premise checks the test would pass on an empty
	// enumeration or on an extension that never decodes.
	if total == 0 {
		t.Fatal("no gastrolog.v1 service descriptors found; the registry walk is broken, not the schema")
	}
	for _, level := range []apiv1.AuthLevel{
		apiv1.AuthLevel_AUTH_LEVEL_PUBLIC,
		apiv1.AuthLevel_AUTH_LEVEL_AUTHENTICATED,
		apiv1.AuthLevel_AUTH_LEVEL_ADMIN,
	} {
		if seen[level] == 0 {
			t.Errorf("no procedure resolved to %v; the method option is not being read", level)
		}
	}
}

func TestOperatorProceduresRequireAdmin(t *testing.T) {
	t.Parallel()
	levels := procedureLevels()

	// Actions that change cluster state, spend operator-configured credentials,
	// or move chunk data. Each is reachable only by an admin.
	operator := []string{
		"/gastrolog.v1.LifecycleService/SetNodeState",
		"/gastrolog.v1.LifecycleService/YieldLeadership",
		"/gastrolog.v1.SystemService/PutLogLevels",
		"/gastrolog.v1.SystemService/DeleteLookup",
		"/gastrolog.v1.SystemService/TestIngester",
		"/gastrolog.v1.SystemService/TestHTTPLookup",
		"/gastrolog.v1.VaultService/RepatriateOrphan",
		"/gastrolog.v1.VaultService/ReconcileCloudIndex",
		"/gastrolog.v1.VaultService/RetryUnreadableChunks",
		"/gastrolog.v1.VaultService/ArchiveChunk",
		"/gastrolog.v1.VaultService/RestoreChunk",
	}
	for _, procedure := range operator {
		if levels[procedure] != apiv1.AuthLevel_AUTH_LEVEL_ADMIN {
			t.Errorf("%s: level %v, want AUTH_LEVEL_ADMIN", procedure, levels[procedure])
		}
	}
}

// TestUndeclaredProcedureIsDenied covers the fail-closed default: a procedure
// the interceptor cannot find a level for never reaches its handler.
func TestUndeclaredProcedureIsDenied(t *testing.T) {
	t.Parallel()
	tokens := NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	adminToken, _, err := tokens.Issue("uid-admin", "admin", "admin")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	reached := false
	const procedure = "/gastrolog.v1.UndeclaredService/Undeclared"
	handler := connect.NewUnaryHandler(
		procedure,
		func(context.Context, *connect.Request[apiv1.LoginRequest]) (*connect.Response[apiv1.LoginResponse], error) {
			reached = true
			return connect.NewResponse(&apiv1.LoginResponse{}), nil
		},
		connect.WithInterceptors(NewAuthInterceptor(tokens, &countingUsers{count: 1}, nil)),
	)
	mux := http.NewServeMux()
	mux.Handle(procedure, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client := connect.NewClient[apiv1.LoginRequest, apiv1.LoginResponse](
		http.DefaultClient, ts.URL+procedure,
		connect.WithInterceptors(&bearerToken{token: adminToken}),
	)
	_, err = client.CallUnary(context.Background(), connect.NewRequest(&apiv1.LoginRequest{}))
	if err == nil {
		t.Fatal("expected an undeclared procedure to be denied")
	}
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("expected PermissionDenied, got %v (%v)", connect.CodeOf(err), err)
	}
	if reached {
		t.Error("handler ran for an undeclared procedure")
	}
}

// countingUsers is a UserCounter with a fixed count.
type countingUsers struct{ count int }

func (c *countingUsers) CountUsers(context.Context) (int, error) { return c.count, nil }

// bearerToken attaches a token to outgoing client requests.
type bearerToken struct{ token string }

func (b *bearerToken) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+b.token)
		return next(ctx, req)
	}
}

func (b *bearerToken) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (b *bearerToken) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
