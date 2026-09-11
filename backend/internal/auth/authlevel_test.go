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
		// Cloud service destinations hold write-only credentials: a caller able
		// to rewrite one redirects the operator's keys at an endpoint of their
		// choosing, and the connection test spends them.
		"/gastrolog.v1.SystemService/PutCloudService",
		"/gastrolog.v1.SystemService/DeleteCloudService",
		"/gastrolog.v1.SystemService/TestCloudService",
		"/gastrolog.v1.SystemService/SetNodeStorageConfig",
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
	adminToken, _, err := tokens.Issue("uid-admin", "admin", "admin", "")
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
		connect.WithInterceptors(NewAuthInterceptor(NewVerifier(tokens, nil), &countingUsers{count: 1})),
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

// TestUnrecognizedLevelIsDenied covers a level this build does not know: a
// value added to the schema ahead of the code enforces as denial, not as the
// weakest known level.
func TestUnrecognizedLevelIsDenied(t *testing.T) {
	t.Parallel()
	tokens := NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	adminToken, _, err := tokens.Issue("uid-admin", "admin", "admin", "")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	headers := http.Header{"Authorization": []string{"Bearer " + adminToken}}
	verifier := NewVerifier(tokens, nil)

	// A level past the last one this build names, and the unspecified zero
	// value. An admin token is the strongest credential available, so a denial
	// here is the level's doing and not the caller's.
	for _, level := range []apiv1.AuthLevel{apiv1.AuthLevel(99), apiv1.AuthLevel_AUTH_LEVEL_UNSPECIFIED} {
		claims, err := verifier.Authorize(context.Background(), level, headers)
		if err == nil {
			t.Errorf("level %d: allowed, want denied", int32(level))
			continue
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("level %d: code %v, want PermissionDenied", int32(level), connect.CodeOf(err))
		}
		if claims != nil {
			t.Errorf("level %d: returned claims %+v, want none", int32(level), claims)
		}
	}

	// The interceptor must not route around the switch: a procedure whose table
	// entry carries an unknown level is denied too.
	const procedure = "/gastrolog.v1.SystemService/GetSystem"
	interceptor := &AuthInterceptor{
		verifier: verifier,
		counter:  &countingUsers{count: 1},
		levels:   map[string]apiv1.AuthLevel{procedure: apiv1.AuthLevel(99)},
	}
	if _, err := interceptor.authenticate(context.Background(), procedure, headers); err == nil {
		t.Error("interceptor allowed an unknown level")
	} else if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("interceptor code %v, want PermissionDenied", connect.CodeOf(err))
	}

	// Premise: the same interceptor allows the same caller at a known level, so
	// the denials above are the level and not the harness.
	interceptor.levels[procedure] = apiv1.AuthLevel_AUTH_LEVEL_ADMIN
	if _, err := interceptor.authenticate(context.Background(), procedure, headers); err != nil {
		t.Errorf("admin at ADMIN level: %v, want allowed", err)
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

// Watching data and asking for it are the same disclosure, so they carry the
// same level. A stream declared below its fetching counterpart hands a caller
// by subscription what they are refused by request, and nothing else notices:
// the stream works, it just works for the wrong people.
//
// Pairs are listed rather than derived, because the relationship is semantic —
// only a reader can say which fetch a given stream mirrors. WatchSystem is
// deliberately absent: it carries a Raft index, not the config GetSystem is
// gated for, so it is not this kind of pair.
func TestStreamsDoNotDiscloseBelowTheirFetchingCounterpart(t *testing.T) {
	t.Parallel()
	levels := procedureLevels()

	// Guard the guard: the comparison below is numeric, so it only means
	// anything while a stricter level sorts above a looser one.
	if !(apiv1.AuthLevel_AUTH_LEVEL_ADMIN > apiv1.AuthLevel_AUTH_LEVEL_AUTHENTICATED) {
		t.Fatal("AuthLevel no longer orders admin above authenticated; the comparison below proves nothing")
	}

	pairs := []struct {
		stream string // the streaming or derived read
		fetch  string // the request that returns the same data
		why    string
	}{
		{
			"/gastrolog.v1.SystemService/WatchIngesterStatus",
			"/gastrolog.v1.SystemService/GetIngesterStatus",
			"streams the ingest counters and error totals the fetch returns",
		},
		{
			"/gastrolog.v1.VaultService/WatchChunks",
			"/gastrolog.v1.VaultService/ListChunks",
			"streams ChunkMeta, which is what the listing is gated for",
		},
		{
			"/gastrolog.v1.VaultService/GetPipelineBacklog",
			"/gastrolog.v1.VaultService/ListChunks",
			"reports the same per-vault chunk pipeline state",
		},
		{
			"/gastrolog.v1.SystemService/PreviewCSVLookup",
			"/gastrolog.v1.SystemService/ListManagedFiles",
			"returns sample rows out of an admin-uploaded managed file",
		},
		{
			"/gastrolog.v1.SystemService/PreviewJSONLookup",
			"/gastrolog.v1.SystemService/ListManagedFiles",
			"returns sample rows out of an admin-uploaded managed file",
		},
		{
			"/gastrolog.v1.SystemService/PreviewYAMLLookup",
			"/gastrolog.v1.SystemService/ListManagedFiles",
			"returns sample rows out of an admin-uploaded managed file",
		},
	}

	for _, p := range pairs {
		streamLevel, ok := levels[p.stream]
		if !ok {
			t.Errorf("%s declares no auth level", p.stream)
			continue
		}
		fetchLevel, ok := levels[p.fetch]
		if !ok {
			t.Errorf("%s declares no auth level", p.fetch)
			continue
		}
		if streamLevel < fetchLevel {
			t.Errorf("%s is %v while %s is %v: it %s, so a caller refused the fetch "+
				"can still subscribe to it",
				p.stream, streamLevel, p.fetch, fetchLevel, p.why)
		}
	}
}
