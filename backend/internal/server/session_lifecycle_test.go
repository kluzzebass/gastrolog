package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/auth"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// sessionHarness wires the auth handlers behind the real auth interceptor and
// the real token validator, so revocation behaves as it does in the server.
type sessionHarness struct {
	client   gastrologv1connect.AuthServiceClient
	cfgStore *sysmem.Store
	tokens   *auth.TokenService
	url      string
}

func newSessionHarness(t *testing.T) *sessionHarness {
	t.Helper()

	cfgStore := sysmem.NewStore()
	tokens := auth.NewTokenService([]byte("session-test-secret-32-bytes!!!"), time.Hour)
	authServer := NewAuthServer(cfgStore, tokens, nil, false)
	interceptor := auth.NewAuthInterceptor(tokens, cfgStore, &tokenValidator{cfgStore: cfgStore})

	_, handler := gastrologv1connect.NewAuthServiceHandler(authServer, connect.WithInterceptors(interceptor))
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	return &sessionHarness{
		client:   gastrologv1connect.NewAuthServiceClient(http.DefaultClient, ts.URL),
		cfgStore: cfgStore,
		tokens:   tokens,
		url:      ts.URL,
	}
}

// as returns a client that presents the given access token.
func (h *sessionHarness) as(token string) gastrologv1connect.AuthServiceClient {
	return gastrologv1connect.NewAuthServiceClient(http.DefaultClient, h.url,
		connect.WithInterceptors(&sessionBearer{token: token}))
}

// register creates the first (admin) user and returns its token pair.
func (h *sessionHarness) register(t *testing.T, username, password string) (accessToken, refreshToken string) {
	t.Helper()
	resp, err := h.client.Register(context.Background(), connect.NewRequest(&apiv1.RegisterRequest{
		Username: username,
		Password: password,
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return resp.Msg.Token.Token, resp.Msg.RefreshToken
}

// login opens an additional session for an existing user.
func (h *sessionHarness) login(t *testing.T, username, password string) (accessToken, refreshToken string) {
	t.Helper()
	resp, err := h.client.Login(context.Background(), connect.NewRequest(&apiv1.LoginRequest{
		Username: username,
		Password: password,
	}))
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	return resp.Msg.Token.Token, resp.Msg.RefreshToken
}

// authenticatedCall exercises an RPC that requires a live access token.
func (h *sessionHarness) authenticatedCall(token string) error {
	_, err := h.as(token).ListUsers(context.Background(), connect.NewRequest(&apiv1.ListUsersRequest{}))
	return err
}

type sessionBearer struct{ token string }

func (b *sessionBearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+b.token)
		return next(ctx, req)
	}
}

func (b *sessionBearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (b *sessionBearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// TestLogoutRevokesAccessToken proves logout ends the session it is called on:
// the access token stops working immediately rather than living out its
// remaining lifetime, and the user's other sessions are left alone.
func TestLogoutRevokesAccessToken(t *testing.T) {
	t.Parallel()
	h := newSessionHarness(t)

	tokenA, refreshA := h.register(t, "admin", "password123")
	tokenB, _ := h.login(t, "admin", "password123")

	if err := h.authenticatedCall(tokenA); err != nil {
		t.Fatalf("token should work before logout: %v", err)
	}

	if _, err := h.as(tokenA).Logout(context.Background(), connect.NewRequest(&apiv1.LogoutRequest{})); err != nil {
		t.Fatalf("Logout: %v", err)
	}

	err := h.authenticatedCall(tokenA)
	if err == nil {
		t.Fatal("access token still works after logout")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated after logout, got %v (%v)", connect.CodeOf(err), err)
	}

	if _, err := h.client.RefreshToken(context.Background(), connect.NewRequest(&apiv1.RefreshTokenRequest{
		RefreshToken: refreshA,
	})); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("refresh token should die with the session, got %v (%v)", connect.CodeOf(err), err)
	}

	if err := h.authenticatedCall(tokenB); err != nil {
		t.Fatalf("logging out one session must not end the others: %v", err)
	}
}

// TestRefreshKeepsSessionAlive proves a legitimate refresh is not mistaken for
// a logout: the new access token works, and so does the one it replaces.
func TestRefreshKeepsSessionAlive(t *testing.T) {
	t.Parallel()
	h := newSessionHarness(t)

	oldToken, refresh := h.register(t, "admin", "password123")

	resp, err := h.client.RefreshToken(context.Background(), connect.NewRequest(&apiv1.RefreshTokenRequest{
		RefreshToken: refresh,
	}))
	if err != nil {
		t.Fatalf("RefreshToken: %v", err)
	}

	if err := h.authenticatedCall(resp.Msg.Token.Token); err != nil {
		t.Fatalf("refreshed access token should work: %v", err)
	}
	if err := h.authenticatedCall(oldToken); err != nil {
		t.Fatalf("refreshing must not revoke the access token already in flight: %v", err)
	}

	// A second refresh with the rotated token must also work, so rotation is
	// not a one-shot that strands a well-behaved client.
	if _, err := h.client.RefreshToken(context.Background(), connect.NewRequest(&apiv1.RefreshTokenRequest{
		RefreshToken: resp.Msg.RefreshToken,
	})); err != nil {
		t.Fatalf("second RefreshToken: %v", err)
	}
}

// TestConcurrentRefreshExchangesTokenOnce races two refreshes of the same
// token. Exactly one may be handed a session; the other must be told the token
// is invalid rather than being issued a second live pair.
func TestConcurrentRefreshExchangesTokenOnce(t *testing.T) {
	t.Parallel()
	h := newSessionHarness(t)
	h.register(t, "admin", "password123")

	const rounds = 60
	for range rounds {
		_, refresh := h.login(t, "admin", "password123")

		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make([]*connect.Response[apiv1.RefreshTokenResponse], 2)
		errs := make([]error, 2)
		for i := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i], errs[i] = h.client.RefreshToken(context.Background(),
					connect.NewRequest(&apiv1.RefreshTokenRequest{RefreshToken: refresh}))
			}()
		}
		close(start)
		wg.Wait()

		winners := 0
		for i := range 2 {
			if errs[i] == nil {
				winners++
				continue
			}
			if connect.CodeOf(errs[i]) != connect.CodeUnauthenticated {
				t.Fatalf("loser should get Unauthenticated, got %v (%v)", connect.CodeOf(errs[i]), errs[i])
			}
		}
		if winners != 1 {
			t.Fatalf("expected exactly one refresh to succeed, got %d", winners)
		}

		// The winner's tokens must be usable — a race must not leave the
		// legitimate client holding a dead pair.
		for i := range 2 {
			if errs[i] != nil {
				continue
			}
			if err := h.authenticatedCall(results[i].Msg.Token.Token); err != nil {
				t.Fatalf("winner's access token should work: %v", err)
			}
			if _, err := h.client.RefreshToken(context.Background(),
				connect.NewRequest(&apiv1.RefreshTokenRequest{RefreshToken: results[i].Msg.RefreshToken})); err != nil {
				t.Fatalf("winner's refresh token should work: %v", err)
			}
		}
	}
}

// laggingStore models a node that has not yet applied the entry which opened a
// session: the row is invisible to local reads until a barrier catches it up.
type laggingStore struct {
	system.Store
	mu       sync.Mutex
	caughtUp bool
	barriers int
}

func (s *laggingStore) Barrier(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.barriers++
	s.caughtUp = true
	return nil
}

func (s *laggingStore) GetRefreshToken(ctx context.Context, id glid.GLID) (*system.RefreshToken, error) {
	s.mu.Lock()
	caughtUp := s.caughtUp
	s.mu.Unlock()
	if !caughtUp {
		return nil, nil
	}
	return s.Store.GetRefreshToken(ctx, id)
}

func (s *laggingStore) barrierCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.barriers
}

// TestSessionMissConfirmedAgainstReplicatedState proves a node that trails the
// login does not mistake its own lag for a logout. Any node serves any request,
// so the node the client lands on next may not have applied the entry that
// opened the session; rejecting on that read would bounce a user who just
// logged in straight back to the login page.
func TestSessionMissConfirmedAgainstReplicatedState(t *testing.T) {
	t.Parallel()
	h := newSessionHarness(t)

	token, _ := h.register(t, "admin", "password123")
	claims, err := h.tokens.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	lagging := &laggingStore{Store: h.cfgStore}
	validator := &tokenValidator{cfgStore: lagging}
	ctx := context.Background()

	valid, err := validator.IsTokenValid(ctx, claims)
	if err != nil {
		t.Fatalf("IsTokenValid: %v", err)
	}
	if !valid {
		t.Fatal("a session the node has not caught up to yet was treated as revoked")
	}
	if got := lagging.barrierCount(); got != 1 {
		t.Fatalf("expected the miss to force one catch-up, got %d", got)
	}

	// Once current, a session that really is gone stays rejected — the
	// catch-up must not turn the check into a rubber stamp.
	sessionID, err := glid.ParseUUID(claims.SessionID)
	if err != nil {
		t.Fatalf("ParseUUID: %v", err)
	}
	if err := h.cfgStore.DeleteRefreshToken(ctx, sessionID); err != nil {
		t.Fatalf("DeleteRefreshToken: %v", err)
	}
	valid, err = validator.IsTokenValid(ctx, claims)
	if err != nil {
		t.Fatalf("IsTokenValid after delete: %v", err)
	}
	if valid {
		t.Fatal("a deleted session should stay rejected after catching up")
	}
}

// TestTokenWithoutSessionIsRejected pins the fail-closed direction: a token
// naming no session cannot be logged out, so it is not honoured.
func TestTokenWithoutSessionIsRejected(t *testing.T) {
	t.Parallel()
	h := newSessionHarness(t)

	token, _ := h.register(t, "admin", "password123")
	claims, err := h.tokens.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	sessionless, _, err := h.tokens.Issue(claims.UserID, claims.Username(), claims.Role, "")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := h.authenticatedCall(sessionless); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected Unauthenticated for a session-less token, got %v (%v)", connect.CodeOf(err), err)
	}
}

// TestInvalidationWithinSameSecond pins the ranking of a token against an
// invalidation recorded in the same wall-clock second. The registered "iat"
// claim is whole seconds, so a token can only be ranked correctly here if it
// carries a finer issue time; the boundary itself counts as invalidated.
func TestInvalidationWithinSameSecond(t *testing.T) {
	t.Parallel()
	h := newSessionHarness(t)

	token, _ := h.register(t, "admin", "password123")
	claims, err := h.tokens.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	issuedAt, ok := claims.IssuedAtPrecise()
	if !ok {
		t.Fatal("issued token carries no precise issue time")
	}
	userID, err := glid.ParseUUID(claims.UserID)
	if err != nil {
		t.Fatalf("ParseUUID: %v", err)
	}

	validator := &tokenValidator{cfgStore: h.cfgStore}
	ctx := context.Background()

	check := func(name string, at time.Time, want bool) {
		t.Helper()
		if err := h.cfgStore.InvalidateTokens(ctx, userID, at); err != nil {
			t.Fatalf("%s: InvalidateTokens: %v", name, err)
		}
		got, err := validator.IsTokenValid(ctx, claims)
		if err != nil {
			t.Fatalf("%s: IsTokenValid: %v", name, err)
		}
		if got != want {
			t.Fatalf("%s: invalidation at %s vs token issued at %s: valid=%v, want %v",
				name, at.Format(time.RFC3339Nano), issuedAt.Format(time.RFC3339Nano), got, want)
		}
	}

	check("invalidated a moment before the token was issued", issuedAt.Add(-time.Nanosecond), true)
	check("invalidated at the instant the token was issued", issuedAt, false)
	check("invalidated a moment after the token was issued", issuedAt.Add(time.Nanosecond), false)
}
