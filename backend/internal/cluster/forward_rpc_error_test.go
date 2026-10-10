package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// A forwarded handler's error must reach the caller as the handler returned
// it: its exact Connect code and its bare message. The internal mux renders
// the error as a Connect JSON body behind an HTTP status that several codes
// share, so neither the status nor the raw body is the error.

// failingVaultService fails SealVault with err.
type failingVaultService struct {
	gastrologv1connect.UnimplementedVaultServiceHandler
	err error
}

func (f *failingVaultService) SealVault(
	context.Context, *connect.Request[gastrologv1.SealVaultRequest],
) (*connect.Response[gastrologv1.SealVaultResponse], error) {
	return nil, f.err
}

func failingVaultMux(err error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(gastrologv1connect.NewVaultServiceHandler(&failingVaultService{err: err}))
	return mux
}

// forwardThrough runs forwardRPCStreamHandler for procedure against handler
// and returns the single response frame.
func forwardThrough(t *testing.T, handler http.Handler, procedure string) *gastrologv1.ForwardRPCFrame {
	t.Helper()
	srv := &Server{}
	srv.SetInternalHandler(handler)
	payload, err := proto.Marshal(&gastrologv1.SealVaultRequest{Vault: "v"})
	if err != nil {
		t.Fatal(err)
	}
	stream := &captureRPCStream{
		ctx: context.Background(),
		req: &gastrologv1.ForwardRPCFrame{Procedure: procedure, Payload: payload},
	}
	if err := forwardRPCStreamHandler(srv, stream); err != nil {
		t.Fatalf("forwardRPCStreamHandler returned transport error: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("expected exactly one response frame, got %d", len(stream.sent))
	}
	return stream.sent[0]
}

func assertErrorFrame(t *testing.T, frame *gastrologv1.ForwardRPCFrame, wantCode connect.Code, wantMsg string) {
	t.Helper()
	if got := connect.Code(frame.GetErrorCode()); got != wantCode {
		t.Errorf("error_code = %v, want %v (message %q)", got, wantCode, frame.GetErrorMessage())
	}
	if got := frame.GetErrorMessage(); got != wantMsg {
		t.Errorf("error_message = %q, want %q", got, wantMsg)
	}
	if len(frame.GetPayload()) != 0 {
		t.Errorf("error frame carries a %d-byte payload", len(frame.GetPayload()))
	}
}

var allErrorCodes = []connect.Code{
	connect.CodeCanceled,
	connect.CodeUnknown,
	connect.CodeInvalidArgument,
	connect.CodeDeadlineExceeded,
	connect.CodeNotFound,
	connect.CodeAlreadyExists,
	connect.CodePermissionDenied,
	connect.CodeResourceExhausted,
	connect.CodeFailedPrecondition,
	connect.CodeAborted,
	connect.CodeOutOfRange,
	connect.CodeUnimplemented,
	connect.CodeInternal,
	connect.CodeUnavailable,
	connect.CodeDataLoss,
	connect.CodeUnauthenticated,
}

// TestForwardRPCErrorFrameCarriesHandlerCode drives every Connect code
// through the internal mux, including the ones whose HTTP status another code
// shares (400, 409, 500).
func TestForwardRPCErrorFrameCarriesHandlerCode(t *testing.T) {
	for _, code := range allErrorCodes {
		t.Run(code.String(), func(t *testing.T) {
			msg := "seal vault: open chunk can only be sealed on the vault's chunking leader (" + code.String() + ")"
			frame := forwardThrough(t, failingVaultMux(connect.NewError(code, errors.New(msg))),
				gastrologv1connect.VaultServiceSealVaultProcedure)
			assertErrorFrame(t, frame, code, msg)
		})
	}
}

// TestForwardRPCErrorFramePlainHandlerError: a handler returning a non-Connect
// error is rendered by Connect as Unknown with the error text.
func TestForwardRPCErrorFramePlainHandlerError(t *testing.T) {
	frame := forwardThrough(t, failingVaultMux(errors.New("disk on fire")),
		gastrologv1connect.VaultServiceSealVaultProcedure)
	assertErrorFrame(t, frame, connect.CodeUnknown, "disk on fire")
}

// TestForwardRPCErrorFrameMessageSurvivesVerbatim: the message is decoded,
// not quoted — escapes, newlines, unicode and JSON-shaped text come through
// exactly, and a message larger than any small read buffer is not cut.
func TestForwardRPCErrorFrameMessageSurvivesVerbatim(t *testing.T) {
	cases := map[string]string{
		"quotes":     `vault "prod" refused: 'leader' is "node-2"`,
		"newlines":   "line one\nline two\r\n\ttabbed",
		"unicode":    "chunk ünïcödé — 日本語 🚫 \u2028 separator",
		"backslash":  `path C:\vaults\prod \" not an escape`,
		"json-shape": `{"code":"not_found","message":"decoy"}`,
		"html":       "<script>alert(1)</script> & </b>",
		"large":      strings.Repeat("0123456789abcdef", 4096),
		"empty":      "",
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			frame := forwardThrough(t,
				failingVaultMux(connect.NewError(connect.CodeFailedPrecondition, errors.New(msg))),
				gastrologv1connect.VaultServiceSealVaultProcedure)
			assertErrorFrame(t, frame, connect.CodeFailedPrecondition, msg)
		})
	}
}

// rawErrorHandler answers every request with status and body verbatim.
type rawErrorHandler struct {
	status      int
	contentType string
	body        string
}

func (h rawErrorHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if h.contentType != "" {
		w.Header().Set("Content-Type", h.contentType)
	}
	w.WriteHeader(h.status)
	_, _ = w.Write([]byte(h.body))
}

// TestForwardRPCErrorFrameNonConnectBody: a response that is not a Connect
// error keeps the HTTP-status mapping and the body text.
func TestForwardRPCErrorFrameNonConnectBody(t *testing.T) {
	cases := []struct {
		name     string
		handler  http.Handler
		wantCode connect.Code
		wantMsg  string
	}{
		{
			name:     "draining server plain text",
			handler:  rawErrorHandler{status: http.StatusServiceUnavailable, contentType: "text/plain; charset=utf-8", body: "server is draining\n"},
			wantCode: connect.CodeUnavailable,
			wantMsg:  "server is draining",
		},
		{
			name:     "empty body 500",
			handler:  rawErrorHandler{status: http.StatusInternalServerError},
			wantCode: connect.CodeUnknown,
			wantMsg:  "upstream error: HTTP 500",
		},
		{
			name:     "empty body 403",
			handler:  rawErrorHandler{status: http.StatusForbidden},
			wantCode: connect.CodePermissionDenied,
			wantMsg:  "upstream error: HTTP 403",
		},
		{
			name:     "whitespace body 504",
			handler:  rawErrorHandler{status: http.StatusGatewayTimeout, body: " \n\t"},
			wantCode: connect.CodeDeadlineExceeded,
			wantMsg:  "upstream error: HTTP 504",
		},
		{
			name:     "json without code or message",
			handler:  rawErrorHandler{status: http.StatusBadRequest, contentType: "application/json", body: `{"error":"nope"}`},
			wantCode: connect.CodeInvalidArgument,
			wantMsg:  `{"error":"nope"}`,
		},
		{
			name:     "json string, not object",
			handler:  rawErrorHandler{status: http.StatusNotFound, contentType: "application/json", body: `"gone"`},
			wantCode: connect.CodeNotFound,
			wantMsg:  `"gone"`,
		},
		{
			name:     "unrecognised code keeps message",
			handler:  rawErrorHandler{status: http.StatusConflict, contentType: "application/json", body: `{"code":"bogus","message":"chunk already archived"}`},
			wantCode: connect.CodeAlreadyExists,
			wantMsg:  "chunk already archived",
		},
		{
			name:     "out-of-range numeric code",
			handler:  rawErrorHandler{status: http.StatusTooManyRequests, contentType: "application/json", body: `{"code":"code_99","message":"slow down"}`},
			wantCode: connect.CodeResourceExhausted,
			wantMsg:  "slow down",
		},
		{
			name:     "truncated json",
			handler:  rawErrorHandler{status: http.StatusBadRequest, contentType: "application/json", body: `{"code":"failed_precondition","mess`},
			wantCode: connect.CodeInvalidArgument,
			wantMsg:  `{"code":"failed_precondition","mess`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame := forwardThrough(t, tc.handler, "/test.Service/Method")
			assertErrorFrame(t, frame, tc.wantCode, tc.wantMsg)
		})
	}
}

// TestForwardRPCErrorFrameUnknownProcedure: a procedure the peer's mux does
// not serve gets the mux's plain-text 404, not a Connect error.
func TestForwardRPCErrorFrameUnknownProcedure(t *testing.T) {
	frame := forwardThrough(t, failingVaultMux(errors.New("unused")), "/gastrolog.v1.NoSuchService/Nope")
	assertErrorFrame(t, frame, connect.CodeNotFound, "404 page not found")
}

// connectErrorBodyOfSize returns a Connect JSON error body of exactly n bytes.
func connectErrorBodyOfSize(t *testing.T, n int) string {
	t.Helper()
	const prefix = `{"code":"failed_precondition","message":"`
	const suffix = `"}`
	fill := n - len(prefix) - len(suffix)
	if fill < 0 {
		t.Fatalf("body size %d below the JSON frame", n)
	}
	return prefix + strings.Repeat("x", fill) + suffix
}

// TestForwardRPCErrorFrameBodyAtLimitDecodes pins the inclusive boundary: an
// error body of exactly ForwardRPCMaxResponseBytes is decoded whole.
func TestForwardRPCErrorFrameBodyAtLimitDecodes(t *testing.T) {
	body := connectErrorBodyOfSize(t, ForwardRPCMaxResponseBytes)
	frame := forwardThrough(t,
		rawErrorHandler{status: http.StatusBadRequest, contentType: "application/json", body: body},
		"/test.Service/Method")
	if got := connect.Code(frame.GetErrorCode()); got != connect.CodeFailedPrecondition {
		t.Fatalf("error_code = %v, want failed_precondition", got)
	}
	wantLen := ForwardRPCMaxResponseBytes - len(`{"code":"failed_precondition","message":""}`)
	if len(frame.GetErrorMessage()) != wantLen || strings.Trim(frame.GetErrorMessage(), "x") != "" {
		t.Errorf("message length %d, want %d bytes of the decoded message", len(frame.GetErrorMessage()), wantLen)
	}
}

// TestForwardRPCErrorFrameBodyOverLimitIsNamed: an error body too large to
// read whole is reported by name, never as a fragment of truncated JSON.
func TestForwardRPCErrorFrameBodyOverLimitIsNamed(t *testing.T) {
	body := connectErrorBodyOfSize(t, ForwardRPCMaxResponseBytes+1)
	frame := forwardThrough(t,
		rawErrorHandler{status: http.StatusBadRequest, contentType: "application/json", body: body},
		"/test.Service/Method")
	want := fmt.Sprintf(
		"upstream error: HTTP 400 with an error body exceeding ForwardRPCMaxResponseBytes limit of %d bytes",
		ForwardRPCMaxResponseBytes)
	assertErrorFrame(t, frame, connect.CodeInvalidArgument, want)
}

// TestForwardRPCErrorCrossesGRPC drives the whole hop — client frame over a
// real gRPC stream to a cluster server, dispatch into the peer's Connect mux,
// error frame back — and checks what ForwardRPC hands its caller.
func TestForwardRPCErrorCrossesGRPC(t *testing.T) {
	const msg = "seal vault: open chunk can only be sealed on the vault's chunking leader \"node-3\""
	peer, err := New(Config{ClusterAddr: "127.0.0.1:0", NodeID: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	peer.Transport()
	peer.SetInternalHandler(failingVaultMux(connect.NewError(connect.CodeFailedPrecondition, errors.New(msg))))
	if err := peer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(peer.Stop)
	peers := NewStaticPeerConns("caller", func(id string) (string, bool) {
		return peer.Addr(), id == "owner"
	})
	t.Cleanup(func() { _ = peers.Close() })

	reqPayload, err := proto.Marshal(&gastrologv1.SealVaultRequest{Vault: "v"})
	if err != nil {
		t.Fatal(err)
	}
	payload, code, gotMsg, err := ForwardRPC(context.Background(), peers, "owner",
		gastrologv1connect.VaultServiceSealVaultProcedure, reqPayload)
	if err != nil {
		t.Fatalf("ForwardRPC transport error: %v", err)
	}
	if connect.Code(code) != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want failed_precondition", connect.Code(code))
	}
	if gotMsg != msg {
		t.Errorf("message = %q, want %q", gotMsg, msg)
	}
	if len(payload) != 0 {
		t.Errorf("error response carried a %d-byte payload", len(payload))
	}
}

// A non-Connect error body is arbitrary bytes, but error_message is a proto3
// string: the message must be valid UTF-8 for the frame to marshal at all.

const invalidUTF8Body = "chunk \xff\xfe\x80 unreadable: \xc3\x28 bad\n"

const invalidUTF8BodyMessage = "chunk � unreadable: �( bad"

// TestForwardRPCErrorFrameInvalidUTF8PlainText: a plain-text error body with
// invalid UTF-8 keeps its code and readable text, each invalid sequence
// replaced with U+FFFD, and the frame marshals.
func TestForwardRPCErrorFrameInvalidUTF8PlainText(t *testing.T) {
	frame := forwardThrough(t,
		rawErrorHandler{status: http.StatusServiceUnavailable, contentType: "text/plain", body: invalidUTF8Body},
		"/test.Service/Method")
	assertErrorFrame(t, frame, connect.CodeUnavailable, invalidUTF8BodyMessage)
	if _, err := proto.Marshal(frame); err != nil {
		t.Fatalf("error frame does not marshal: %v", err)
	}
}

// TestForwardRPCInvalidUTF8ErrorCrossesGRPC: over a real gRPC stream the
// caller receives the handler's error, not a transport failure.
func TestForwardRPCInvalidUTF8ErrorCrossesGRPC(t *testing.T) {
	payload, code, msg := forwardOverGRPC(t,
		rawErrorHandler{status: http.StatusServiceUnavailable, contentType: "text/plain", body: invalidUTF8Body}, nil)
	if connect.Code(code) != connect.CodeUnavailable {
		t.Errorf("code = %v, want unavailable (message %q)", connect.Code(code), msg)
	}
	if msg != invalidUTF8BodyMessage {
		t.Errorf("message = %q, want %q", msg, invalidUTF8BodyMessage)
	}
	if len(payload) != 0 {
		t.Errorf("error response carried a %d-byte payload", len(payload))
	}
}

// TestForwardRPCInvalidUTF8PlainTextOverLimitIsNamed: a plain-text body
// within the limit that outgrows it once each invalid byte becomes U+FFFD is
// refused by name instead of overflowing the frame.
func TestForwardRPCInvalidUTF8PlainTextOverLimitIsNamed(t *testing.T) {
	body := strings.Repeat("a\xff", ForwardRPCMaxResponseBytes/2)
	payload, code, msg := forwardOverGRPC(t,
		rawErrorHandler{status: http.StatusServiceUnavailable, contentType: "text/plain", body: body}, nil)
	if connect.Code(code) != connect.CodeUnavailable {
		t.Fatalf("code = %v, want unavailable (message %.120q)", connect.Code(code), msg)
	}
	want := fmt.Sprintf(
		"upstream error: HTTP 503 with a decoded error message exceeding ForwardRPCMaxResponseBytes limit of %d bytes",
		ForwardRPCMaxResponseBytes)
	if msg != want {
		t.Errorf("message = %.120q, want %q", msg, want)
	}
	if len(payload) != 0 {
		t.Errorf("error response carried a %d-byte payload", len(payload))
	}
}
