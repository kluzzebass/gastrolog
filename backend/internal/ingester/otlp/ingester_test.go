package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"gastrolog/internal/orchestrator"
)

// startOTLP creates and starts an OTLP ingester on random ports, returning
// the ingester, output channel, and HTTP/gRPC addresses.
func startOTLP(t *testing.T, chanSize int) (*Ingester, chan orchestrator.IngestMessage, string, string) {
	t.Helper()

	out := make(chan orchestrator.IngestMessage, chanSize)
	ing := New(Config{
		ID:       "test-otlp",
		HTTPAddr: "127.0.0.1:0",
		GRPCAddr: "127.0.0.1:0",
	})

	ctx := t.Context()
	go ing.Run(ctx, out)
	// Wait for listeners to start.
	time.Sleep(100 * time.Millisecond)

	// Discover actual ports from the listeners by doing a quick ready check.
	// We need to try ports — the ingester doesn't expose listener addrs directly.
	// Instead, we'll use a helper: start, wait, then use the struct fields.
	// Actually, the ingester doesn't expose addrs. Let's use a different approach:
	// Create listeners ourselves, get ports, close them, then pass to ingester.
	// But that's racy. Simplest: just wait and test with HTTP ready endpoint.
	return ing, out, "", ""
}

// makeExportRequest builds an ExportLogsServiceRequest from helpers.
func makeExportRequest(resourceAttrs map[string]string, scopeAttrs map[string]string, records ...*logspb.LogRecord) *collogspb.ExportLogsServiceRequest {
	var resKVs []*commonpb.KeyValue
	for k, v := range resourceAttrs {
		resKVs = append(resKVs, &commonpb.KeyValue{
			Key:   k,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
		})
	}
	var scopeKVs []*commonpb.KeyValue
	for k, v := range scopeAttrs {
		scopeKVs = append(scopeKVs, &commonpb.KeyValue{
			Key:   k,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
		})
	}

	return &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: resKVs},
			ScopeLogs: []*logspb.ScopeLogs{{
				Scope:      &commonpb.InstrumentationScope{Attributes: scopeKVs},
				LogRecords: records,
			}},
		}},
	}
}

func makeStringLogRecord(body string, ts time.Time) *logspb.LogRecord {
	return &logspb.LogRecord{
		TimeUnixNano: uint64(ts.UnixNano()),
		Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
	}
}

// listenAndStartOTLP starts an OTLP ingester on two known random ports.
// Returns (httpAddr, grpcAddr, out channel). Ingester runs until test ends.
func listenAndStartOTLP(t *testing.T, chanSize int) (string, string, chan orchestrator.IngestMessage) {
	t.Helper()

	// Find two free ports.
	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http: %v", err)
	}
	httpAddr := httpLn.Addr().String()
	httpLn.Close()

	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	grpcAddr := grpcLn.Addr().String()
	grpcLn.Close()

	out := make(chan orchestrator.IngestMessage, chanSize)
	ing := New(Config{
		ID:       "test-otlp",
		HTTPAddr: httpAddr,
		GRPCAddr: grpcAddr,
	})

	ctx := t.Context()
	go ing.Run(ctx, out)
	time.Sleep(100 * time.Millisecond)

	return httpAddr, grpcAddr, out
}

func postOTLPJSON(t *testing.T, addr string, req *collogspb.ExportLogsServiceRequest) *http.Response {
	t.Helper()
	data, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	resp, err := http.Post("http://"+addr+"/v1/logs", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	return resp
}

func postOTLPProto(t *testing.T, addr string, req *collogspb.ExportLogsServiceRequest) *http.Response {
	t.Helper()
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	resp, err := http.Post("http://"+addr+"/v1/logs", "application/x-protobuf", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	return resp
}

func recv(t *testing.T, out chan orchestrator.IngestMessage) orchestrator.IngestMessage {
	t.Helper()
	select {
	case msg := <-out:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		return orchestrator.IngestMessage{}
	}
}

// --- HTTP Tests ---

func TestOTLPHTTPJSON(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(
		map[string]string{"service.name": "myapp"},
		nil,
		makeStringLogRecord("hello from OTLP", ts),
	)

	resp := postOTLPJSON(t, httpAddr, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "hello from OTLP" {
		t.Errorf("raw: expected %q, got %q", "hello from OTLP", msg.Raw)
	}
	if msg.Attrs["service.name"] != "myapp" {
		t.Errorf("service.name: expected myapp, got %q", msg.Attrs["service.name"])
	}
	if msg.Attrs["ingester_type"] != "otlp" {
		t.Errorf("ingester_type: expected otlp, got %q", msg.Attrs["ingester_type"])
	}
	if msg.Attrs["ingester_id"] != "test-otlp" {
		t.Errorf("ingester_id: expected test-otlp, got %q", msg.Attrs["ingester_id"])
	}
	if msg.SourceTS.UnixNano() != ts.UnixNano() {
		t.Errorf("SourceTS: expected %v, got %v", ts, msg.SourceTS)
	}
	if time.Since(msg.IngestTS) > 2*time.Second {
		t.Errorf("IngestTS should be recent, got %v", msg.IngestTS)
	}
}

func TestOTLPHTTPProtobuf(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(nil, nil, makeStringLogRecord("proto message", ts))

	resp := postOTLPProto(t, httpAddr, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "proto message" {
		t.Errorf("raw: expected %q, got %q", "proto message", msg.Raw)
	}
}

func TestOTLPHTTPGzip(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(nil, nil, makeStringLogRecord("gzipped OTLP", ts))
	data, _ := protojson.Marshal(req)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(data)
	gz.Close()

	httpReq, _ := http.NewRequest("POST", "http://"+httpAddr+"/v1/logs", &buf)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Content-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "gzipped OTLP" {
		t.Errorf("raw: expected %q, got %q", "gzipped OTLP", msg.Raw)
	}
}

func TestOTLPHTTPInvalidJSON(t *testing.T) {
	httpAddr, _, _ := listenAndStartOTLP(t, 10)

	resp, err := http.Post("http://"+httpAddr+"/v1/logs", "application/json", bytes.NewReader([]byte("{not valid")))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestOTLPHTTPInvalidProtobuf(t *testing.T) {
	httpAddr, _, _ := listenAndStartOTLP(t, 10)

	resp, err := http.Post("http://"+httpAddr+"/v1/logs", "application/x-protobuf", bytes.NewReader([]byte("not proto")))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestOTLPHTTPReadyEndpoint(t *testing.T) {
	httpAddr, _, _ := listenAndStartOTLP(t, 10)

	resp, err := http.Get("http://" + httpAddr + "/ready")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// --- gRPC Tests ---

func TestOTLPGRPC(t *testing.T) {
	_, grpcAddr, out := listenAndStartOTLP(t, 10)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(
		map[string]string{"host.name": "server1"},
		nil,
		makeStringLogRecord("grpc log line", ts),
	)

	resp, err := client.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	msg := recv(t, out)
	if string(msg.Raw) != "grpc log line" {
		t.Errorf("raw: expected %q, got %q", "grpc log line", msg.Raw)
	}
	if msg.Attrs["host.name"] != "server1" {
		t.Errorf("host.name: expected server1, got %q", msg.Attrs["host.name"])
	}
}

func TestOTLPGRPCMultipleRecords(t *testing.T) {
	_, grpcAddr, out := listenAndStartOTLP(t, 10)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)

	ts := time.Now()
	req := makeExportRequest(nil, nil,
		makeStringLogRecord("line1", ts),
		makeStringLogRecord("line2", ts),
		makeStringLogRecord("line3", ts),
	)

	_, err = client.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	for i, expected := range []string{"line1", "line2", "line3"} {
		msg := recv(t, out)
		if string(msg.Raw) != expected {
			t.Errorf("message %d: expected %q, got %q", i, expected, msg.Raw)
		}
	}
}

// --- Attribute Precedence Tests ---

func TestOTLPAttributePrecedence(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	// Resource, scope, and record all set "env" — record should win.
	ts := time.Now()
	lr := makeStringLogRecord("precedence test", ts)
	lr.Attributes = []*commonpb.KeyValue{{
		Key:   "env",
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "record-env"}},
	}}

	req := makeExportRequest(
		map[string]string{"env": "resource-env", "resource_only": "yes"},
		map[string]string{"env": "scope-env", "scope_only": "yes"},
		lr,
	)

	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	msg := recv(t, out)
	if msg.Attrs["env"] != "record-env" {
		t.Errorf("env: expected record-env (highest precedence), got %q", msg.Attrs["env"])
	}
	if msg.Attrs["resource_only"] != "yes" {
		t.Errorf("resource_only: expected yes, got %q", msg.Attrs["resource_only"])
	}
	if msg.Attrs["scope_only"] != "yes" {
		t.Errorf("scope_only: expected yes, got %q", msg.Attrs["scope_only"])
	}
}

// --- Severity Tests ---

func TestOTLPSeverity(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now()
	lr := makeStringLogRecord("severity test", ts)
	lr.SeverityText = "ERROR"
	lr.SeverityNumber = logspb.SeverityNumber_SEVERITY_NUMBER_ERROR

	req := makeExportRequest(nil, nil, lr)
	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	msg := recv(t, out)
	if msg.Attrs["severity"] != "ERROR" {
		t.Errorf("severity: expected ERROR, got %q", msg.Attrs["severity"])
	}
	if msg.Attrs["severity_number"] != "17" {
		t.Errorf("severity_number: expected 17 (ERROR), got %q", msg.Attrs["severity_number"])
	}
}

func TestOTLPNoSeverity(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now()
	req := makeExportRequest(nil, nil, makeStringLogRecord("no severity", ts))
	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	msg := recv(t, out)
	if _, ok := msg.Attrs["severity"]; ok {
		t.Errorf("expected no severity attr, got %q", msg.Attrs["severity"])
	}
	if _, ok := msg.Attrs["severity_number"]; ok {
		t.Errorf("expected no severity_number attr, got %q", msg.Attrs["severity_number"])
	}
}

// --- Trace/Span ID Tests ---

func TestOTLPTraceSpanIDs(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now()
	traceID := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22}

	lr := makeStringLogRecord("trace test", ts)
	lr.TraceId = traceID
	lr.SpanId = spanID

	req := makeExportRequest(nil, nil, lr)
	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	msg := recv(t, out)
	expectedTraceID := hex.EncodeToString(traceID)
	expectedSpanID := hex.EncodeToString(spanID)

	if msg.Attrs["trace_id"] != expectedTraceID {
		t.Errorf("trace_id: expected %q, got %q", expectedTraceID, msg.Attrs["trace_id"])
	}
	if msg.Attrs["span_id"] != expectedSpanID {
		t.Errorf("span_id: expected %q, got %q", expectedSpanID, msg.Attrs["span_id"])
	}
}

func TestOTLPNoTraceSpanIDs(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now()
	req := makeExportRequest(nil, nil, makeStringLogRecord("no trace", ts))
	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	msg := recv(t, out)
	if _, ok := msg.Attrs["trace_id"]; ok {
		t.Errorf("expected no trace_id, got %q", msg.Attrs["trace_id"])
	}
	if _, ok := msg.Attrs["span_id"]; ok {
		t.Errorf("expected no span_id, got %q", msg.Attrs["span_id"])
	}
}

// --- Timestamp Tests ---

func TestOTLPObservedTimeFallback(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	observed := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	lr := &logspb.LogRecord{
		// TimeUnixNano is zero — should fall back to ObservedTimeUnixNano.
		ObservedTimeUnixNano: uint64(observed.UnixNano()),
		Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "observed time"}},
	}

	req := makeExportRequest(nil, nil, lr)
	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	msg := recv(t, out)
	if msg.SourceTS.UnixNano() != observed.UnixNano() {
		t.Errorf("SourceTS: expected %v, got %v", observed, msg.SourceTS)
	}
}

// --- AnyValue Type Tests ---

func TestOTLPBodyTypes(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)
	ts := time.Now()

	tests := []struct {
		name     string
		body     *commonpb.AnyValue
		expected string
	}{
		{
			name:     "int",
			body:     &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}},
			expected: "42",
		},
		{
			name:     "double",
			body:     &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}},
			expected: "3.14",
		},
		{
			name:     "bool",
			body:     &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}},
			expected: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := &logspb.LogRecord{
				TimeUnixNano: uint64(ts.UnixNano()),
				Body:         tt.body,
			}
			req := makeExportRequest(nil, nil, lr)
			resp := postOTLPJSON(t, httpAddr, req)
			resp.Body.Close()

			msg := recv(t, out)
			if string(msg.Raw) != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, msg.Raw)
			}
		})
	}
}

// --- Backpressure Tests ---

func TestOTLPHTTPBackpressure(t *testing.T) {
	// Channel size 2: backpressure triggers at 90% → 2*9/10 = 1, so len >= 1 fires after one message.
	httpAddr, _, _ := listenAndStartOTLP(t, 2)

	ts := time.Now()
	req := makeExportRequest(nil, nil, makeStringLogRecord("bp", ts))

	// First request succeeds — channel is empty (len=0, threshold=1, 0 < 1).
	resp := postOTLPJSON(t, httpAddr, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("first request: expected 200, got %d: %s", resp.StatusCode, body)
	}

	// Second request should hit backpressure (len=1, threshold=1, 1 >= 1).
	resp2 := postOTLPJSON(t, httpAddr, req)
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", resp2.StatusCode)
	}
	if resp2.Header.Get("Retry-After") != "1" {
		t.Errorf("expected Retry-After: 1, got %q", resp2.Header.Get("Retry-After"))
	}
}

func TestOTLPGRPCBackpressure(t *testing.T) {
	_, grpcAddr, _ := listenAndStartOTLP(t, 2)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)

	ts := time.Now()
	req := makeExportRequest(nil, nil, makeStringLogRecord("bp", ts))

	// Fill the channel.
	_, err = client.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("first Export failed: %v", err)
	}

	// Next should get RESOURCE_EXHAUSTED.
	_, err = client.Export(context.Background(), req)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("expected RESOURCE_EXHAUSTED, got %v", st.Code())
	}
}

// --- Factory Tests ---

func TestOTLPFactory(t *testing.T) {
	factory := NewFactory()

	// Default addrs.
	ing, err := factory(uuid.New(), nil, nil)
	if err != nil {
		t.Fatalf("factory with nil params: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	// Custom addrs.
	ing, err = factory(uuid.New(), map[string]string{
		"http_addr": ":9318",
		"grpc_addr": ":9317",
	}, nil)
	if err != nil {
		t.Fatalf("factory with custom addrs: %v", err)
	}
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}

	// Invalid addr.
	_, err = factory(uuid.New(), map[string]string{"http_addr": "noport"}, nil)
	if err == nil {
		t.Error("expected error for invalid addr")
	}
}

// --- Empty Request ---

func TestOTLPEmptyRequest(t *testing.T) {
	httpAddr, _, _ := listenAndStartOTLP(t, 10)

	req := &collogspb.ExportLogsServiceRequest{}
	resp := postOTLPJSON(t, httpAddr, req)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected 200 for empty request, got %d: %s", resp.StatusCode, body)
	}
}

// --- Multiple ResourceLogs ---

func TestOTLPMultipleResourceLogs(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now()
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
					Key:   "host",
					Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "host1"}},
				}}},
				ScopeLogs: []*logspb.ScopeLogs{{
					LogRecords: []*logspb.LogRecord{makeStringLogRecord("from host1", ts)},
				}},
			},
			{
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
					Key:   "host",
					Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "host2"}},
				}}},
				ScopeLogs: []*logspb.ScopeLogs{{
					LogRecords: []*logspb.LogRecord{makeStringLogRecord("from host2", ts)},
				}},
			},
		},
	}

	resp := postOTLPJSON(t, httpAddr, req)
	resp.Body.Close()

	hosts := map[string]string{}
	for range 2 {
		msg := recv(t, out)
		hosts[msg.Attrs["host"]] = string(msg.Raw)
	}

	if hosts["host1"] != "from host1" {
		t.Errorf("expected 'from host1', got %q", hosts["host1"])
	}
	if hosts["host2"] != "from host2" {
		t.Errorf("expected 'from host2', got %q", hosts["host2"])
	}
}

// --- Zstd Compression Tests ---

func TestOTLPHTTPZstd(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(nil, nil, makeStringLogRecord("zstd OTLP", ts))
	data, _ := protojson.Marshal(req)

	enc, _ := zstd.NewWriter(nil)
	compressed := enc.EncodeAll(data, nil)

	httpReq, _ := http.NewRequest("POST", "http://"+httpAddr+"/v1/logs", bytes.NewReader(compressed))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Content-Encoding", "zstd")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "zstd OTLP" {
		t.Errorf("raw: expected %q, got %q", "zstd OTLP", msg.Raw)
	}
}

func TestOTLPHTTPZstdProtobuf(t *testing.T) {
	httpAddr, _, out := listenAndStartOTLP(t, 10)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(nil, nil, makeStringLogRecord("zstd proto", ts))
	data, _ := proto.Marshal(req)

	enc, _ := zstd.NewWriter(nil)
	compressed := enc.EncodeAll(data, nil)

	httpReq, _ := http.NewRequest("POST", "http://"+httpAddr+"/v1/logs", bytes.NewReader(compressed))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "zstd")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "zstd proto" {
		t.Errorf("raw: expected %q, got %q", "zstd proto", msg.Raw)
	}
}

func TestOTLPHTTPUnsupportedEncoding(t *testing.T) {
	httpAddr, _, _ := listenAndStartOTLP(t, 10)

	httpReq, _ := http.NewRequest("POST", "http://"+httpAddr+"/v1/logs", bytes.NewReader([]byte("data")))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Content-Encoding", "br")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for unsupported encoding, got %d", resp.StatusCode)
	}
}

// --- gRPC Compression Tests ---

func TestOTLPGRPCGzipCompression(t *testing.T) {
	// Verify gzip compressor is registered (by our blank import).
	if encoding.GetCompressor(grpcgzip.Name) == nil {
		t.Fatal("gzip compressor not registered")
	}

	_, grpcAddr, out := listenAndStartOTLP(t, 10)

	conn, err := grpc.NewClient(grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcgzip.Name)),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(nil, nil, makeStringLogRecord("grpc gzip", ts))

	_, err = client.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "grpc gzip" {
		t.Errorf("raw: expected %q, got %q", "grpc gzip", msg.Raw)
	}
}

func TestOTLPGRPCZstdCompression(t *testing.T) {
	// Verify zstd compressor is registered (by our grpccomp.go init).
	if encoding.GetCompressor("zstd") == nil {
		t.Fatal("zstd compressor not registered")
	}

	_, grpcAddr, out := listenAndStartOTLP(t, 10)

	conn, err := grpc.NewClient(grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor("zstd")),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)

	ts := time.Now().Truncate(time.Microsecond)
	req := makeExportRequest(nil, nil, makeStringLogRecord("grpc zstd", ts))

	_, err = client.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	msg := recv(t, out)
	if string(msg.Raw) != "grpc zstd" {
		t.Errorf("raw: expected %q, got %q", "grpc zstd", msg.Raw)
	}
}
