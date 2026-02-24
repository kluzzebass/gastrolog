// Package otlp provides an OTLP ingester that accepts OpenTelemetry log records
// via HTTP (POST /v1/logs) and gRPC (LogsService/Export).
package otlp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"gastrolog/internal/ingester/bodyutil"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Ingester accepts OpenTelemetry log records via HTTP and gRPC.
type Ingester struct {
	id       string
	httpAddr string
	grpcAddr string
	out      chan<- orchestrator.IngestMessage
	logger   *slog.Logger
}

// Config holds OTLP ingester configuration.
type Config struct {
	ID       string
	HTTPAddr string // e.g. ":4318"
	GRPCAddr string // e.g. ":4317"
	Logger   *slog.Logger
}

// New creates a new OTLP ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		id:       cfg.ID,
		httpAddr: cfg.HTTPAddr,
		grpcAddr: cfg.GRPCAddr,
		logger:   logging.Default(cfg.Logger).With("component", "ingester", "type", "otlp"),
	}
}

// Run starts both HTTP and gRPC servers and blocks until ctx is cancelled.
func (ing *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	ing.out = out

	errCh := make(chan error, 2)

	// Start HTTP server.
	httpLn, err := net.Listen("tcp", ing.httpAddr)
	if err != nil {
		return fmt.Errorf("otlp http listen: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", ing.handleHTTP)
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	httpSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		if err := httpSrv.Serve(httpLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("otlp http: %w", err)
		}
	}()
	ing.logger.Info("otlp http listening", "addr", httpLn.Addr().String())

	// Start gRPC server.
	grpcLn, err := net.Listen("tcp", ing.grpcAddr)
	if err != nil {
		_ = httpSrv.Close()
		return fmt.Errorf("otlp grpc listen: %w", err)
	}

	grpcSrv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(grpcSrv, &logsServiceServer{ing: ing})

	go func() {
		if err := grpcSrv.Serve(grpcLn); err != nil {
			errCh <- fmt.Errorf("otlp grpc: %w", err)
		}
	}()
	ing.logger.Info("otlp grpc listening", "addr", grpcLn.Addr().String())

	// Wait for shutdown or error.
	select {
	case <-ctx.Done():
		ing.logger.Info("otlp ingester stopping")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		grpcSrv.GracefulStop()
		return nil
	case err := <-errCh:
		_ = httpSrv.Close()
		grpcSrv.Stop()
		return err
	}
}

// handleHTTP handles POST /v1/logs requests.
// Accepts protobuf (application/x-protobuf) and JSON (application/json).
func (ing *Ingester) handleHTTP(w http.ResponseWriter, req *http.Request) {
	data, err := bodyutil.ReadBody(req.Body, req.Header.Get("Content-Encoding"), 10<<20)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	exportReq := &collogspb.ExportLogsServiceRequest{}

	ct := req.Header.Get("Content-Type")
	switch ct {
	case "application/x-protobuf", "application/protobuf":
		if err := proto.Unmarshal(data, exportReq); err != nil {
			http.Error(w, "invalid protobuf", http.StatusBadRequest)
			return
		}
	default:
		// Default to JSON (the OTLP/HTTP spec recommends JSON as default).
		if err := protojson.Unmarshal(data, exportReq); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
	}

	if err := ing.processExportRequest(req.Context(), exportReq); err != nil {
		if errors.Is(err, errBackpressure) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "queue full, retry later", http.StatusTooManyRequests)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with empty ExportLogsServiceResponse.
	resp := &collogspb.ExportLogsServiceResponse{}
	respData, _ := proto.Marshal(resp)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respData)
}

// errBackpressure signals the queue is near capacity.
var errBackpressure = errors.New("backpressure: ingest queue near capacity")

// processExportRequest converts OTLP log records to IngestMessages and sends them.
func (ing *Ingester) processExportRequest(ctx context.Context, req *collogspb.ExportLogsServiceRequest) error {
	if c := cap(ing.out); c > 0 && len(ing.out) >= c*9/10 {
		return errBackpressure
	}

	now := time.Now()

	for _, rl := range req.GetResourceLogs() {
		resourceAttrs := flattenKVList(rl.GetResource().GetAttributes())

		for _, sl := range rl.GetScopeLogs() {
			scopeAttrs := flattenKVList(sl.GetScope().GetAttributes())

			for _, lr := range sl.GetLogRecords() {
				msg := ing.logRecordToMessage(lr, resourceAttrs, scopeAttrs, now)
				select {
				case ing.out <- msg:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return nil
}

func (ing *Ingester) logRecordToMessage(lr *logspb.LogRecord, resourceAttrs, scopeAttrs map[string]string, now time.Time) orchestrator.IngestMessage {
	attrs := make(map[string]string, len(resourceAttrs)+len(scopeAttrs)+8)

	maps.Copy(attrs, resourceAttrs)
	maps.Copy(attrs, scopeAttrs)
	maps.Copy(attrs, flattenKVList(lr.GetAttributes()))

	if lr.GetSeverityText() != "" {
		attrs["severity"] = lr.GetSeverityText()
	}
	if lr.GetSeverityNumber() != logspb.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		attrs["severity_number"] = strconv.Itoa(int(lr.GetSeverityNumber()))
	}
	if len(lr.GetTraceId()) > 0 {
		attrs["trace_id"] = hex.EncodeToString(lr.GetTraceId())
	}
	if len(lr.GetSpanId()) > 0 {
		attrs["span_id"] = hex.EncodeToString(lr.GetSpanId())
	}

	attrs["ingester_type"] = "otlp"
	attrs["ingester_id"] = ing.id

	var sourceTS time.Time
	switch {
	case lr.GetTimeUnixNano() != 0:
		sourceTS = time.Unix(0, int64(lr.GetTimeUnixNano())) //nolint:gosec // G115: OTLP nanosecond timestamps are well within int64 range
	case lr.GetObservedTimeUnixNano() != 0:
		sourceTS = time.Unix(0, int64(lr.GetObservedTimeUnixNano())) //nolint:gosec // G115: OTLP nanosecond timestamps are well within int64 range
	}

	return orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      []byte(anyValueToString(lr.GetBody())),
		SourceTS: sourceTS,
		IngestTS: now,
	}
}

// logsServiceServer implements the gRPC LogsService.
type logsServiceServer struct {
	collogspb.UnimplementedLogsServiceServer
	ing *Ingester
}

func (s *logsServiceServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	if err := s.ing.processExportRequest(ctx, req); err != nil {
		if errors.Is(err, errBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue near capacity")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// flattenKVList converts OTLP KeyValue list to a flat map, stringifying values.
func flattenKVList(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(val.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		data, _ := json.Marshal(arrayValueToSlice(val.ArrayValue))
		return string(data)
	case *commonpb.AnyValue_KvlistValue:
		data, _ := json.Marshal(kvListToMap(val.KvlistValue))
		return string(data)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func arrayValueToSlice(av *commonpb.ArrayValue) []any {
	if av == nil {
		return nil
	}
	out := make([]any, len(av.GetValues()))
	for i, v := range av.GetValues() {
		out[i] = anyValueToString(v)
	}
	return out
}

func kvListToMap(kv *commonpb.KeyValueList) map[string]string {
	if kv == nil {
		return nil
	}
	return flattenKVList(kv.GetValues())
}
