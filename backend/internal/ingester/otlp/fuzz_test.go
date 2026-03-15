package otlp

import (
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"gastrolog/internal/orchestrator"
)

// FuzzAnyValueToString feeds random proto-encoded AnyValue messages into
// the string conversion function. It must never panic regardless of content.
func FuzzAnyValueToString(f *testing.F) {
	// String value.
	sv := &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}
	if b, err := proto.Marshal(sv); err == nil {
		f.Add(b)
	}

	// Int value.
	iv := &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}
	if b, err := proto.Marshal(iv); err == nil {
		f.Add(b)
	}

	// Double value.
	dv := &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}
	if b, err := proto.Marshal(dv); err == nil {
		f.Add(b)
	}

	// Bool value.
	bv := &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}
	if b, err := proto.Marshal(bv); err == nil {
		f.Add(b)
	}

	// Bytes value.
	bytesV := &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xDE, 0xAD}}}
	if b, err := proto.Marshal(bytesV); err == nil {
		f.Add(b)
	}

	// Array value.
	av := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{
		Values: []*commonpb.AnyValue{sv, iv},
	}}}
	if b, err := proto.Marshal(av); err == nil {
		f.Add(b)
	}

	// KvList value.
	kv := &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
		Values: []*commonpb.KeyValue{
			{Key: "k1", Value: sv},
			{Key: "k2", Value: iv},
		},
	}}}
	if b, err := proto.Marshal(kv); err == nil {
		f.Add(b)
	}

	// Nil / empty.
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		v := &commonpb.AnyValue{}
		if err := proto.Unmarshal(data, v); err != nil {
			// Invalid proto — still call with nil to cover that path.
			_ = anyValueToString(nil)
			return
		}
		_ = anyValueToString(v)
	})
}

// FuzzFlattenKVList feeds random proto-encoded KeyValue lists into flattenKVList.
func FuzzFlattenKVList(f *testing.F) {
	kv := &commonpb.KeyValueList{
		Values: []*commonpb.KeyValue{
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my-svc"}}},
			{Key: "count", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 10}}},
		},
	}
	if b, err := proto.Marshal(kv); err == nil {
		f.Add(b)
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		kv := &commonpb.KeyValueList{}
		if err := proto.Unmarshal(data, kv); err != nil {
			return
		}
		_ = flattenKVList(kv.GetValues())
	})
}

// FuzzLogRecordToMessage feeds random proto-encoded LogRecord messages through
// the OTLP-to-IngestMessage conversion. Must never panic.
func FuzzLogRecordToMessage(f *testing.F) {
	lr := &logspb.LogRecord{
		TimeUnixNano:   1700000000000000000,
		SeverityText:   "ERROR",
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "something failed"}},
		Attributes: []*commonpb.KeyValue{
			{Key: "host", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "node-1"}}},
		},
		TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	if b, err := proto.Marshal(lr); err == nil {
		f.Add(b)
	}

	// Minimal record.
	minLR := &logspb.LogRecord{}
	if b, err := proto.Marshal(minLR); err == nil {
		f.Add(b)
	}

	f.Add([]byte{})
	f.Add([]byte{0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		lr := &logspb.LogRecord{}
		if err := proto.Unmarshal(data, lr); err != nil {
			return
		}

		// Create a minimal ingester just for the conversion method.
		ing := &Ingester{id: "fuzz-otlp"}
		_ = ing.logRecordToMessage(lr, nil, nil, time.Now())
	})
}

// FuzzExportLogsServiceRequest feeds random proto bytes into the full
// ExportLogsServiceRequest → processExportRequest path (minus channel send).
func FuzzExportLogsServiceRequest(f *testing.F) {
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log"}},
							},
						},
					},
				},
			},
		},
	}
	if b, err := proto.Marshal(req); err == nil {
		f.Add(b)
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		req := &collogspb.ExportLogsServiceRequest{}
		if err := proto.Unmarshal(data, req); err != nil {
			return
		}

		// Use a buffered channel so processExportRequest doesn't block.
		out := make(chan orchestrator.IngestMessage, 1000)
		ing := &Ingester{id: "fuzz-otlp", out: out}

		ctx := t.Context()
		_ = ing.processExportRequest(ctx, req)

		// Drain channel.
		close(out)
		for range out {
		}
	})
}
