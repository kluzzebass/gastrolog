package server

import (
	"context"
	"sort"

	"connectrpc.com/connect"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/tokenizer"
)

const (
	defaultFieldSamples = 500
	maxFieldSamples     = 2000
	maxTopValues        = 10
)

// GetFields samples matching records, runs the backend's full extractor
// suite (KV, logfmt, access log), and returns aggregated field names with
// value distributions. This replaces client-side field extraction.
func (s *QueryServer) GetFields(
	ctx context.Context,
	req *connect.Request[apiv1.GetFieldsRequest],
) (*connect.Response[apiv1.GetFieldsResponse], error) {
	q, _, err := parseExpression(req.Msg.Expression)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	maxSamples := int(req.Msg.MaxSamples)
	if maxSamples <= 0 {
		maxSamples = defaultFieldSamples
	}
	if maxSamples > maxFieldSamples {
		maxSamples = maxFieldSamples
	}
	q.Limit = maxSamples

	eng := s.orch.MultiVaultQueryEngine()
	searchIter, _ := eng.Search(ctx, q, nil)

	attrAgg := newFieldAggregator()
	kvAgg := newFieldAggregator()
	extractors := tokenizer.DefaultExtractors()

	for rec, err := range searchIter {
		if err != nil {
			break
		}

		// Attrs are first-class structured fields from the ingester.
		for k, v := range rec.Attrs {
			attrAgg.add(k, v)
		}

		// Run the full extractor suite on the raw message body.
		kvPairs := tokenizer.CombinedExtract(rec.Raw, extractors)
		for _, kv := range kvPairs {
			kvAgg.add(kv.Key, kv.Value)
		}
	}

	// Include records from remote nodes in the cluster.
	remoteIter, _, _ := s.collectRemote(ctx, q, nil)
	if remoteIter != nil {
		for rec, iterErr := range remoteIter {
			if iterErr != nil {
				break
			}
			for k, v := range rec.Attrs {
				attrAgg.add(k, v)
			}
			kvPairs := tokenizer.CombinedExtract(rec.Raw, extractors)
			for _, kv := range kvPairs {
				kvAgg.add(kv.Key, kv.Value)
			}
		}
	}

	kvFields := kvAgg.toProto()
	// Skip "level" from KV fields — it's handled by the severity system.
	kvFields = filterOutKey(kvFields, "level")

	return connect.NewResponse(&apiv1.GetFieldsResponse{
		AttrFields: attrAgg.toProto(),
		KvFields:   kvFields,
	}), nil
}

// fieldAggregator counts occurrences of (key, value) pairs for field discovery.
type fieldAggregator struct {
	keys map[string]*fieldEntry
}

type fieldEntry struct {
	count  int
	values map[string]int
}

func newFieldAggregator() *fieldAggregator {
	return &fieldAggregator{keys: make(map[string]*fieldEntry)}
}

func (a *fieldAggregator) add(key, value string) {
	e := a.keys[key]
	if e == nil {
		e = &fieldEntry{values: make(map[string]int)}
		a.keys[key] = e
	}
	e.count++
	e.values[value]++
}

func (a *fieldAggregator) toProto() []*apiv1.FieldInfo {
	fields := make([]*apiv1.FieldInfo, 0, len(a.keys))
	for key, entry := range a.keys {
		fi := &apiv1.FieldInfo{
			Key:   key,
			Count: int32(entry.count), //nolint:gosec // G115: field count fits in int32
		}

		// Sort values by count descending, take top N.
		type valCount struct {
			value string
			count int
		}
		sorted := make([]valCount, 0, len(entry.values))
		for v, c := range entry.values {
			sorted = append(sorted, valCount{v, c})
		}
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].count > sorted[j].count
		})
		limit := min(maxTopValues, len(sorted))
		fi.TopValues = make([]*apiv1.FieldValue, limit)
		for i := range limit {
			fi.TopValues[i] = &apiv1.FieldValue{
				Value: sorted[i].value,
				Count: int32(sorted[i].count), //nolint:gosec // G115: value count fits in int32
			}
		}

		fields = append(fields, fi)
	}

	// Sort by count descending.
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Count > fields[j].Count
	})

	return fields
}

func filterOutKey(fields []*apiv1.FieldInfo, key string) []*apiv1.FieldInfo {
	result := make([]*apiv1.FieldInfo, 0, len(fields))
	for _, f := range fields {
		if f.Key != key {
			result = append(result, f)
		}
	}
	return result
}
