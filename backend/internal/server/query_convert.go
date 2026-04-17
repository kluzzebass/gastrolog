package server

import (
	"gastrolog/internal/glid"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// protoToQuery converts a proto Query to the internal query.Query type.
// If the Expression field is set, it is parsed server-side and takes
// precedence over the legacy Tokens/KvPredicates fields.
// Returns the pipeline if the expression contains pipe operators (e.g. "| stats count").
func protoToQuery(pq *apiv1.Query) (query.Query, *querylang.Pipeline, error) {
	if pq == nil {
		return query.Query{}, nil, nil
	}

	// If Expression is set, parse it server-side (same logic as repl/parse.go).
	// Proto-level fields (Limit, Start, End) override expression-level values
	// when set, so the frontend can control page size without injecting limit=
	// into the expression string.
	if pq.Expression != "" {
		q, pipeline, err := parseExpression(pq.Expression)
		if err != nil {
			return q, nil, err
		}
		if pq.Limit > 0 && q.Limit == 0 {
			q.Limit = int(pq.Limit)
		}
		if pq.Start != nil && q.Start.IsZero() {
			q.Start = pq.Start.AsTime()
		}
		if pq.End != nil && q.End.IsZero() {
			q.End = pq.End.AsTime()
		}
		return q, pipeline, nil
	}

	// Legacy path: use structured Tokens/KvPredicates fields.
	q := query.Query{
		Tokens:        pq.Tokens,
		Limit:         int(pq.Limit),
		ContextBefore: int(pq.ContextBefore),
		ContextAfter:  int(pq.ContextAfter),
	}

	if pq.Start != nil {
		q.Start = pq.Start.AsTime()
	}
	if pq.End != nil {
		q.End = pq.End.AsTime()
	}

	if len(pq.KvPredicates) > 0 {
		q.KV = make([]query.KeyValueFilter, len(pq.KvPredicates))
		for i, kv := range pq.KvPredicates {
			q.KV[i] = query.KeyValueFilter{Key: kv.Key, Value: kv.Value}
		}
	}

	return q, nil, nil
}

const maxExpressionLength = 4096

// parseExpression parses a raw query expression string into a Query and optional Pipeline.
// Control arguments (start=, end=, limit=) are extracted; the remainder
// ParseExpression parses a query expression string into a Query and optional Pipeline.
// Exported for use by the search executor in cluster forwarding.
func ParseExpression(expr string) (query.Query, *querylang.Pipeline, error) {
	return parseExpression(expr)
}

// is parsed through the pipeline parser. If the expression contains pipe
// operators (e.g. "| stats count"), the pipeline is returned; otherwise
// only the filter expression is set on the query.
func parseExpression(expr string) (query.Query, *querylang.Pipeline, error) {
	if len(expr) > maxExpressionLength {
		return query.Query{}, nil, fmt.Errorf("expression too long: %d bytes (max %d)", len(expr), maxExpressionLength)
	}
	expr = querylang.StripComments(expr)
	parts := strings.Fields(expr)
	if len(parts) == 0 {
		return query.Query{}, nil, nil
	}

	var q query.Query
	var filterParts []string

	for _, part := range parts {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			filterParts = append(filterParts, part)
			continue
		}
		consumed, err := applyDirective(&q, k, v)
		if err != nil {
			return q, nil, err
		}
		if !consumed {
			filterParts = append(filterParts, part)
		}
	}

	if len(filterParts) == 0 {
		return q, nil, nil
	}

	pipeline, err := querylang.ParsePipeline(strings.Join(filterParts, " "))
	if err != nil {
		return q, nil, fmt.Errorf("parse error: %w", err)
	}
	q.BoolExpr = pipeline.Filter
	if len(pipeline.Pipes) > 0 {
		return q, pipeline, nil
	}
	return q, nil, nil
}

func applyDirective(q *query.Query, k, v string) (bool, error) {
	switch k {
	case "reverse":
		q.IsReverse = v == "true"
		return true, nil
	case "start":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid start time: %w", err)
		}
		q.Start = t
		return true, nil
	case "end":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid end time: %w", err)
		}
		q.End = t
		return true, nil
	case "last":
		d, err := parseDuration(v)
		if err != nil {
			return false, fmt.Errorf("invalid last duration: %w", err)
		}
		now := time.Now()
		q.Start = now.Add(-d)
		q.End = now
		return true, nil
	case "source_start":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid source_start time: %w", err)
		}
		q.SourceStart = t
		return true, nil
	case "source_end":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid source_end time: %w", err)
		}
		q.SourceEnd = t
		return true, nil
	case "ingest_start":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid ingest_start time: %w", err)
		}
		q.Start = t
		return true, nil
	case "ingest_end":
		t, err := parseTime(v)
		if err != nil {
			return false, fmt.Errorf("invalid ingest_end time: %w", err)
		}
		q.End = t
		return true, nil
	case "limit":
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return false, fmt.Errorf("invalid limit: %w", err)
		}
		q.Limit = n
		return true, nil
	case "pos":
		var n uint64
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return false, fmt.Errorf("invalid pos: %w", err)
		}
		q.Pos = &n
		return true, nil
	case "order":
		switch v {
		case "ingest_ts":
			q.OrderBy = query.OrderByIngestTS
		case "source_ts":
			q.OrderBy = query.OrderBySourceTS
		default:
			return false, fmt.Errorf("invalid order: %s (use ingest_ts or source_ts)", v)
		}
		return true, nil
	default:
		return false, nil
	}
}

// parseDuration parses a duration string like "5m", "1h", or "3d".
// Extends time.ParseDuration with support for day suffixes.
func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		var days int
		if _, err := fmt.Sscanf(s, "%dd", &days); err == nil && days > 0 {
			return time.Duration(days) * 24 * time.Hour, nil
		}
	}
	return time.ParseDuration(s)
}

// parseTime parses a time string in RFC3339 format or as a Unix timestamp.
func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	var unix int64
	if n, err := fmt.Sscanf(s, "%d", &unix); err == nil && n == 1 && strconv.FormatInt(unix, 10) == s {
		return time.Unix(unix, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %s (use RFC3339 or Unix timestamp)", s)
}

// recordToProto converts an internal Record to the proto type.
func recordToProto(rec chunk.Record) *apiv1.Record {
	r := &apiv1.Record{
		IngestTs:   timestamppb.New(rec.IngestTS),
		WriteTs:    timestamppb.New(rec.WriteTS),
		Attrs:      rec.Attrs,
		Raw:        rec.Raw,
		IngestSeq:  rec.EventID.IngestSeq,
		IngesterId: rec.EventID.IngesterID[:],
		NodeId:     rec.EventID.NodeID[:],
		Ref: &apiv1.RecordRef{
			ChunkId: glid.GLID(rec.Ref.ChunkID).ToProto(),
			Pos:     rec.Ref.Pos,
			VaultId: rec.VaultID.ToProto(),
		},
	}
	if !rec.SourceTS.IsZero() {
		r.SourceTs = timestamppb.New(rec.SourceTS)
	}
	return r
}

// exportToRecord converts an ExportRecord to a Record proto.
func exportToRecord(er *apiv1.ExportRecord) *apiv1.Record {
	rec := &apiv1.Record{
		Raw:        er.Raw,
		SourceTs:   er.SourceTs,
		IngestTs:   er.IngestTs,
		WriteTs:    er.WriteTs,
		IngestSeq:  er.IngestSeq,
		IngesterId: er.IngesterId,
		NodeId:     er.NodeId,
	}
	if len(er.Attrs) > 0 {
		rec.Attrs = make(map[string]string, len(er.Attrs))
		maps.Copy(rec.Attrs, er.Attrs)
	}
	if len(er.VaultId) != 0 {
		rec.Ref = &apiv1.RecordRef{
			VaultId: er.VaultId,
			ChunkId: er.ChunkId,
			Pos:     er.Pos,
		}
	}
	return rec
}

// ProtoToResumeToken converts a proto resume token to the internal type.
func ProtoToResumeToken(data []byte) (*query.ResumeToken, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var protoToken apiv1.ResumeToken
	if err := proto.Unmarshal(data, &protoToken); err != nil {
		return nil, fmt.Errorf("unmarshal resume token: %w", err)
	}

	token := &query.ResumeToken{}
	if len(protoToken.VaultTokens) > 0 {
		token.VaultTokens = make(map[glid.GLID][]byte, len(protoToken.VaultTokens))
		for vidStr, tokenData := range protoToken.VaultTokens {
			vid, err := glid.ParseUUID(vidStr)
			if err != nil {
				continue
			}
			token.VaultTokens[vid] = tokenData
		}
	}
	if protoToken.FrozenStart != nil {
		token.FrozenStart = protoToken.FrozenStart.AsTime()
	}
	if protoToken.FrozenEnd != nil {
		token.FrozenEnd = protoToken.FrozenEnd.AsTime()
	}
	return token, nil
}

// VaultTokenToPositions deserializes a per-vault opaque token into Positions.
// Used by searchDirect to extract local vault resume state.
func VaultTokenToPositions(data []byte) ([]query.MultiVaultPosition, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var inner apiv1.InnerVaultToken
	if err := proto.Unmarshal(data, &inner); err != nil {
		return nil, err
	}
	positions := make([]query.MultiVaultPosition, len(inner.Positions))
	for i, pos := range inner.Positions {
		chunkID := chunk.ChunkID(glid.FromBytes(pos.ChunkId))
		vaultID := glid.FromBytes(pos.VaultId)
		mvp := query.MultiVaultPosition{
			VaultID:  vaultID,
			ChunkID:  chunkID,
			Position: pos.Position,
		}
		if pos.ResumeTs != nil {
			mvp.ResumeTS = pos.ResumeTs.AsTime()
		}
		positions[i] = mvp
	}
	return positions, nil
}

// PositionsToVaultToken serializes Positions into a per-vault opaque token.
func PositionsToVaultToken(positions []query.MultiVaultPosition) []byte {
	if len(positions) == 0 {
		return nil
	}
	inner := &apiv1.InnerVaultToken{
		Positions: make([]*apiv1.VaultPosition, len(positions)),
	}
	for i, pos := range positions {
		vp := &apiv1.VaultPosition{
			VaultId:  pos.VaultID.ToProto(),
			ChunkId:  glid.GLID(pos.ChunkID).ToProto(),
			Position: pos.Position,
		}
		if !pos.ResumeTS.IsZero() {
			vp.ResumeTs = timestamppb.New(pos.ResumeTS)
		}
		inner.Positions[i] = vp
	}
	data, err := proto.Marshal(inner)
	if err != nil {
		return nil
	}
	return data
}

// ResumeTokenToProto converts an internal resume token to proto bytes.
func ResumeTokenToProto(token *query.ResumeToken) []byte {
	if token == nil || (len(token.Positions) == 0 && len(token.VaultTokens) == 0) {
		return nil
	}

	protoToken := &apiv1.ResumeToken{}

	// If there are raw Positions (from eng.Search local), serialize them
	// as per-vault tokens grouped by vault ID.
	if len(token.Positions) > 0 {
		byVault := make(map[glid.GLID][]query.MultiVaultPosition)
		for _, pos := range token.Positions {
			byVault[pos.VaultID] = append(byVault[pos.VaultID], pos)
		}
		if token.VaultTokens == nil {
			token.VaultTokens = make(map[glid.GLID][]byte)
		}
		for vid, positions := range byVault {
			token.VaultTokens[vid] = PositionsToVaultToken(positions)
		}
	}

	if len(token.VaultTokens) > 0 {
		protoToken.VaultTokens = make(map[string][]byte, len(token.VaultTokens))
		for vid, tokenData := range token.VaultTokens {
			protoToken.VaultTokens[vid.String()] = tokenData
		}
	}
	if !token.FrozenStart.IsZero() {
		protoToken.FrozenStart = timestamppb.New(token.FrozenStart)
	}
	if !token.FrozenEnd.IsZero() {
		protoToken.FrozenEnd = timestamppb.New(token.FrozenEnd)
	}

	data, err := proto.Marshal(protoToken)
	if err != nil {
		return nil // Should not happen with valid data
	}
	return data
}
