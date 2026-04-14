package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"gastrolog/internal/glid"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// printer handles table or JSON output.
type printer struct {
	format string
	w      io.Writer
}

func newPrinter(format string) *printer {
	return &printer{format: format, w: os.Stdout}
}

// json marshals v as indented JSON. For proto messages, uses protojson then
// post-processes to convert GLID byte fields (base64) to base32hex strings
// so the output is human-readable and round-trippable through the CLI.
func (p *printer) json(v any) error {
	// Single proto message.
	if msg, ok := v.(proto.Message); ok {
		return p.writeProtoJSON(msg)
	}
	// Try to handle slices of proto messages via encoding/json + post-processing.
	enc := json.NewEncoder(p.w)
	enc.SetIndent("", "  ")
	// Marshal, then decode and convert GLID fields.
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	converted := convertGLIDFields(raw)
	var indented json.RawMessage
	if err := json.Unmarshal(converted, &indented); err != nil {
		return enc.Encode(v) // fallback
	}
	return enc.Encode(indented)
}

func (p *printer) writeProtoJSON(msg proto.Message) error {
	b, err := protojson.MarshalOptions{Indent: "  "}.Marshal(msg)
	if err != nil {
		return err
	}
	converted := convertGLIDFields(b)
	_, err = p.w.Write(converted)
	if err == nil {
		_, err = fmt.Fprintln(p.w)
	}
	return err
}

// convertGLIDFields walks a JSON value and converts base64 strings that
// decode to exactly 16 bytes (GLID size) in fields named *_id or "id"
// to their base32hex representation.
func convertGLIDFields(data []byte) []byte {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return data
	}
	walkJSON(v, "")
	out, err := json.Marshal(v)
	if err != nil {
		return data
	}
	return out
}

func walkJSON(v any, key string) {
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			if s, ok := child.(string); ok && isIDField(k) {
				if converted, ok := tryConvertGLID(s); ok {
					val[k] = converted
				}
			} else {
				walkJSON(child, k)
			}
		}
	case []any:
		for _, item := range val {
			walkJSON(item, key)
		}
	}
}

func isIDField(name string) bool {
	lower := strings.ToLower(name)
	return lower == "id" || strings.HasSuffix(lower, "_id") ||
		strings.HasSuffix(lower, "Id") || // camelCase from protojson
		lower == "sender_id" || lower == "senderid" ||
		lower == "node_id" || lower == "nodeid"
}

// tryConvertGLID attempts to decode a base64 string as a GLID.
// Handles two cases:
//   - 16-byte raw GLID (standard proto bytes encoding)
//   - 26-byte ASCII base32hex string stored as proto bytes (legacy string-form IDs
//     like StorageID and NodeID that went through []byte(string) encoding)
//
// Returns the base32hex representation if successful.
func tryConvertGLID(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			return "", false
		}
	}
	switch len(decoded) {
	case glid.Size: // 16-byte raw GLID
		g := glid.FromBytes(decoded)
		if g.IsZero() {
			return "", false
		}
		return g.String(), true
	case 26: // base32hex string stored as []byte(string)
		str := string(decoded)
		if g, err := glid.Parse(str); err == nil && !g.IsZero() {
			return g.String(), true
		}
		return "", false
	default:
		return "", false
	}
}

// table writes rows using tabwriter. header is the first row.
func (p *printer) table(header []string, rows [][]string) {
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for i, h := range header {
		if i > 0 {
			_, _ = fmt.Fprint(tw, "\t")
		}
		_, _ = fmt.Fprint(tw, h)
	}
	_, _ = fmt.Fprintln(tw)
	for _, row := range rows {
		for i, col := range row {
			if i > 0 {
				_, _ = fmt.Fprint(tw, "\t")
			}
			_, _ = fmt.Fprint(tw, col)
		}
		_, _ = fmt.Fprintln(tw)
	}
	_ = tw.Flush()
}

// kv prints a key-value detail view.
func (p *printer) kv(pairs [][2]string) {
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for _, pair := range pairs {
		_, _ = fmt.Fprintf(tw, "%s:\t%s\n", pair[0], pair[1])
	}
	_ = tw.Flush()
}
