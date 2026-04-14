// Package glid provides a unified identifier type for all GastroLog entities.
// GLID wraps uuid.UUID (v7) and adds base32hex string encoding (26 chars,
// lexicographically sortable by creation time). On the proto wire format,
// GLIDs are raw bytes (16 bytes, no encoding overhead).
package glid

import (
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Size is the byte length of a GLID (same as UUID: 16 bytes).
const Size = 16

// encoding is base32hex (RFC 4648) lowercase without padding.
// Alphabet 0-9a-v preserves lexicographic sort order.
var encoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// GLID uniquely identifies an entity. It wraps uuid.UUID (v7) with
// base32hex string encoding. Sortable by creation time.
type GLID uuid.UUID

// Nil is the zero GLID.
var Nil GLID

// New creates a new GLID from a fresh UUIDv7.
func New() GLID {
	return GLID(uuid.Must(uuid.NewV7()))
}

// FromUUID converts a uuid.UUID to a GLID.
func FromUUID(u uuid.UUID) GLID {
	return GLID(u)
}

// UUID returns the underlying uuid.UUID.
func (g GLID) UUID() uuid.UUID {
	return uuid.UUID(g)
}

// Parse parses a 26-character base32hex string into a GLID.
func Parse(s string) (GLID, error) {
	if len(s) == 0 {
		return Nil, nil
	}
	if len(s) != 26 {
		return Nil, fmt.Errorf("invalid GLID length: %d (want 26)", len(s))
	}
	decoded, err := encoding.DecodeString(strings.ToUpper(s))
	if err != nil {
		return Nil, fmt.Errorf("invalid GLID: %w", err)
	}
	var g GLID
	copy(g[:], decoded)
	return g, nil
}

// ParseUUID parses a UUID string or base32hex string into a GLID.
// Accepts both formats for backward compatibility.
func ParseUUID(s string) (GLID, error) {
	return ParseAny(s)
}

// FromBytes creates a GLID from a 16-byte slice (proto wire format).
// Returns Nil for nil/empty/short input.
func FromBytes(b []byte) GLID {
	if len(b) < Size {
		return Nil
	}
	var g GLID
	copy(g[:], b)
	return g
}

// Bytes returns the raw 16-byte representation (proto wire format).
func (g GLID) Bytes() []byte {
	return g[:]
}

// String returns the 26-character lowercase base32hex representation.
func (g GLID) String() string {
	if g == Nil {
		return ""
	}
	return strings.ToLower(encoding.EncodeToString(g[:]))
}

// IsZero reports whether the GLID is the zero value.
func (g GLID) IsZero() bool {
	return g == Nil
}

// Time delegates to uuid.UUID.Time(), which returns the UUIDv7 timestamp
// with the full precision that the uuid package provides.
func (g GLID) Time() uuid.Time {
	return uuid.UUID(g).Time()
}

// MarshalJSON encodes the GLID as a JSON string in base32hex format.
func (g GLID) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.String())
}

// UnmarshalJSON decodes a JSON string in base32hex format.
func (g *GLID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := Parse(s)
	if err != nil {
		return err
	}
	*g = parsed
	return nil
}

// MarshalText encodes the GLID as base32hex text.
func (g GLID) MarshalText() ([]byte, error) {
	return []byte(g.String()), nil
}

// UnmarshalText decodes base32hex text.
func (g *GLID) UnmarshalText(data []byte) error {
	parsed, err := Parse(string(data))
	if err != nil {
		return err
	}
	*g = parsed
	return nil
}

// Compare returns -1, 0, or 1 for ordering. UUIDv7 GLIDs are naturally
// ordered by creation time.
func (g GLID) Compare(other GLID) int {
	for i := range Size {
		if g[i] < other[i] {
			return -1
		}
		if g[i] > other[i] {
			return 1
		}
	}
	return 0
}

// ParseOptional parses a base32hex string, returning a pointer.
// Returns nil for empty strings.
func ParseOptional(s string) (*GLID, error) {
	if s == "" {
		return nil, nil
	}
	g, err := Parse(s)
	if err != nil {
		return nil, err
	}
	return &g, nil
}

// OptionalString returns the base32hex string or empty for nil.
func OptionalString(g *GLID) string {
	if g == nil {
		return ""
	}
	return g.String()
}

// MustParse parses a base32hex or UUID string or panics. For tests and constants.
func MustParse(s string) GLID {
	g, err := ParseAny(s)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseAny tries base32hex (26 chars) first, then UUID format (36 chars).
// Accepts both formats for backward compatibility during migration.
func ParseAny(s string) (GLID, error) {
	if s == "" {
		return Nil, nil
	}
	if len(s) == 26 {
		return Parse(s)
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return Nil, err
	}
	return GLID(u), nil
}

// --- Proto boundary helpers ---
// These simplify the conversion between glid.GLID and proto []byte fields.

// ToProto converts a GLID to the proto wire format ([]byte).
// Alias for Bytes() — exists for readability at proto boundaries.
func (g GLID) ToProto() []byte { return g[:] }

// OptionalToProto converts a *GLID to proto bytes. Returns nil for nil pointer.
func OptionalToProto(g *GLID) []byte {
	if g == nil {
		return nil
	}
	return g[:]
}

// SliceToProto converts []GLID to [][]byte for repeated proto fields.
func SliceToProto(ids []GLID) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id[:]
	}
	return out
}

// SliceFromProto converts [][]byte from repeated proto fields to []GLID.
func SliceFromProto(bs [][]byte) []GLID {
	if len(bs) == 0 {
		return nil
	}
	out := make([]GLID, len(bs))
	for i, b := range bs {
		out[i] = FromBytes(b)
	}
	return out
}

// OptionalFromProto converts proto bytes to *GLID. Returns nil for nil/empty input.
func OptionalFromProto(b []byte) *GLID {
	if len(b) < Size {
		return nil
	}
	g := FromBytes(b)
	return &g
}

var (
	ErrInvalidLength = errors.New("invalid GLID length")
	ErrInvalidFormat = errors.New("invalid GLID format")
)
