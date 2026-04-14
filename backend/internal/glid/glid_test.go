package glid

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestNewIsNotZero(t *testing.T) {
	g := New()
	if g.IsZero() {
		t.Fatal("New() returned zero GLID")
	}
}

func TestNewIsMonotonic(t *testing.T) {
	a := New()
	b := New()
	if a.Compare(b) >= 0 {
		t.Fatalf("expected a < b, got a=%s b=%s", a, b)
	}
}

func TestStringRoundTrip(t *testing.T) {
	g := New()
	s := g.String()
	if len(s) != 26 {
		t.Fatalf("String() length = %d, want 26", len(s))
	}
	parsed, err := Parse(s)
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	if parsed != g {
		t.Fatalf("round-trip failed: %s != %s", parsed, g)
	}
}

func TestParseEmpty(t *testing.T) {
	g, err := Parse("")
	if err != nil {
		t.Fatal(err)
	}
	if !g.IsZero() {
		t.Fatal("Parse empty should return Nil")
	}
}

func TestParseInvalidLength(t *testing.T) {
	_, err := Parse("tooshort")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseInvalidChars(t *testing.T) {
	_, err := Parse("!!!!!!!!!!!!!!!!!!!!!!!!!!")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFromUUIDRoundTrip(t *testing.T) {
	u := uuid.Must(uuid.NewV7())
	g := FromUUID(u)
	if g.UUID() != u {
		t.Fatal("UUID round-trip failed")
	}
}

func TestParseUUIDMigration(t *testing.T) {
	u := uuid.Must(uuid.NewV7())
	g, err := ParseUUID(u.String())
	if err != nil {
		t.Fatal(err)
	}
	if g.UUID() != u {
		t.Fatal("ParseUUID round-trip failed")
	}
}

func TestParseUUIDEmpty(t *testing.T) {
	g, err := ParseUUID("")
	if err != nil {
		t.Fatal(err)
	}
	if !g.IsZero() {
		t.Fatal("ParseUUID empty should return Nil")
	}
}

func TestBytesRoundTrip(t *testing.T) {
	g := New()
	b := g.Bytes()
	if len(b) != Size {
		t.Fatalf("Bytes() length = %d, want %d", len(b), Size)
	}
	restored := FromBytes(b)
	if restored != g {
		t.Fatal("Bytes round-trip failed")
	}
}

func TestFromBytesShort(t *testing.T) {
	g := FromBytes([]byte{1, 2, 3})
	if !g.IsZero() {
		t.Fatal("FromBytes with short input should return Nil")
	}
}

func TestFromBytesNil(t *testing.T) {
	g := FromBytes(nil)
	if !g.IsZero() {
		t.Fatal("FromBytes nil should return Nil")
	}
}

func TestTime(t *testing.T) {
	g := New()
	if g.Time() == 0 {
		t.Fatal("expected non-zero time from UUIDv7")
	}
}

func TestNilString(t *testing.T) {
	if Nil.String() != "" {
		t.Fatalf("Nil.String() = %q, want empty", Nil.String())
	}
}

func TestJSONRoundTrip(t *testing.T) {
	g := New()
	data, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	var parsed GLID
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed != g {
		t.Fatal("JSON round-trip failed")
	}
}

func TestJSONNil(t *testing.T) {
	data, _ := json.Marshal(Nil)
	if string(data) != `""` {
		t.Fatalf("Nil JSON = %s, want empty string", data)
	}
	var g GLID
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatal(err)
	}
	if !g.IsZero() {
		t.Fatal("JSON nil round-trip should be zero")
	}
}

func TestTextRoundTrip(t *testing.T) {
	g := New()
	text, err := g.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var parsed GLID
	if err := parsed.UnmarshalText(text); err != nil {
		t.Fatal(err)
	}
	if parsed != g {
		t.Fatal("Text round-trip failed")
	}
}

func TestCompare(t *testing.T) {
	a := New()
	b := New()
	if a.Compare(a) != 0 {
		t.Fatal("Compare(self) != 0")
	}
	if a.Compare(b) != -1 {
		t.Fatal("earlier GLID should be less")
	}
	if b.Compare(a) != 1 {
		t.Fatal("later GLID should be greater")
	}
}

func TestOptional(t *testing.T) {
	ptr, err := ParseOptional("")
	if err != nil || ptr != nil {
		t.Fatal("ParseOptional empty should return nil")
	}

	g := New()
	ptr, err = ParseOptional(g.String())
	if err != nil || ptr == nil || *ptr != g {
		t.Fatal("ParseOptional round-trip failed")
	}

	if OptionalString(nil) != "" {
		t.Fatal("OptionalString nil should be empty")
	}
	if OptionalString(&g) != g.String() {
		t.Fatal("OptionalString mismatch")
	}
}

func TestMustParsePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustParse should panic on invalid input")
		}
	}()
	MustParse("invalid")
}

func TestStringIsSortable(t *testing.T) {
	ids := make([]GLID, 100)
	for i := range ids {
		ids[i] = New()
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1].String() >= ids[i].String() {
			t.Fatalf("not sortable: %s >= %s", ids[i-1], ids[i])
		}
	}
}
