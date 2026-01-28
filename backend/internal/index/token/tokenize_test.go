package token

import (
	"reflect"
	"testing"
)

func TestSimpleBasic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "simple words",
			input: "hello world",
			want:  []string{"hello", "world"},
		},
		{
			name:  "uppercase converted",
			input: "Hello World",
			want:  []string{"hello", "world"},
		},
		{
			name:  "mixed case",
			input: "HeLLo WoRLD",
			want:  []string{"hello", "world"},
		},
		{
			name:  "with numbers",
			input: "error 404 not found",
			want:  []string{"error", "404", "not", "found"},
		},
		{
			name:  "punctuation splits",
			input: "user-agent: mozilla/5.0",
			want:  []string{"user", "agent", "mozilla"},
		},
		{
			name:  "json-like",
			input: `{"level":"ERROR","msg":"failed"}`,
			want:  []string{"level", "error", "msg", "failed"},
		},
		{
			name:  "key=value",
			input: "status=200 duration=15ms",
			want:  []string{"status", "200", "duration", "15ms"},
		},
		{
			name:  "single char tokens skipped",
			input: "a b c de fg",
			want:  []string{"de", "fg"},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "only delimiters",
			input: "   ---   ",
			want:  nil,
		},
		{
			name:  "only single chars",
			input: "a b c 1 2 3",
			want:  nil,
		},
		{
			name:  "trailing token",
			input: "hello world",
			want:  []string{"hello", "world"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Simple([]byte(tt.input))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Simple(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSimpleHighBytes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "german umlauts",
			input: "Größe überschritten",
			want:  []string{"größe", "überschritten"},
		},
		{
			name:  "mixed ascii and utf8",
			input: "error: Verbindungsfehler aufgetreten",
			want:  []string{"error", "verbindungsfehler", "aufgetreten"},
		},
		{
			name:  "non-breaking space splits",
			input: "hello\xA0world", // 0xA0 non-breaking space (raw byte)
			want:  []string{"hello", "world"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Simple([]byte(tt.input))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Simple(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsWordByte(t *testing.T) {
	// ASCII lowercase
	for b := byte('a'); b <= 'z'; b++ {
		if !isWordByte(b) {
			t.Errorf("isWordByte(%c) = false, want true", b)
		}
	}

	// ASCII uppercase
	for b := byte('A'); b <= 'Z'; b++ {
		if !isWordByte(b) {
			t.Errorf("isWordByte(%c) = false, want true", b)
		}
	}

	// Digits
	for b := byte('0'); b <= '9'; b++ {
		if !isWordByte(b) {
			t.Errorf("isWordByte(%c) = false, want true", b)
		}
	}

	// High bytes (except 0xA0)
	for b := byte(0x80); b <= 0x9F; b++ {
		if !isWordByte(b) {
			t.Errorf("isWordByte(0x%02X) = false, want true", b)
		}
	}
	if isWordByte(0xA0) {
		t.Error("isWordByte(0xA0) = true, want false (non-breaking space)")
	}
	for b := byte(0xA1); b != 0; b++ { // 0xA1 to 0xFF, wraps to 0
		if !isWordByte(b) {
			t.Errorf("isWordByte(0x%02X) = false, want true", b)
		}
		if b == 0xFF {
			break
		}
	}

	// Delimiters
	delimiters := []byte{' ', '\t', '\n', '\r', '.', ',', ':', ';', '!', '?', '"', '\'', '(', ')', '[', ']', '{', '}', '/', '\\', '=', '+', '-', '*', '&', '^', '%', '$', '#', '@', '`', '~', '<', '>', '|'}
	for _, b := range delimiters {
		if isWordByte(b) {
			t.Errorf("isWordByte(%c) = true, want false", b)
		}
	}
}

func TestLowercase(t *testing.T) {
	// ASCII uppercase -> lowercase
	for b := byte('A'); b <= 'Z'; b++ {
		got := lowercase(b)
		want := b + ('a' - 'A')
		if got != want {
			t.Errorf("lowercase(%c) = %c, want %c", b, got, want)
		}
	}

	// ASCII lowercase unchanged
	for b := byte('a'); b <= 'z'; b++ {
		if lowercase(b) != b {
			t.Errorf("lowercase(%c) = %c, want %c", b, lowercase(b), b)
		}
	}

	// Digits unchanged
	for b := byte('0'); b <= '9'; b++ {
		if lowercase(b) != b {
			t.Errorf("lowercase(%c) = %c, want %c", b, lowercase(b), b)
		}
	}

	// High bytes unchanged (no Unicode lowercasing)
	if lowercase(0xC4) != 0xC4 { // Ä in Latin-1
		t.Errorf("lowercase(0xC4) changed, should be unchanged")
	}
}
