package tokenizer

import "testing"

func TestIsLetter(t *testing.T) {
	tests := []struct {
		c    byte
		want bool
	}{
		{'a', true},
		{'z', true},
		{'A', true},
		{'Z', true},
		{'m', true},
		{'M', true},
		{'0', false},
		{'9', false},
		{' ', false},
		{'_', false},
		{'-', false},
		{'.', false},
		{'@', false},
	}

	for _, tt := range tests {
		got := IsLetter(tt.c)
		if got != tt.want {
			t.Errorf("IsLetter(%q) = %v, want %v", tt.c, got, tt.want)
		}
	}
}

func TestIsDigit(t *testing.T) {
	tests := []struct {
		c    byte
		want bool
	}{
		{'0', true},
		{'9', true},
		{'5', true},
		{'a', false},
		{'A', false},
		{' ', false},
		{'.', false},
	}

	for _, tt := range tests {
		got := IsDigit(tt.c)
		if got != tt.want {
			t.Errorf("IsDigit(%q) = %v, want %v", tt.c, got, tt.want)
		}
	}
}

func TestIsHexDigit(t *testing.T) {
	tests := []struct {
		c    byte
		want bool
	}{
		{'0', true},
		{'9', true},
		{'a', true},
		{'f', true},
		{'A', false}, // uppercase hex not supported
		{'F', false},
		{'g', false},
		{'z', false},
		{' ', false},
	}

	for _, tt := range tests {
		got := IsHexDigit(tt.c)
		if got != tt.want {
			t.Errorf("IsHexDigit(%q) = %v, want %v", tt.c, got, tt.want)
		}
	}
}

func TestIsWhitespace(t *testing.T) {
	tests := []struct {
		c    byte
		want bool
	}{
		{' ', true},
		{'\t', true},
		{'\n', true},
		{'\r', true},
		{'a', false},
		{'0', false},
		{'_', false},
	}

	for _, tt := range tests {
		got := IsWhitespace(tt.c)
		if got != tt.want {
			t.Errorf("IsWhitespace(%q) = %v, want %v", tt.c, got, tt.want)
		}
	}
}

func TestLowercase(t *testing.T) {
	tests := []struct {
		c    byte
		want byte
	}{
		{'A', 'a'},
		{'Z', 'z'},
		{'M', 'm'},
		{'a', 'a'},
		{'z', 'z'},
		{'0', '0'},
		{' ', ' '},
		{'_', '_'},
	}

	for _, tt := range tests {
		got := Lowercase(tt.c)
		if got != tt.want {
			t.Errorf("Lowercase(%q) = %q, want %q", tt.c, got, tt.want)
		}
	}
}

func TestToLowerASCII(t *testing.T) {
	tests := []struct {
		input []byte
		want  string
	}{
		{[]byte("HELLO"), "hello"},
		{[]byte("hello"), "hello"},
		{[]byte("HeLLo"), "hello"},
		{[]byte("Hello World"), "hello world"},
		{[]byte("ABC123"), "abc123"},
		{[]byte(""), ""},
		{[]byte("foo_BAR"), "foo_bar"},
	}

	for _, tt := range tests {
		got := ToLowerASCII(tt.input)
		if got != tt.want {
			t.Errorf("ToLowerASCII(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
