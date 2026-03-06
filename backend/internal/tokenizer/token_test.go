package tokenizer

import (
	"reflect"
	"testing"
)

func TestTokensBasic(t *testing.T) {
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
			name:  "pure numbers excluded",
			input: "error 404 not found",
			want:  []string{"error", "not", "found"},
		},
		{
			name:  "hyphen kept in token",
			input: "user-agent mozilla",
			want:  []string{"user-agent", "mozilla"},
		},
		{
			name:  "underscore kept in token",
			input: "user_id timeout",
			want:  []string{"user_id", "timeout"},
		},
		{
			name:  "colon splits",
			input: "user-agent: mozilla/5.0",
			want:  []string{"user-agent", "mozilla"},
		},
		{
			name:  "json-like",
			input: `{"level":"ERROR","msg":"failed"}`,
			want:  []string{"level", "error", "msg", "failed"},
		},
		{
			name:  "key=value with number suffix kept",
			input: "status=200 duration=15ms",
			want:  []string{"status", "duration", "15ms"},
		},
		{
			name:  "single char tokens skipped",
			input: "a b c de fg",
			want:  []string{"fg"}, // "de" is pure hex, excluded
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "only delimiters",
			input: "   ...   ",
			want:  nil,
		},
		{
			name:  "only single chars",
			input: "a b c",
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
			got := Tokens([]byte(tt.input))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tokens(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNumericExclusion(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "decimal excluded",
			input: "15 404 99999",
			want:  nil,
		},
		{
			name:  "hex prefix excluded",
			input: "0xdeadbeef 0x1234",
			want:  nil,
		},
		{
			name:  "octal prefix excluded",
			input: "0o755 0o644",
			want:  nil,
		},
		{
			name:  "binary prefix excluded",
			input: "0b101010 0b1111",
			want:  nil,
		},
		{
			name:  "pure hex excluded",
			input: "deadbeef a1b2c3d4 cafe",
			want:  nil,
		},
		{
			name:  "mixed with letters kept",
			input: "15ms timeout 404error",
			want:  []string{"15ms", "timeout", "404error"},
		},
		{
			name:  "hex-like with g kept",
			input: "deadbeeg cafeg",
			want:  []string{"deadbeeg", "cafeg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Tokens([]byte(tt.input))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tokens(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestUUIDExclusion(t *testing.T) {
	// UUIDs and hex-with-hyphens are excluded by the numeric rule.
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "uuid excluded",
			input: "id=019c0bc0-d19f-77db-bbdf-4c36766e13ca",
			want:  []string{"id"}, // hex with hyphens, excluded
		},
		{
			name:  "short uuid-like excluded",
			input: "id=0000-1111-2222",
			want:  []string{"id"}, // hex with hyphens, excluded
		},
		{
			name:  "pure hex excluded",
			input: "019c0bc0d19f77dbbbdf4c36766e13ca",
			want:  nil, // pure hex, excluded
		},
		{
			name:  "hex with hyphens excluded",
			input: "dead-beef-cafe",
			want:  nil, // hex with hyphens, excluded
		},
		{
			name:  "not hex kept",
			input: "hello-world",
			want:  []string{"hello-world"}, // has non-hex letters
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Tokens([]byte(tt.input))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tokens(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestHighBytesExcluded(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "utf8 splits token",
			input: "Größe überschritten",
			want:  []string{"gr", "berschritten"}, // ö and ü are high bytes (multi-byte UTF-8), act as delimiters
		},
		{
			name:  "pure ascii extracted",
			input: "error: 日本語 message",
			want:  []string{"error", "message"},
		},
		{
			name:  "emoji splits",
			input: "fire🔥error",
			want:  []string{"fire", "error"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Tokens([]byte(tt.input))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tokens(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestMaxTokenLength(t *testing.T) {
	// Token longer than 16 bytes should be truncated
	long := "abcdefghijklmnopqrstuvwxyz"
	got := Tokens([]byte(long))
	if len(got) != 1 {
		t.Fatalf("expected 1 token, got %d", len(got))
	}
	if len(got[0]) != 16 {
		t.Errorf("expected token length 16, got %d", len(got[0]))
	}
}

func TestIsTokenByte(t *testing.T) {
	// ASCII lowercase
	for b := byte('a'); b <= 'z'; b++ {
		if !isTokenByte(b) {
			t.Errorf("isTokenByte(%c) = false, want true", b)
		}
	}

	// ASCII uppercase
	for b := byte('A'); b <= 'Z'; b++ {
		if !isTokenByte(b) {
			t.Errorf("isTokenByte(%c) = false, want true", b)
		}
	}

	// Digits
	for b := byte('0'); b <= '9'; b++ {
		if !isTokenByte(b) {
			t.Errorf("isTokenByte(%c) = false, want true", b)
		}
	}

	// Underscore and hyphen
	if !isTokenByte('_') {
		t.Error("isTokenByte('_') = false, want true")
	}
	if !isTokenByte('-') {
		t.Error("isTokenByte('-') = false, want true")
	}

	// High bytes excluded
	for b := byte(0x80); b != 0; b++ {
		if isTokenByte(b) {
			t.Errorf("isTokenByte(0x%02X) = true, want false", b)
		}
		if b == 0xFF {
			break
		}
	}

	// Punctuation excluded
	delimiters := []byte{' ', '\t', '\n', '\r', '.', ',', ':', ';', '!', '?', '"', '\'', '(', ')', '[', ']', '{', '}', '/', '\\', '=', '+', '*', '&', '^', '%', '$', '#', '@', '`', '~', '<', '>', '|'}
	for _, b := range delimiters {
		if isTokenByte(b) {
			t.Errorf("isTokenByte(%c) = true, want false", b)
		}
	}
}

func TestLowercaseFunc(t *testing.T) {
	// ASCII uppercase -> lowercase
	for b := byte('A'); b <= 'Z'; b++ {
		got := Lowercase(b)
		want := b + ('a' - 'A')
		if got != want {
			t.Errorf("Lowercase(%c) = %c, want %c", b, got, want)
		}
	}

	// ASCII lowercase unchanged
	for b := byte('a'); b <= 'z'; b++ {
		if Lowercase(b) != b {
			t.Errorf("Lowercase(%c) = %c, want %c", b, Lowercase(b), b)
		}
	}

	// Digits unchanged
	for b := byte('0'); b <= '9'; b++ {
		if Lowercase(b) != b {
			t.Errorf("Lowercase(%c) = %c, want %c", b, Lowercase(b), b)
		}
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"15", true},
		{"404", true},
		{"0", true},
		{"0x1234", true},
		{"0xdeadbeef", true},
		{"0o755", true},
		{"0b101010", true},
		{"deadbeef", true},
		{"a1b2c3d4", true},
		{"cafe", true},
		{"dead-beef", true},      // hex with hyphens
		{"0000-1111-2222", true}, // hex with hyphens (UUID-like)
		{"15ms", false},
		{"timeout", false},
		{"hello-world", false}, // has non-hex letters
		{"deadbeeg", false},    // 'g' is not hex
		{"0xghij", false},      // invalid hex
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isNumeric([]byte(tt.input))
			if got != tt.want {
				t.Errorf("isNumeric(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsUUID(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"019c0bc0-d19f-77db-bbdf-4c36766e13ca", true},
		{"00000000-0000-0000-0000-000000000000", true},
		{"ffffffff-ffff-ffff-ffff-ffffffffffff", true},
		{"019c0bc0d19f77dbbbdf4c36766e13ca", false},      // no hyphens
		{"019c0bc0-d19f-77db-bbdf-4c36766e13c", false},   // too short
		{"019c0bc0-d19f-77db-bbdf-4c36766e13caa", false}, // too long
		{"019c0bc0-d19f-77db-bbdf-4c36766e13cg", false},  // 'g' not hex
		{"hello-world", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isUUID([]byte(tt.input))
			if got != tt.want {
				t.Errorf("isUUID(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIterTokens(t *testing.T) {
	input := "hello world timeout"
	var tokens []string
	buf := make([]byte, 0, 64)

	IterTokens([]byte(input), buf, DefaultMaxTokenLen, func(tok []byte) bool {
		tokens = append(tokens, string(tok))
		return true
	})

	want := []string{"hello", "world", "timeout"}
	if !reflect.DeepEqual(tokens, want) {
		t.Errorf("IterTokens(%q) = %v, want %v", input, tokens, want)
	}
}

func TestIterTokensEarlyStop(t *testing.T) {
	input := "one two three four"
	var tokens []string
	buf := make([]byte, 0, 64)

	IterTokens([]byte(input), buf, DefaultMaxTokenLen, func(tok []byte) bool {
		tokens = append(tokens, string(tok))
		return len(tokens) < 2 // stop after 2
	})

	want := []string{"one", "two"}
	if !reflect.DeepEqual(tokens, want) {
		t.Errorf("IterTokens early stop: got %v, want %v", tokens, want)
	}
}

// ─── Benchmarks ──────────────────────────────────────────────────────────────

// BenchmarkIterTokens_Short measures tokenization of a short structured log line.
func BenchmarkIterTokens_Short(b *testing.B) {
	data := []byte(`level=ERROR msg="connection refused" host=db-primary-01 port=5432 retry=3`)
	buf := make([]byte, 0, DefaultMaxTokenLen)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		IterTokens(data, buf, DefaultMaxTokenLen, func([]byte) bool { return true })
	}
}

// BenchmarkIterTokens_Long measures tokenization of a 1 KB JSON log line.
func BenchmarkIterTokens_Long(b *testing.B) {
	data := []byte(`{"timestamp":"2024-01-15T10:22:15.123Z","level":"ERROR","logger":"com.example.auth.UserService","thread":"http-nio-8080-exec-5","message":"Authentication failed for user admin@example.com from 192.168.1.100","exception":"org.springframework.security.authentication.BadCredentialsException: Bad credentials","context":{"request_id":"019c0bc0-d19f-77db-bbdf-4c36766e13ca","session_id":"abc-def-123","user_agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36","method":"POST","path":"/api/v1/auth/login","status":401,"duration_ms":42,"remote_ip":"192.168.1.100","server":"app-server-03","datacenter":"us-east-1","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7"},"tags":["security","auth","failure"]}`)
	buf := make([]byte, 0, DefaultMaxTokenLen)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		IterTokens(data, buf, DefaultMaxTokenLen, func([]byte) bool { return true })
	}
}

// BenchmarkTokens allocates and returns a []string (the non-streaming API).
func BenchmarkTokens(b *testing.B) {
	data := []byte(`level=ERROR msg="connection refused" host=db-primary-01 port=5432 retry=3`)
	for b.Loop() {
		_ = Tokens(data)
	}
}

// BenchmarkIsIndexable tests the per-token query-time check.
func BenchmarkIsIndexable(b *testing.B) {
	tokens := []string{"error", "timeout", "15ms", "0xdeadbeef", "019c0bc0-d19f-77db-bbdf-4c36766e13ca", "hello-world", "ab"}
	for b.Loop() {
		for _, tok := range tokens {
			_ = IsIndexable(tok)
		}
	}
}

// BenchmarkIsNumeric measures the numeric detection hot path.
func BenchmarkIsNumeric(b *testing.B) {
	cases := [][]byte{
		[]byte("404"),
		[]byte("0xdeadbeef"),
		[]byte("deadbeef"),
		[]byte("dead-beef-cafe"),
		[]byte("hello-world"),
		[]byte("15ms"),
	}
	for b.Loop() {
		for _, c := range cases {
			_ = isNumeric(c)
		}
	}
}

// BenchmarkIsUUID measures UUID detection (fixed 36-byte format check).
func BenchmarkIsUUID(b *testing.B) {
	uuid := []byte("019c0bc0-d19f-77db-bbdf-4c36766e13ca")
	nonUUID := []byte("hello-world-foo-bar-baz")
	for b.Loop() {
		_ = isUUID(uuid)
		_ = isUUID(nonUUID)
	}
}
