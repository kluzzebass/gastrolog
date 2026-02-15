package chatterbox

import (
	"math/rand/v2"
	"time"
)

// WeirdFormat generates random/malformed data to stress tokenization.
type WeirdFormat struct {
	pools *AttributePools
}

// NewWeirdFormat creates a weird format generator.
func NewWeirdFormat(pools *AttributePools) *WeirdFormat {
	return &WeirdFormat{pools: pools}
}

func (f *WeirdFormat) Generate(rng *rand.Rand) ([]byte, map[string]string, time.Time) {
	var data []byte

	switch rng.IntN(8) {
	case 0:
		// Random bytes
		data = make([]byte, 50+rng.IntN(200))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
	case 1:
		// Control characters mixed with text
		text := "normal log message with\x00null\x07bell\x1bescape\x0bnewlines"
		data = []byte(text)
	case 2:
		// High-bit / UTF-8 edge cases
		samples := []string{
			"日本語ログメッセージ with mixed 文字",
			"Ошибка подключения к серверу",
			"🔥 Error: something went wrong 💥",
			"café résumé naïve",
			"\xc0\xc1\xfe\xff invalid UTF-8 sequences",
		}
		data = []byte(pick(rng, samples))
	case 3:
		// Very long tokens
		data = make([]byte, 1000+rng.IntN(1000))
		for i := range data {
			data[i] = 'a' + byte(rng.IntN(26))
		}
	case 4:
		// Repeated patterns
		pattern := pick(rng, []string{"AAAA", "abab", "123123", "....", "====", "----"})
		count := 10 + rng.IntN(50)
		for range count {
			data = append(data, pattern...)
		}
	case 5:
		// Empty or whitespace
		spaces := []string{"", " ", "\t", "\n", "   \t\n   ", "\r\n"}
		data = []byte(pick(rng, spaces))
	case 6:
		// JSON-like but malformed
		malformed := []string{
			`{"key": "value"`,
			`{key: value}`,
			`{"nested": {"deep": {"broken":`,
			`["array", "without", "end"`,
			`{"escape": "bad \q escape"}`,
		}
		data = []byte(pick(rng, malformed))
	default:
		// Mixed binary and text
		data = []byte("START")
		for range 20 {
			if rng.IntN(2) == 0 {
				data = append(data, byte(rng.IntN(32))) // control char
			} else {
				data = append(data, byte('A'+rng.IntN(26)))
			}
		}
		data = append(data, []byte("END")...)
	}

	attrs := map[string]string{
		"service": "unknown",
		"host":    pick(rng, f.pools.Hosts),
	}

	return data, attrs, time.Time{}
}
