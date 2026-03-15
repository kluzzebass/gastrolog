package level

import "testing"

func FuzzExtractLevel(f *testing.F) {
	// Syslog priority.
	f.Add([]byte("<34>some syslog message"))
	f.Add([]byte("<0>emergency"))
	f.Add([]byte("<191>debug message"))

	// KV format.
	f.Add([]byte("ts=2024-01-01 level=ERROR msg=oops"))
	f.Add([]byte("severity=warn something happened"))
	f.Add([]byte(`level="info" key=value`))

	// JSON format.
	f.Add([]byte(`{"level":"error","msg":"fail"}`))
	f.Add([]byte(`{"severity":"warning","ts":123}`))
	f.Add([]byte(`{"level": "debug", "message": "test"}`))

	// Edge cases.
	f.Add([]byte(""))
	f.Add([]byte("no level here"))
	f.Add([]byte("<>invalid priority"))
	f.Add([]byte("level="))
	f.Add([]byte(`level="`))
	f.Add([]byte("level=UNKNOWN_VALUE"))
	f.Add([]byte("alevel=error"))   // mid-word, should not match
	f.Add([]byte("xlevelx=error"))  // surrounded by word chars

	f.Fuzz(func(t *testing.T, raw []byte) {
		result := extractLevel(raw)
		if result != "" {
			switch result {
			case "error", "warn", "info", "debug", "trace":
				// valid
			default:
				t.Errorf("unexpected level value: %q", result)
			}
		}
	})
}

func FuzzNormalize(f *testing.F) {
	f.Add("error")
	f.Add("ERROR")
	f.Add("warn")
	f.Add("WARNING")
	f.Add("info")
	f.Add("debug")
	f.Add("trace")
	f.Add("fatal")
	f.Add("CRITICAL")
	f.Add("")
	f.Add("garbage")
	f.Add("eRrOr")

	f.Fuzz(func(t *testing.T, val string) {
		result := normalize(val)
		if result != "" {
			switch result {
			case "error", "warn", "info", "debug", "trace":
				// valid
			default:
				t.Errorf("unexpected normalized value: %q", result)
			}
		}
	})
}
