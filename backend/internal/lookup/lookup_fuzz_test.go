package lookup

import (
	"context"
	"testing"
)

// FuzzUserAgentParse verifies that the UserAgent parser never panics on
// arbitrary strings and always returns valid structured fields.
func FuzzUserAgentParse(f *testing.F) {
	f.Add("")
	f.Add("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	f.Add("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1")
	f.Add("curl/8.4.0")
	f.Add("Googlebot/2.1 (+http://www.google.com/bot.html)")
	f.Add("Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)")
	f.Add("python-requests/2.31.0")
	f.Add("Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36")
	f.Add("PostmanRuntime/7.36.0")
	f.Add("\x00\xff\xfe\xfd")
	f.Add("a]b[c{d}e(f)g")
	f.Add("()")
	f.Add("/")

	ua := NewUserAgent()
	ctx := context.Background()

	f.Fuzz(func(t *testing.T, raw string) {
		result := ua.LookupValues(ctx, map[string]string{"value": raw})

		if result == nil {
			return
		}

		// Validate only known suffixes are returned.
		validSuffixes := map[string]bool{
			"browser": true, "browser_version": true,
			"os": true, "os_version": true,
			"device": true, "device_type": true,
		}
		for k := range result {
			if !validSuffixes[k] {
				t.Fatalf("unexpected output key: %q", k)
			}
		}

		// Validate device_type if present.
		if dt, ok := result["device_type"]; ok {
			switch dt {
			case "bot", "tablet", "mobile", "desktop":
				// valid
			default:
				t.Fatalf("unexpected device_type: %q", dt)
			}
		}
	})
}

// FuzzRDNSLookupValues verifies that RDNS.LookupValues never panics on
// arbitrary input strings. Note: actual DNS lookups will fail/timeout for
// garbage input, but the function must handle that gracefully.
func FuzzRDNSLookupValues(f *testing.F) {
	f.Add("")
	f.Add("127.0.0.1")
	f.Add("::1")
	f.Add("8.8.8.8")
	f.Add("not-an-ip")
	f.Add("999.999.999.999")
	f.Add("fe80::1%eth0")
	f.Add("\x00")
	f.Add("192.168.1.1")

	rdns := NewRDNS()
	// Use a very short timeout to avoid slow DNS lookups during fuzzing.
	rdns.timeout = 1 // 1 nanosecond — will always timeout

	f.Fuzz(func(t *testing.T, value string) {
		ctx := context.Background()
		result := rdns.LookupValues(ctx, map[string]string{"value": value})

		if result == nil {
			return
		}

		// The only output key should be "hostname".
		if len(result) != 1 {
			t.Fatalf("expected 1 result key, got %d", len(result))
		}
		if _, ok := result["hostname"]; !ok {
			t.Fatal("expected 'hostname' key in result")
		}
	})
}
