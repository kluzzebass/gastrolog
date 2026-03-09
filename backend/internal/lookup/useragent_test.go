package lookup

import (
	"context"
	"testing"
)

func TestUserAgentChrome(t *testing.T) {
	u := NewUserAgent()

	result := u.LookupValues(context.Background(), map[string]string{
		"value": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	})
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result["browser"] != "Chrome" {
		t.Errorf("browser = %q, want Chrome", result["browser"])
	}
	if result["os"] != "Windows" {
		t.Errorf("os = %q, want Windows", result["os"])
	}
	if result["device_type"] != "desktop" {
		t.Errorf("device_type = %q, want desktop", result["device_type"])
	}
}

func TestUserAgentMobile(t *testing.T) {
	u := NewUserAgent()

	result := u.LookupValues(context.Background(), map[string]string{
		"value": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
	})
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result["browser"] != "Safari" {
		t.Errorf("browser = %q, want Safari", result["browser"])
	}
	if result["os"] != "iOS" {
		t.Errorf("os = %q, want iOS", result["os"])
	}
	if result["device_type"] != "mobile" {
		t.Errorf("device_type = %q, want mobile", result["device_type"])
	}
	if result["device"] != "iPhone" {
		t.Errorf("device = %q, want iPhone", result["device"])
	}
}

func TestUserAgentBot(t *testing.T) {
	u := NewUserAgent()

	result := u.LookupValues(context.Background(), map[string]string{
		"value": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
	})
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result["browser"] != "Googlebot" {
		t.Errorf("browser = %q, want Googlebot", result["browser"])
	}
	if result["device_type"] != "bot" {
		t.Errorf("device_type = %q, want bot", result["device_type"])
	}
}

func TestUserAgentEmpty(t *testing.T) {
	u := NewUserAgent()

	if result := u.LookupValues(context.Background(), map[string]string{"value": ""}); result != nil {
		t.Errorf("expected nil for empty UA, got %v", result)
	}
	if result := u.LookupValues(context.Background(), map[string]string{}); result != nil {
		t.Errorf("expected nil for missing value, got %v", result)
	}
}

func TestUserAgentSuffixes(t *testing.T) {
	u := NewUserAgent()
	want := []string{"browser", "browser_version", "os", "os_version", "device", "device_type"}
	got := u.Suffixes()
	if len(got) != len(want) {
		t.Fatalf("suffixes len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("suffixes[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
