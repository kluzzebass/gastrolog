package cli

import "testing"

func TestParseUnixURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		addr     string
		wantPath string
		wantOK   bool
	}{
		{"absolute path", "unix:///var/run/gastrolog.sock", "/var/run/gastrolog.sock", true},
		{"relative path", "unix://data/node1/gastrolog.sock", "data/node1/gastrolog.sock", true},
		{"just scheme", "unix://", "", false},
		{"http url", "http://localhost:4564", "", false},
		{"https url", "https://example.com", "", false},
		{"bare path", "data/node1/gastrolog.sock", "", false},
		{"empty", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, ok := parseUnixURL(tt.addr)
			if ok != tt.wantOK {
				t.Errorf("parseUnixURL(%q) ok = %v, want %v", tt.addr, ok, tt.wantOK)
			}
			if path != tt.wantPath {
				t.Errorf("parseUnixURL(%q) path = %q, want %q", tt.addr, path, tt.wantPath)
			}
		})
	}
}
