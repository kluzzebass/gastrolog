package querylang

import "testing"

func TestCompileAttrFilter(t *testing.T) {
	tests := []struct {
		name      string
		expr      string
		wantNil   bool // expect nil DNF (match-all)
		wantError bool
	}{
		{name: "empty is match-all", expr: "", wantNil: true},
		{name: "whitespace is match-all", expr: "   ", wantNil: true},
		{name: "simple kv", expr: "env=prod"},
		{name: "key exists", expr: "env=*"},
		{name: "value exists", expr: "*=error"},
		{name: "and expression", expr: "env=prod AND level=error"},
		{name: "or expression", expr: "env=prod OR env=staging"},
		{name: "not expression", expr: "NOT env=prod"},
		{name: "glob in value", expr: "image=nginx*"},
		{name: "glob in key", expr: "label.*=prod"},
		{name: "reject token predicate", expr: "error", wantError: true},
		{name: "reject token in and", expr: "error AND env=prod", wantError: true},
		{name: "reject regex predicate", expr: "/error.*/", wantError: true},
		{name: "reject glob predicate", expr: "error*", wantError: true},
		{name: "invalid syntax", expr: "env=prod AND", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dnf, err := CompileAttrFilter(tt.expr)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil {
				if dnf != nil {
					t.Fatalf("expected nil DNF, got %v", dnf)
				}
				return
			}
			if dnf == nil {
				t.Fatal("expected non-nil DNF")
			}
		})
	}
}

func TestMatchAttrsNilDNF(t *testing.T) {
	// Nil DNF matches everything.
	if !MatchAttrs(nil, map[string]string{"any": "thing"}) {
		t.Error("nil DNF should match all")
	}
	if !MatchAttrs(nil, nil) {
		t.Error("nil DNF should match nil attrs")
	}
}

func TestMatchAttrsKV(t *testing.T) {
	dnf := mustCompile(t, "env=prod")

	if !MatchAttrs(dnf, map[string]string{"env": "prod"}) {
		t.Error("should match exact kv")
	}
	if !MatchAttrs(dnf, map[string]string{"env": "PROD"}) {
		t.Error("should match case-insensitive value")
	}
	if !MatchAttrs(dnf, map[string]string{"ENV": "prod"}) {
		t.Error("should match case-insensitive key")
	}
	if MatchAttrs(dnf, map[string]string{"env": "staging"}) {
		t.Error("should not match different value")
	}
	if MatchAttrs(dnf, map[string]string{"other": "prod"}) {
		t.Error("should not match different key")
	}
}

func TestMatchAttrsKeyExists(t *testing.T) {
	dnf := mustCompile(t, "env=*")

	if !MatchAttrs(dnf, map[string]string{"env": "anything"}) {
		t.Error("should match when key exists")
	}
	if !MatchAttrs(dnf, map[string]string{"ENV": "anything"}) {
		t.Error("should match case-insensitive key")
	}
	if MatchAttrs(dnf, map[string]string{"other": "value"}) {
		t.Error("should not match when key missing")
	}
}

func TestMatchAttrsValueExists(t *testing.T) {
	dnf := mustCompile(t, "*=error")

	if !MatchAttrs(dnf, map[string]string{"level": "error"}) {
		t.Error("should match when value exists")
	}
	if !MatchAttrs(dnf, map[string]string{"level": "ERROR"}) {
		t.Error("should match case-insensitive value")
	}
	if !MatchAttrs(dnf, map[string]string{"status": "error", "env": "prod"}) {
		t.Error("should match value in any key")
	}
	if MatchAttrs(dnf, map[string]string{"level": "info"}) {
		t.Error("should not match different value")
	}
}

func TestMatchAttrsAND(t *testing.T) {
	dnf := mustCompile(t, "env=prod AND level=error")

	if !MatchAttrs(dnf, map[string]string{"env": "prod", "level": "error"}) {
		t.Error("should match when both present")
	}
	if MatchAttrs(dnf, map[string]string{"env": "prod", "level": "info"}) {
		t.Error("should not match when second fails")
	}
	if MatchAttrs(dnf, map[string]string{"env": "staging", "level": "error"}) {
		t.Error("should not match when first fails")
	}
}

func TestMatchAttrsOR(t *testing.T) {
	dnf := mustCompile(t, "env=prod OR env=staging")

	if !MatchAttrs(dnf, map[string]string{"env": "prod"}) {
		t.Error("should match first branch")
	}
	if !MatchAttrs(dnf, map[string]string{"env": "staging"}) {
		t.Error("should match second branch")
	}
	if MatchAttrs(dnf, map[string]string{"env": "dev"}) {
		t.Error("should not match neither branch")
	}
}

func TestMatchAttrsNOT(t *testing.T) {
	dnf := mustCompile(t, "NOT env=prod")

	if MatchAttrs(dnf, map[string]string{"env": "prod"}) {
		t.Error("should not match negated")
	}
	if !MatchAttrs(dnf, map[string]string{"env": "staging"}) {
		t.Error("should match non-negated")
	}
	if !MatchAttrs(dnf, map[string]string{}) {
		t.Error("should match empty attrs (key doesn't exist)")
	}
}

func TestMatchAttrsGlobValuePattern(t *testing.T) {
	dnf := mustCompile(t, "image=nginx*")

	if !MatchAttrs(dnf, map[string]string{"image": "nginx:latest"}) {
		t.Error("should match glob value")
	}
	if !MatchAttrs(dnf, map[string]string{"image": "NGINX:1.25"}) {
		t.Error("should match glob value case-insensitive")
	}
	if MatchAttrs(dnf, map[string]string{"image": "redis:7"}) {
		t.Error("should not match non-matching value")
	}
}

func TestMatchAttrsGlobKeyPattern(t *testing.T) {
	dnf := mustCompile(t, "label.*=prod")

	if !MatchAttrs(dnf, map[string]string{"label.env": "prod"}) {
		t.Error("should match glob key")
	}
	if !MatchAttrs(dnf, map[string]string{"label.tier": "prod"}) {
		t.Error("should match different glob key with same value")
	}
	if MatchAttrs(dnf, map[string]string{"label.env": "staging"}) {
		t.Error("should not match wrong value")
	}
	if MatchAttrs(dnf, map[string]string{"env": "prod"}) {
		t.Error("should not match key without label prefix")
	}
}

func TestMatchAttrsGlobKeyExists(t *testing.T) {
	dnf := mustCompile(t, "label.*=*")

	if !MatchAttrs(dnf, map[string]string{"label.env": "anything"}) {
		t.Error("should match key with glob pattern")
	}
	if MatchAttrs(dnf, map[string]string{"env": "anything"}) {
		t.Error("should not match key without glob pattern")
	}
}

func TestMatchAttrsGlobValueExists(t *testing.T) {
	dnf := mustCompile(t, "*=err*")

	if !MatchAttrs(dnf, map[string]string{"level": "error"}) {
		t.Error("should match glob value in any key")
	}
	if !MatchAttrs(dnf, map[string]string{"status": "err_timeout"}) {
		t.Error("should match glob value pattern")
	}
	if MatchAttrs(dnf, map[string]string{"level": "info"}) {
		t.Error("should not match non-matching value")
	}
}

func TestMatchAttrsComplex(t *testing.T) {
	// (name=web* AND label.env=prod) OR image=nginx*
	dnf := mustCompile(t, "(name=web* AND label.env=prod) OR image=nginx*")

	if !MatchAttrs(dnf, map[string]string{"name": "web-1", "label.env": "prod"}) {
		t.Error("should match first branch")
	}
	if !MatchAttrs(dnf, map[string]string{"image": "nginx:latest"}) {
		t.Error("should match second branch")
	}
	if MatchAttrs(dnf, map[string]string{"name": "web-1", "label.env": "staging"}) {
		t.Error("should not match first branch with wrong env")
	}
	if MatchAttrs(dnf, map[string]string{"name": "api-1", "image": "redis:7"}) {
		t.Error("should not match neither branch")
	}
}

func mustCompile(t *testing.T, expr string) *DNF {
	t.Helper()
	dnf, err := CompileAttrFilter(expr)
	if err != nil {
		t.Fatalf("CompileAttrFilter(%q): %v", expr, err)
	}
	return dnf
}
