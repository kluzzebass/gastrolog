package cli

import (
	"testing"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "gastrolog/api/gen/gastrolog/v1"
)

func TestNavigateDescriptor(t *testing.T) {
	getDesc := (&v1.GetSettingsResponse{}).ProtoReflect().Descriptor()

	tests := []struct {
		name   string
		path   []string
		expect string // expected message name, "" if nil
	}{
		{"top-level auth", []string{"auth"}, "gastrolog.v1.AuthSettings"},
		{"nested password_policy", []string{"auth", "password_policy"}, "gastrolog.v1.PasswordPolicySettings"},
		{"top-level query", []string{"query"}, "gastrolog.v1.QuerySettings"},
		{"top-level scheduler", []string{"scheduler"}, "gastrolog.v1.SchedulerSettings"},
		{"top-level tls", []string{"tls"}, "gastrolog.v1.TLSSettings"},
		{"top-level lookup", []string{"lookup"}, "gastrolog.v1.LookupSettings"},
		{"nested maxmind", []string{"lookup", "maxmind"}, "gastrolog.v1.MaxMindSettings"},
		{"nonexistent path", []string{"nonexistent"}, ""},
		{"nonexistent nested", []string{"auth", "nonexistent"}, ""},
		{"scalar field (not message)", []string{"setup_wizard_dismissed"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := navigateDescriptor(getDesc, tt.path)
			if tt.expect == "" {
				if got != nil {
					t.Fatalf("expected nil, got %s", got.FullName())
				}
				return
			}
			if got == nil {
				t.Fatalf("expected %s, got nil", tt.expect)
			}
			if string(got.FullName()) != tt.expect {
				t.Fatalf("expected %s, got %s", tt.expect, got.FullName())
			}
		})
	}
}

func TestNavigateDescriptorPut(t *testing.T) {
	putDesc := (&v1.PutSettingsRequest{}).ProtoReflect().Descriptor()

	tests := []struct {
		name   string
		path   []string
		expect string
	}{
		{"put auth", []string{"auth"}, "gastrolog.v1.PutAuthSettings"},
		{"put auth.password_policy", []string{"auth", "password_policy"}, "gastrolog.v1.PutPasswordPolicySettings"},
		{"put query", []string{"query"}, "gastrolog.v1.PutQuerySettings"},
		{"put lookup.maxmind", []string{"lookup", "maxmind"}, "gastrolog.v1.PutMaxMindSettings"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := navigateDescriptor(putDesc, tt.path)
			if got == nil {
				t.Fatalf("expected %s, got nil", tt.expect)
			}
			if string(got.FullName()) != tt.expect {
				t.Fatalf("expected %s, got %s", tt.expect, got.FullName())
			}
		})
	}
}

func TestNavigateMessage(t *testing.T) {
	resp := &v1.GetSettingsResponse{
		Auth: &v1.AuthSettings{
			TokenDuration: "15m",
			PasswordPolicy: &v1.PasswordPolicySettings{
				MinLength: 10,
			},
		},
		Query: &v1.QuerySettings{
			Timeout: "30s",
		},
	}

	msg := resp.ProtoReflect()

	// Navigate to auth.
	auth := navigateMessage(msg, []string{"auth"})
	if auth == nil {
		t.Fatal("expected non-nil auth message")
	}
	fd := auth.Descriptor().Fields().ByName("token_duration")
	if fd == nil {
		t.Fatal("expected token_duration field")
	}
	if auth.Get(fd).String() != "15m" {
		t.Fatalf("expected 15m, got %s", auth.Get(fd).String())
	}

	// Navigate to auth.password_policy.
	pp := navigateMessage(msg, []string{"auth", "password_policy"})
	if pp == nil {
		t.Fatal("expected non-nil password_policy message")
	}
	fd = pp.Descriptor().Fields().ByName("min_length")
	if fd == nil {
		t.Fatal("expected min_length field")
	}
	if pp.Get(fd).Int() != 10 {
		t.Fatalf("expected 10, got %d", pp.Get(fd).Int())
	}

	// Navigate to unset tls should return nil.
	tls := navigateMessage(msg, []string{"tls"})
	if tls != nil {
		t.Fatal("expected nil for unset tls sub-message")
	}

	// Navigate to unset lookup.maxmind should return nil.
	mm := navigateMessage(msg, []string{"lookup", "maxmind"})
	if mm != nil {
		t.Fatal("expected nil for unset lookup.maxmind")
	}
}

func TestEnsureSubMessage(t *testing.T) {
	req := &v1.PutSettingsRequest{}
	msg := req.ProtoReflect()

	// Ensure auth sub-message is created.
	auth := ensureSubMessage(msg, []string{"auth"})
	if auth == nil {
		t.Fatal("expected non-nil auth sub-message")
	}

	// Set a field on it.
	fd := auth.Descriptor().Fields().ByName("token_duration")
	if fd == nil {
		t.Fatal("expected token_duration field descriptor")
	}
	auth.Set(fd, protoreflect.ValueOfString("1h"))

	// Verify the PutSettingsRequest was mutated.
	if req.Auth == nil {
		t.Fatal("expected Auth to be set on request")
	}
	if *req.Auth.TokenDuration != "1h" {
		t.Fatalf("expected 1h, got %v", req.Auth.TokenDuration)
	}

	// Ensure nested path creates intermediate messages.
	pp := ensureSubMessage(msg, []string{"auth", "password_policy"})
	if pp == nil {
		t.Fatal("expected non-nil password_policy sub-message")
	}
	fd = pp.Descriptor().Fields().ByName("min_length")
	pp.Set(fd, protoreflect.ValueOfInt32(12))

	if req.Auth.PasswordPolicy == nil {
		t.Fatal("expected PasswordPolicy to be set")
	}
	if *req.Auth.PasswordPolicy.MinLength != 12 {
		t.Fatalf("expected 12, got %v", req.Auth.PasswordPolicy.MinLength)
	}

	// Ensure lookup.maxmind creates both levels.
	mm := ensureSubMessage(msg, []string{"lookup", "maxmind"})
	if mm == nil {
		t.Fatal("expected non-nil maxmind sub-message")
	}
	fd = mm.Descriptor().Fields().ByName("auto_download")
	mm.Set(fd, protoreflect.ValueOfBool(true))

	if req.Lookup == nil || req.Lookup.Maxmind == nil {
		t.Fatal("expected Lookup.Maxmind to be set")
	}
	if *req.Lookup.Maxmind.AutoDownload != true {
		t.Fatal("expected auto_download to be true")
	}
}

func TestProtoGetString(t *testing.T) {
	resp := &v1.GetSettingsResponse{
		Auth: &v1.AuthSettings{
			TokenDuration:       "15m",
			JwtSecretConfigured: true,
			PasswordPolicy: &v1.PasswordPolicySettings{
				MinLength:        10,
				RequireMixedCase: true,
			},
		},
	}

	authMsg := navigateMessage(resp.ProtoReflect(), []string{"auth"})

	tests := []struct {
		name   string
		field  string
		expect string
	}{
		{"string field", "token_duration", "15m"},
		{"bool field true", "jwt_secret_configured", "true"},
		{"nonexistent", "nonexistent_field", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protoGetString(authMsg, tt.field)
			if got != tt.expect {
				t.Fatalf("expected %q, got %q", tt.expect, got)
			}
		})
	}

	// Test on password_policy sub-message.
	ppMsg := navigateMessage(resp.ProtoReflect(), []string{"auth", "password_policy"})
	if got := protoGetString(ppMsg, "min_length"); got != "10" {
		t.Fatalf("expected \"10\", got %q", got)
	}
	if got := protoGetString(ppMsg, "require_mixed_case"); got != "true" {
		t.Fatalf("expected \"true\", got %q", got)
	}

	// Test nil message returns empty.
	if got := protoGetString(nil, "anything"); got != "" {
		t.Fatalf("expected empty for nil message, got %q", got)
	}
}

func TestFieldDisplayValueSecret(t *testing.T) {
	resp := &v1.GetSettingsResponse{
		Auth: &v1.AuthSettings{
			JwtSecretConfigured: true,
		},
	}

	authMsg := navigateMessage(resp.ProtoReflect(), []string{"auth"})

	configured := settingsField{getKey: "jwt_secret_configured", secret: true}
	if got := fieldDisplayValue(authMsg, configured); got != "(configured)" {
		t.Fatalf("expected (configured), got %q", got)
	}

	// Unset secret.
	resp2 := &v1.GetSettingsResponse{
		Auth: &v1.AuthSettings{
			JwtSecretConfigured: false,
		},
	}
	authMsg2 := navigateMessage(resp2.ProtoReflect(), []string{"auth"})
	if got := fieldDisplayValue(authMsg2, configured); got != "(not set)" {
		t.Fatalf("expected (not set), got %q", got)
	}

	// nil message secret.
	if got := fieldDisplayValue(nil, configured); got != "(not set)" {
		t.Fatalf("expected (not set) for nil, got %q", got)
	}
}

func TestSettingsGroupsFieldMappings(t *testing.T) {
	// This validates the same thing as init() but as an explicit test
	// that provides clearer diagnostics if it fails.
	getDesc := (&v1.GetSettingsResponse{}).ProtoReflect().Descriptor()
	putDesc := (&v1.PutSettingsRequest{}).ProtoReflect().Descriptor()

	for _, g := range settingsGroups {
		getSubDesc := navigateDescriptor(getDesc, g.getPath)
		putSubDesc := navigateDescriptor(putDesc, g.setPath)

		if getSubDesc == nil {
			t.Errorf("group %q: getPath %v does not resolve to a message", g.name, g.getPath)
			continue
		}
		if putSubDesc == nil {
			t.Errorf("group %q: setPath %v does not resolve to a message", g.name, g.setPath)
			continue
		}

		for _, f := range g.fields {
			if f.getKey != "" {
				fd := getSubDesc.Fields().ByName(protoreflect.Name(f.getKey))
				if fd == nil {
					t.Errorf("group %q: getKey %q not found in %s", g.name, f.getKey, getSubDesc.FullName())
				}
			}
			if f.setKey != "" {
				fd := putSubDesc.Fields().ByName(protoreflect.Name(f.setKey))
				if fd == nil {
					t.Errorf("group %q: setKey %q not found in %s", g.name, f.setKey, putSubDesc.FullName())
				}
			}
		}
	}
}

func TestFindGroup(t *testing.T) {
	for _, name := range []string{"auth", "password-policy", "query", "scheduler", "tls", "lookup", "maxmind"} {
		g := findGroup(name)
		if g.name != name {
			t.Errorf("findGroup(%q) returned group with name %q", name, g.name)
		}
	}

	// Unknown group should panic.
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown group")
		}
	}()
	findGroup("nonexistent")
}

func TestProtoGetTyped(t *testing.T) {
	resp := &v1.GetSettingsResponse{
		Auth: &v1.AuthSettings{
			TokenDuration:       "15m",
			JwtSecretConfigured: true,
			PasswordPolicy: &v1.PasswordPolicySettings{
				MinLength:        10,
				RequireMixedCase: true,
			},
		},
		Query: &v1.QuerySettings{
			Timeout: "30s",
		},
	}

	authMsg := navigateMessage(resp.ProtoReflect(), []string{"auth"})
	ppMsg := navigateMessage(resp.ProtoReflect(), []string{"auth", "password_policy"})
	queryMsg := navigateMessage(resp.ProtoReflect(), []string{"query"})

	// String field.
	got := protoGetTyped(authMsg, settingsField{getKey: "token_duration"})
	if got != "15m" {
		t.Fatalf("expected \"15m\", got %v", got)
	}

	// Bool field.
	got = protoGetTyped(ppMsg, settingsField{getKey: "require_mixed_case"})
	if got != true {
		t.Fatalf("expected true, got %v", got)
	}

	// Int32 field.
	got = protoGetTyped(ppMsg, settingsField{getKey: "min_length"})
	if got != int64(10) {
		t.Fatalf("expected int64(10), got %v (%T)", got, got)
	}

	// Secret field — configured (bool true).
	got = protoGetTyped(authMsg, settingsField{getKey: "jwt_secret_configured", secret: true})
	if got != true {
		t.Fatalf("expected true for configured secret, got %v", got)
	}

	// Secret field — not configured (bool false).
	resp2 := &v1.GetSettingsResponse{Auth: &v1.AuthSettings{JwtSecretConfigured: false}}
	authMsg2 := navigateMessage(resp2.ProtoReflect(), []string{"auth"})
	got = protoGetTyped(authMsg2, settingsField{getKey: "jwt_secret_configured", secret: true})
	if got != false {
		t.Fatalf("expected false for unconfigured secret, got %v", got)
	}

	// Nil message — secret returns false.
	got = protoGetTyped(nil, settingsField{getKey: "anything", secret: true})
	if got != false {
		t.Fatalf("expected false for nil secret, got %v", got)
	}

	// Nil message — non-secret returns nil.
	got = protoGetTyped(nil, settingsField{getKey: "anything"})
	if got != nil {
		t.Fatalf("expected nil for nil non-secret, got %v", got)
	}

	// Nonexistent field returns nil.
	got = protoGetTyped(queryMsg, settingsField{getKey: "nonexistent"})
	if got != nil {
		t.Fatalf("expected nil for nonexistent field, got %v", got)
	}
}

func TestApplyFlag(t *testing.T) {
	// Build a PutSettingsRequest and get sub-messages to mutate.
	req := &v1.PutSettingsRequest{}
	msg := req.ProtoReflect()

	// String field: auth.token_duration.
	authMsg := ensureSubMessage(msg, []string{"auth"})
	cmd := &cobra.Command{}
	cmd.Flags().String("token-duration", "1h", "")
	err := applyFlag(cmd, authMsg, settingsField{flag: "token-duration", setKey: "token_duration"})
	if err != nil {
		t.Fatal(err)
	}
	if *req.Auth.TokenDuration != "1h" {
		t.Fatalf("expected 1h, got %v", req.Auth.TokenDuration)
	}

	// Bool field: auth.password_policy.require_mixed_case.
	ppMsg := ensureSubMessage(msg, []string{"auth", "password_policy"})
	cmd2 := &cobra.Command{}
	cmd2.Flags().Bool("require-mixed-case", true, "")
	err = applyFlag(cmd2, ppMsg, settingsField{flag: "require-mixed-case", setKey: "require_mixed_case"})
	if err != nil {
		t.Fatal(err)
	}
	if *req.Auth.PasswordPolicy.RequireMixedCase != true {
		t.Fatal("expected require_mixed_case to be true")
	}

	// Int32 field: auth.password_policy.min_length.
	cmd3 := &cobra.Command{}
	cmd3.Flags().Int32("min-length", 12, "")
	err = applyFlag(cmd3, ppMsg, settingsField{flag: "min-length", setKey: "min_length"})
	if err != nil {
		t.Fatal(err)
	}
	if *req.Auth.PasswordPolicy.MinLength != 12 {
		t.Fatalf("expected 12, got %v", *req.Auth.PasswordPolicy.MinLength)
	}

	// Nil message returns error.
	err = applyFlag(cmd, nil, settingsField{flag: "x", setKey: "x"})
	if err == nil {
		t.Fatal("expected error for nil message")
	}

	// Unknown field returns error.
	err = applyFlag(cmd, authMsg, settingsField{flag: "token-duration", setKey: "nonexistent_field"})
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestFieldDisplayValueNonSecret(t *testing.T) {
	resp := &v1.GetSettingsResponse{
		Query: &v1.QuerySettings{Timeout: "30s"},
	}
	queryMsg := navigateMessage(resp.ProtoReflect(), []string{"query"})

	// Normal field returns its string value.
	got := fieldDisplayValue(queryMsg, settingsField{getKey: "timeout"})
	if got != "30s" {
		t.Fatalf("expected \"30s\", got %q", got)
	}

	// Nil message, non-secret returns empty.
	got = fieldDisplayValue(nil, settingsField{getKey: "timeout"})
	if got != "" {
		t.Fatalf("expected empty for nil non-secret, got %q", got)
	}
}

func TestDisplayLabel(t *testing.T) {
	tests := []struct {
		field  settingsField
		expect string
	}{
		{settingsField{label: "custom_label", getKey: "get_key", setKey: "set_key"}, "custom_label"},
		{settingsField{getKey: "get_key", setKey: "set_key"}, "get_key"},
		{settingsField{setKey: "set_key"}, "set_key"},
		{settingsField{}, ""},
	}

	for _, tt := range tests {
		if got := tt.field.displayLabel(); got != tt.expect {
			t.Errorf("displayLabel() = %q, want %q", got, tt.expect)
		}
	}
}
