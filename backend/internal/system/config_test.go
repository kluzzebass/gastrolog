package system

import (
	"gastrolog/internal/glid"
	"testing"

	"gastrolog/internal/chunk"
)

// ParseSize is the single byte-size parser (ParseBytes, which read "GB" as
// binary, is gone): strict SI/IEC — KB/MB/GB/TB decimal, KiB/MiB/GiB/TiB
// binary — matching the frontend parseBytes and every display label.
func TestParseSizeStrictUnits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected uint64
	}{
		{"100", 100},
		{"100B", 100},
		{"100b", 100},
		{"1KB", 1000},
		{"1KiB", 1024},
		{"64MB", 64 * 1000 * 1000},
		{"64MiB", 64 * 1024 * 1024},
		{"1GB", 1000 * 1000 * 1000},
		{"1gib", 1024 * 1024 * 1024},
		{"1.5GB", 1500 * 1000 * 1000},
		{"2TB", 2 * 1000 * 1000 * 1000 * 1000},
		{" 100 MB ", 100 * 1000 * 1000},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got, err := ParseSize(tc.input)
			if err != nil {
				t.Fatalf("ParseSize(%q) error: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("ParseSize(%q) = %d, want %d", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParseSizeInvalid(t *testing.T) {
	t.Parallel()
	tests := []string{
		"",
		"abc",
		"-100",
		"100XB",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := ParseSize(input)
			if err == nil {
				t.Errorf("ParseSize(%q) expected error, got nil", input)
			}
		})
	}
}

func TestRotationPolicyConfigToPolicy(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{}
		policy, err := cfg.ToRotationPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy != nil {
			t.Error("expected nil policy for empty config")
		}
	})

	t.Run("maxBytes only", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{MaxBytes: new("64MB")}
		policy, err := cfg.ToRotationPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}

		// Test that it triggers rotation at the right size
		state := chunk.ActiveChunkState{Bytes: 64 * 1024 * 1024}
		rec := chunk.Record{Raw: []byte("test")}
		if policy.ShouldRotate(state, rec) == nil {
			t.Error("expected rotation when at max bytes")
		}

		state.Bytes = 1024
		if policy.ShouldRotate(state, rec) != nil {
			t.Error("unexpected rotation when under max bytes")
		}
	})

	t.Run("maxRecords only", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{MaxRecords: new(int64(1000))}
		policy, err := cfg.ToRotationPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}

		state := chunk.ActiveChunkState{Records: 1000}
		rec := chunk.Record{}
		if policy.ShouldRotate(state, rec) == nil {
			t.Error("expected rotation when at max records")
		}

		state.Records = 100
		if policy.ShouldRotate(state, rec) != nil {
			t.Error("unexpected rotation when under max records")
		}
	})

	t.Run("composite", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{
			MaxBytes:   new("1MB"),
			MaxRecords: new(int64(100)),
		}
		policy, err := cfg.ToRotationPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}

		rec := chunk.Record{}

		// Trigger by bytes
		state := chunk.ActiveChunkState{Bytes: 2 * 1024 * 1024, Records: 10}
		if policy.ShouldRotate(state, rec) == nil {
			t.Error("expected rotation when over max bytes")
		}

		// Trigger by records
		state = chunk.ActiveChunkState{Bytes: 1024, Records: 100}
		if policy.ShouldRotate(state, rec) == nil {
			t.Error("expected rotation when at max records")
		}

		// No trigger
		state = chunk.ActiveChunkState{Bytes: 1024, Records: 10}
		if policy.ShouldRotate(state, rec) != nil {
			t.Error("unexpected rotation when under both limits")
		}
	})

	t.Run("invalid maxBytes", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{MaxBytes: new("invalid")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for invalid maxBytes")
		}
	})

	t.Run("invalid maxAge", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{MaxAge: new("invalid")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for invalid maxAge")
		}
	})

	t.Run("negative maxAge", func(t *testing.T) {
		t.Parallel()
		cfg := RotationPolicyConfig{MaxAge: new("-1h")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for negative maxAge")
		}
	})
}

func TestValidateCron(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cron    *string
		wantErr bool
	}{
		{"nil cron", nil, false},
		{"empty string", new(""), false},
		{"every minute", new("* * * * *"), false},
		{"hourly at minute 0", new("0 * * * *"), false},
		{"daily at midnight", new("0 0 * * *"), false},
		{"6-field second-level", new("30 0 * * * *"), false},
		{"invalid expression", new("not-a-cron"), true},
		{"too many fields", new("* * * * * * *"), true},
		{"invalid minute range", new("99 * * * *"), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := RotationPolicyConfig{Cron: tc.cron}
			err := cfg.ValidateCron()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestStringPtr(t *testing.T) {
	t.Parallel()
	p := StringPtr("hello")
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != "hello" {
		t.Errorf("got %q, want %q", *p, "hello")
	}

	// Empty string.
	p = StringPtr("")
	if p == nil {
		t.Fatal("expected non-nil pointer for empty string")
	}
	if *p != "" {
		t.Errorf("got %q, want empty string", *p)
	}
}

func TestUUIDPtr(t *testing.T) {
	t.Parallel()
	id := glid.New()
	p := UUIDPtr(id)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != id {
		t.Errorf("got %v, want %v", *p, id)
	}

	// Zero UUID.
	p = UUIDPtr(glid.Nil)
	if p == nil {
		t.Fatal("expected non-nil pointer for zero UUID")
	}
	if *p != glid.Nil {
		t.Errorf("got %v, want zero UUID", *p)
	}
}

func TestToRetentionPolicy(t *testing.T) {
	t.Parallel()
	t.Run("empty config", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{}
		policy, err := cfg.ToRetentionPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy != nil {
			t.Error("expected nil policy for empty config")
		}
	})

	t.Run("maxAge only", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxAge: new("24h")}
		policy, err := cfg.ToRetentionPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}
	})

	t.Run("maxBytes only", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxBytes: new("10GB")}
		policy, err := cfg.ToRetentionPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}
	})

	t.Run("maxChunks only", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxChunks: new(int64(5))}
		policy, err := cfg.ToRetentionPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}
	})

	t.Run("composite age and chunks", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{
			MaxAge:    new("720h"),
			MaxChunks: new(int64(100)),
		}
		policy, err := cfg.ToRetentionPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy for composite config")
		}
	})

	t.Run("all three conditions", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{
			MaxAge:    new("720h"),
			MaxBytes:  new("10GB"),
			MaxChunks: new(int64(50)),
		}
		policy, err := cfg.ToRetentionPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy for triple-condition config")
		}
	})

	t.Run("invalid maxAge", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxAge: new("not-a-duration")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for invalid maxAge")
		}
	})

	t.Run("negative maxAge", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxAge: new("-1h")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for negative maxAge")
		}
	})

	t.Run("zero maxAge", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxAge: new("0s")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for zero maxAge")
		}
	})

	t.Run("invalid maxBytes", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxBytes: new("not-bytes")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for invalid maxBytes")
		}
	})

	t.Run("zero maxChunks", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxChunks: new(int64(0))}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for zero maxChunks")
		}
	})

	t.Run("negative maxChunks", func(t *testing.T) {
		t.Parallel()
		cfg := RetentionPolicyConfig{MaxChunks: new(int64(-1))}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for negative maxChunks")
		}
	})
}

// gastrolog-1rbuf regression: IsEmpty must report true for fully-unset
// configs and false as soon as any condition is populated. PutRotationPolicy
// uses IsEmpty to reject silent-no-op policies at the admission boundary.
func TestRotationPolicyConfigIsEmpty(t *testing.T) {
	t.Parallel()
	empty := ""
	cases := []struct {
		name string
		cfg  RotationPolicyConfig
		want bool
	}{
		{"zero value", RotationPolicyConfig{}, true},
		{"empty MaxBytes string", RotationPolicyConfig{MaxBytes: &empty}, true},
		{"empty MaxAge string", RotationPolicyConfig{MaxAge: &empty}, true},
		{"empty Cron string", RotationPolicyConfig{Cron: &empty}, true},
		{"all empty strings", RotationPolicyConfig{MaxBytes: &empty, MaxAge: &empty, Cron: &empty}, true},
		{"MaxBytes set", RotationPolicyConfig{MaxBytes: new("64MB")}, false},
		{"MaxAge set", RotationPolicyConfig{MaxAge: new("1h")}, false},
		{"MaxRecords set", RotationPolicyConfig{MaxRecords: new(int64(1000))}, false},
		{"Cron set", RotationPolicyConfig{Cron: new("0 * * * *")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.IsEmpty(); got != tc.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tc.want)
			}
		})
	}
}

// gastrolog-1rbuf regression: same shape for retention policies.
func TestRetentionPolicyConfigIsEmpty(t *testing.T) {
	t.Parallel()
	empty := ""
	cases := []struct {
		name string
		cfg  RetentionPolicyConfig
		want bool
	}{
		{"zero value", RetentionPolicyConfig{}, true},
		{"empty MaxAge string", RetentionPolicyConfig{MaxAge: &empty}, true},
		{"empty MaxBytes string", RetentionPolicyConfig{MaxBytes: &empty}, true},
		{"all empty strings", RetentionPolicyConfig{MaxAge: &empty, MaxBytes: &empty}, true},
		{"MaxAge set", RetentionPolicyConfig{MaxAge: new("24h")}, false},
		{"MaxBytes set", RetentionPolicyConfig{MaxBytes: new("10GB")}, false},
		{"MaxChunks set", RetentionPolicyConfig{MaxChunks: new(int64(10))}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.IsEmpty(); got != tc.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tc.want)
			}
		})
	}
}
