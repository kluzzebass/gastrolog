package config

import (
	"testing"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

func TestParseBytesValid(t *testing.T) {
	tests := []struct {
		input    string
		expected uint64
	}{
		{"100", 100},
		{"100B", 100},
		{"100b", 100},
		{"1KB", 1024},
		{"1kb", 1024},
		{"64MB", 64 * 1024 * 1024},
		{"64mb", 64 * 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"1gb", 1024 * 1024 * 1024},
		{" 100 MB ", 100 * 1024 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseBytes(tc.input)
			if err != nil {
				t.Fatalf("ParseBytes(%q) error: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("ParseBytes(%q) = %d, want %d", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParseBytesInvalid(t *testing.T) {
	tests := []string{
		"",
		"abc",
		"-100",
		"100TB",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := ParseBytes(input)
			if err == nil {
				t.Errorf("ParseBytes(%q) expected error, got nil", input)
			}
		})
	}
}

func TestRotationPolicyConfigToPolicy(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
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
		cfg := RotationPolicyConfig{MaxBytes: new("invalid")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for invalid maxBytes")
		}
	})

	t.Run("invalid maxAge", func(t *testing.T) {
		cfg := RotationPolicyConfig{MaxAge: new("invalid")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for invalid maxAge")
		}
	})

	t.Run("negative maxAge", func(t *testing.T) {
		cfg := RotationPolicyConfig{MaxAge: new("-1h")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for negative maxAge")
		}
	})
}

func TestValidateCron(t *testing.T) {
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
	id := uuid.Must(uuid.NewV7())
	p := UUIDPtr(id)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != id {
		t.Errorf("got %v, want %v", *p, id)
	}

	// Zero UUID.
	p = UUIDPtr(uuid.Nil)
	if p == nil {
		t.Fatal("expected non-nil pointer for zero UUID")
	}
	if *p != uuid.Nil {
		t.Errorf("got %v, want zero UUID", *p)
	}
}

func TestToRetentionPolicy(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
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
		cfg := RetentionPolicyConfig{MaxAge: new("not-a-duration")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for invalid maxAge")
		}
	})

	t.Run("negative maxAge", func(t *testing.T) {
		cfg := RetentionPolicyConfig{MaxAge: new("-1h")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for negative maxAge")
		}
	})

	t.Run("zero maxAge", func(t *testing.T) {
		cfg := RetentionPolicyConfig{MaxAge: new("0s")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for zero maxAge")
		}
	})

	t.Run("invalid maxBytes", func(t *testing.T) {
		cfg := RetentionPolicyConfig{MaxBytes: new("not-bytes")}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for invalid maxBytes")
		}
	})

	t.Run("zero maxChunks", func(t *testing.T) {
		cfg := RetentionPolicyConfig{MaxChunks: new(int64(0))}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for zero maxChunks")
		}
	})

	t.Run("negative maxChunks", func(t *testing.T) {
		cfg := RetentionPolicyConfig{MaxChunks: new(int64(-1))}
		_, err := cfg.ToRetentionPolicy()
		if err == nil {
			t.Error("expected error for negative maxChunks")
		}
	})
}
