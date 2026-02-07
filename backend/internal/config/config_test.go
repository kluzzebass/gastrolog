package config

import (
	"testing"

	"gastrolog/internal/chunk"
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
			got, err := parseBytes(tc.input)
			if err != nil {
				t.Fatalf("parseBytes(%q) error: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Errorf("parseBytes(%q) = %d, want %d", tc.input, got, tc.expected)
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
			_, err := parseBytes(input)
			if err == nil {
				t.Errorf("parseBytes(%q) expected error, got nil", input)
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
		cfg := RotationPolicyConfig{MaxBytes: StringPtr("64MB")}
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
		if !policy.ShouldRotate(state, rec) {
			t.Error("expected rotation when at max bytes")
		}

		state.Bytes = 1024
		if policy.ShouldRotate(state, rec) {
			t.Error("unexpected rotation when under max bytes")
		}
	})

	t.Run("maxRecords only", func(t *testing.T) {
		cfg := RotationPolicyConfig{MaxRecords: Int64Ptr(1000)}
		policy, err := cfg.ToRotationPolicy()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if policy == nil {
			t.Fatal("expected non-nil policy")
		}

		state := chunk.ActiveChunkState{Records: 1000}
		rec := chunk.Record{}
		if !policy.ShouldRotate(state, rec) {
			t.Error("expected rotation when at max records")
		}

		state.Records = 100
		if policy.ShouldRotate(state, rec) {
			t.Error("unexpected rotation when under max records")
		}
	})

	t.Run("composite", func(t *testing.T) {
		cfg := RotationPolicyConfig{
			MaxBytes:   StringPtr("1MB"),
			MaxRecords: Int64Ptr(100),
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
		if !policy.ShouldRotate(state, rec) {
			t.Error("expected rotation when over max bytes")
		}

		// Trigger by records
		state = chunk.ActiveChunkState{Bytes: 1024, Records: 100}
		if !policy.ShouldRotate(state, rec) {
			t.Error("expected rotation when at max records")
		}

		// No trigger
		state = chunk.ActiveChunkState{Bytes: 1024, Records: 10}
		if policy.ShouldRotate(state, rec) {
			t.Error("unexpected rotation when under both limits")
		}
	})

	t.Run("invalid maxBytes", func(t *testing.T) {
		cfg := RotationPolicyConfig{MaxBytes: StringPtr("invalid")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for invalid maxBytes")
		}
	})

	t.Run("invalid maxAge", func(t *testing.T) {
		cfg := RotationPolicyConfig{MaxAge: StringPtr("invalid")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for invalid maxAge")
		}
	})

	t.Run("negative maxAge", func(t *testing.T) {
		cfg := RotationPolicyConfig{MaxAge: StringPtr("-1h")}
		_, err := cfg.ToRotationPolicy()
		if err == nil {
			t.Error("expected error for negative maxAge")
		}
	})
}
