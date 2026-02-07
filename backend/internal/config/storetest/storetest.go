// Package storetest provides a shared conformance test suite for config.Store
// implementations. Each backend (memory, file, sqlite) wires this suite to
// verify it satisfies the full Store contract.
package storetest

import (
	"context"
	"testing"

	"gastrolog/internal/config"
)

// TestStore runs the full conformance suite against a Store implementation.
// newStore must return a fresh, empty store for each sub-test.
func TestStore(t *testing.T, newStore func(t *testing.T) config.Store) {
	t.Run("LoadEmpty", func(t *testing.T) {
		s := newStore(t)
		cfg, err := s.Load(context.Background())
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg != nil {
			t.Fatalf("expected nil config from empty store, got %+v", cfg)
		}
	})

	// Rotation policies
	t.Run("PutGetRotationPolicy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		rp := config.RotationPolicyConfig{
			MaxBytes:   config.StringPtr("64MB"),
			MaxAge:     config.StringPtr("1h"),
			MaxRecords: config.Int64Ptr(1000),
		}

		if err := s.PutRotationPolicy(ctx, "default", rp); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, "default")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected rotation policy, got nil")
		}
		assertStringPtr(t, "MaxBytes", got.MaxBytes, "64MB")
		assertStringPtr(t, "MaxAge", got.MaxAge, "1h")
		assertInt64Ptr(t, "MaxRecords", got.MaxRecords, 1000)
	})

	t.Run("PutRotationPolicyUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		rp1 := config.RotationPolicyConfig{MaxAge: config.StringPtr("1h")}
		if err := s.PutRotationPolicy(ctx, "p1", rp1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		rp2 := config.RotationPolicyConfig{MaxAge: config.StringPtr("2h")}
		if err := s.PutRotationPolicy(ctx, "p1", rp2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, "p1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		assertStringPtr(t, "MaxAge", got.MaxAge, "2h")

		// Should still be only one policy.
		all, err := s.ListRotationPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("expected 1 policy after upsert, got %d", len(all))
		}
	})

	t.Run("ListRotationPolicies", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// Empty list.
		all, err := s.ListRotationPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("expected 0, got %d", len(all))
		}

		if err := s.PutRotationPolicy(ctx, "a", config.RotationPolicyConfig{MaxAge: config.StringPtr("1h")}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutRotationPolicy(ctx, "b", config.RotationPolicyConfig{MaxBytes: config.StringPtr("10MB")}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListRotationPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}
		if _, ok := all["a"]; !ok {
			t.Error("missing policy 'a'")
		}
		if _, ok := all["b"]; !ok {
			t.Error("missing policy 'b'")
		}
	})

	t.Run("DeleteRotationPolicy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		if err := s.PutRotationPolicy(ctx, "del", config.RotationPolicyConfig{MaxAge: config.StringPtr("5m")}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteRotationPolicy(ctx, "del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, "del")
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		// Delete non-existent is a no-op.
		if err := s.DeleteRotationPolicy(ctx, "nonexistent"); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilOptionalFields", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// All nil pointer fields.
		rp := config.RotationPolicyConfig{}
		if err := s.PutRotationPolicy(ctx, "empty", rp); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, "empty")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected rotation policy, got nil")
		}
		if got.MaxBytes != nil {
			t.Errorf("expected nil MaxBytes, got %v", *got.MaxBytes)
		}
		if got.MaxAge != nil {
			t.Errorf("expected nil MaxAge, got %v", *got.MaxAge)
		}
		if got.MaxRecords != nil {
			t.Errorf("expected nil MaxRecords, got %v", *got.MaxRecords)
		}
	})

	// Stores
	t.Run("PutGetStore", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		st := config.StoreConfig{
			ID:     "main",
			Type:   "file",
			Route:  config.StringPtr("*"),
			Policy: config.StringPtr("default"),
			Params: map[string]string{"dir": "/var/log"},
		}

		if err := s.PutStore(ctx, st); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetStore(ctx, "main")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected store, got nil")
		}
		if got.ID != "main" {
			t.Errorf("ID: expected %q, got %q", "main", got.ID)
		}
		if got.Type != "file" {
			t.Errorf("Type: expected %q, got %q", "file", got.Type)
		}
		assertStringPtr(t, "Route", got.Route, "*")
		assertStringPtr(t, "Policy", got.Policy, "default")
		if got.Params["dir"] != "/var/log" {
			t.Errorf("Params[dir]: expected %q, got %q", "/var/log", got.Params["dir"])
		}
	})

	t.Run("PutStoreUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		st1 := config.StoreConfig{ID: "s1", Type: "file", Route: config.StringPtr("*")}
		if err := s.PutStore(ctx, st1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		st2 := config.StoreConfig{ID: "s1", Type: "memory", Route: config.StringPtr("env=prod")}
		if err := s.PutStore(ctx, st2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetStore(ctx, "s1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Type != "memory" {
			t.Errorf("Type after upsert: expected %q, got %q", "memory", got.Type)
		}

		all, err := s.ListStores(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("expected 1 store after upsert, got %d", len(all))
		}
	})

	t.Run("ListStores", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		all, err := s.ListStores(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("expected 0, got %d", len(all))
		}

		if err := s.PutStore(ctx, config.StoreConfig{ID: "a", Type: "file"}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutStore(ctx, config.StoreConfig{ID: "b", Type: "memory"}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListStores(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}

		ids := map[string]bool{}
		for _, st := range all {
			ids[st.ID] = true
		}
		if !ids["a"] || !ids["b"] {
			t.Errorf("expected stores a and b, got %v", ids)
		}
	})

	t.Run("DeleteStore", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		if err := s.PutStore(ctx, config.StoreConfig{ID: "del", Type: "file"}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteStore(ctx, "del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetStore(ctx, "del")
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		// Delete non-existent is a no-op.
		if err := s.DeleteStore(ctx, "nonexistent"); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilStoreParams", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		st := config.StoreConfig{ID: "s1", Type: "memory", Params: nil}
		if err := s.PutStore(ctx, st); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetStore(ctx, "s1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Params != nil {
			t.Errorf("expected nil Params, got %v", got.Params)
		}
	})

	// Ingesters
	t.Run("PutGetIngester", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		ing := config.IngesterConfig{
			ID:     "syslog1",
			Type:   "syslog-udp",
			Params: map[string]string{"port": "514"},
		}

		if err := s.PutIngester(ctx, ing); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetIngester(ctx, "syslog1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected ingester, got nil")
		}
		if got.ID != "syslog1" {
			t.Errorf("ID: expected %q, got %q", "syslog1", got.ID)
		}
		if got.Type != "syslog-udp" {
			t.Errorf("Type: expected %q, got %q", "syslog-udp", got.Type)
		}
		if got.Params["port"] != "514" {
			t.Errorf("Params[port]: expected %q, got %q", "514", got.Params["port"])
		}
	})

	t.Run("PutIngesterUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		ing1 := config.IngesterConfig{ID: "i1", Type: "syslog-udp"}
		if err := s.PutIngester(ctx, ing1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		ing2 := config.IngesterConfig{ID: "i1", Type: "file"}
		if err := s.PutIngester(ctx, ing2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetIngester(ctx, "i1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Type != "file" {
			t.Errorf("Type after upsert: expected %q, got %q", "file", got.Type)
		}

		all, err := s.ListIngesters(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("expected 1 ingester after upsert, got %d", len(all))
		}
	})

	t.Run("ListIngesters", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		all, err := s.ListIngesters(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("expected 0, got %d", len(all))
		}

		if err := s.PutIngester(ctx, config.IngesterConfig{ID: "a", Type: "syslog-udp"}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: "b", Type: "file"}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListIngesters(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}

		ids := map[string]bool{}
		for _, ing := range all {
			ids[ing.ID] = true
		}
		if !ids["a"] || !ids["b"] {
			t.Errorf("expected ingesters a and b, got %v", ids)
		}
	})

	t.Run("DeleteIngester", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		if err := s.PutIngester(ctx, config.IngesterConfig{ID: "del", Type: "test"}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteIngester(ctx, "del"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetIngester(ctx, "del")
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		// Delete non-existent is a no-op.
		if err := s.DeleteIngester(ctx, "nonexistent"); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilIngesterParams", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		ing := config.IngesterConfig{ID: "i1", Type: "test", Params: nil}
		if err := s.PutIngester(ctx, ing); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetIngester(ctx, "i1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Params != nil {
			t.Errorf("expected nil Params, got %v", got.Params)
		}
	})

	// Integration
	t.Run("LoadAfterCRUD", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// Put several entities via CRUD.
		if err := s.PutRotationPolicy(ctx, "fast", config.RotationPolicyConfig{MaxAge: config.StringPtr("5m")}); err != nil {
			t.Fatalf("PutRotationPolicy: %v", err)
		}
		if err := s.PutRotationPolicy(ctx, "slow", config.RotationPolicyConfig{MaxAge: config.StringPtr("1h")}); err != nil {
			t.Fatalf("PutRotationPolicy: %v", err)
		}
		if err := s.PutStore(ctx, config.StoreConfig{ID: "main", Type: "file", Route: config.StringPtr("*"), Policy: config.StringPtr("fast")}); err != nil {
			t.Fatalf("PutStore: %v", err)
		}
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: "sys1", Type: "syslog-udp", Params: map[string]string{"port": "514"}}); err != nil {
			t.Fatalf("PutIngester: %v", err)
		}
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: "file1", Type: "file", Params: map[string]string{"path": "/var/log/app.log"}}); err != nil {
			t.Fatalf("PutIngester: %v", err)
		}

		// Load should return the full Config.
		cfg, err := s.Load(ctx)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected config, got nil")
		}

		if len(cfg.RotationPolicies) != 2 {
			t.Errorf("expected 2 rotation policies, got %d", len(cfg.RotationPolicies))
		}
		if len(cfg.Stores) != 1 {
			t.Errorf("expected 1 store, got %d", len(cfg.Stores))
		}
		if len(cfg.Ingesters) != 2 {
			t.Errorf("expected 2 ingesters, got %d", len(cfg.Ingesters))
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		rp, err := s.GetRotationPolicy(ctx, "nope")
		if err != nil {
			t.Fatalf("GetRotationPolicy: %v", err)
		}
		if rp != nil {
			t.Errorf("expected nil, got %+v", rp)
		}

		st, err := s.GetStore(ctx, "nope")
		if err != nil {
			t.Fatalf("GetStore: %v", err)
		}
		if st != nil {
			t.Errorf("expected nil, got %+v", st)
		}

		ing, err := s.GetIngester(ctx, "nope")
		if err != nil {
			t.Fatalf("GetIngester: %v", err)
		}
		if ing != nil {
			t.Errorf("expected nil, got %+v", ing)
		}
	})
}

func assertStringPtr(t *testing.T, name string, got *string, want string) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: expected %q, got nil", name, want)
		return
	}
	if *got != want {
		t.Errorf("%s: expected %q, got %q", name, want, *got)
	}
}

func assertInt64Ptr(t *testing.T, name string, got *int64, want int64) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: expected %d, got nil", name, want)
		return
	}
	if *got != want {
		t.Errorf("%s: expected %d, got %d", name, want, *got)
	}
}
