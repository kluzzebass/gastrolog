// Package vaulttest provides a shared conformance test suite for config.Store
// implementations. Each backend (memory, raft) wires this suite to
// verify it satisfies the full Store contract.
package storetest

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/config"

	"github.com/google/uuid"
)

func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// TestVault runs the full conformance suite against a Vault implementation.
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

	// Filters
	t.Run("PutGetFilter", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		fc := config.FilterConfig{ID: id, Name: "catch-all", Expression: "*"}
		if err := s.PutFilter(ctx, fc); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetFilter(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected filter, got nil")
		}
		if got.Name != "catch-all" {
			t.Errorf("Name: expected %q, got %q", "catch-all", got.Name)
		}
		if got.Expression != "*" {
			t.Errorf("Expression: expected %q, got %q", "*", got.Expression)
		}
	})

	// Rotation policies
	t.Run("PutGetRotationPolicy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		rp := config.RotationPolicyConfig{
			ID:         id,
			Name:       "default",
			MaxBytes:   new("64MB"),
			MaxAge:     new("1h"),
			MaxRecords: new(int64(1000)),
			Cron:       new("0 * * * *"),
		}

		if err := s.PutRotationPolicy(ctx, rp); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected rotation policy, got nil")
		}
		assertStringPtr(t, "MaxBytes", got.MaxBytes, "64MB")
		assertStringPtr(t, "MaxAge", got.MaxAge, "1h")
		assertInt64Ptr(t, "MaxRecords", got.MaxRecords, 1000)
		assertStringPtr(t, "Cron", got.Cron, "0 * * * *")
	})

	t.Run("PutRotationPolicyUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		rp1 := config.RotationPolicyConfig{ID: id, Name: "p1", MaxAge: new("1h"), Cron: new("0 * * * *")}
		if err := s.PutRotationPolicy(ctx, rp1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		// Upsert: change MaxAge, remove Cron.
		rp2 := config.RotationPolicyConfig{ID: id, Name: "p1", MaxAge: new("2h")}
		if err := s.PutRotationPolicy(ctx, rp2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		assertStringPtr(t, "MaxAge", got.MaxAge, "2h")
		if got.Cron != nil {
			t.Errorf("expected Cron to be nil after upsert without cron, got %q", *got.Cron)
		}

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

		idA := newID()
		idB := newID()
		if err := s.PutRotationPolicy(ctx, config.RotationPolicyConfig{ID: idA, Name: "a", MaxAge: new("1h")}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutRotationPolicy(ctx, config.RotationPolicyConfig{ID: idB, Name: "b", MaxBytes: new("10MB")}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListRotationPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}
		ids := map[uuid.UUID]bool{}
		for _, rp := range all {
			ids[rp.ID] = true
		}
		if !ids[idA] || !ids[idB] {
			t.Errorf("expected policies %s and %s, got %v", idA, idB, ids)
		}
	})

	t.Run("DeleteRotationPolicy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		if err := s.PutRotationPolicy(ctx, config.RotationPolicyConfig{ID: id, Name: "del", MaxAge: new("5m")}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteRotationPolicy(ctx, id); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		// Delete non-existent is a no-op.
		if err := s.DeleteRotationPolicy(ctx, uuid.Must(uuid.NewV7())); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilOptionalFields", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// All nil pointer fields.
		id := newID()
		rp := config.RotationPolicyConfig{ID: id, Name: "empty"}
		if err := s.PutRotationPolicy(ctx, rp); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetRotationPolicy(ctx, id)
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

	// Retention policies
	t.Run("PutGetRetentionPolicy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		rp := config.RetentionPolicyConfig{
			ID:        id,
			Name:      "default",
			MaxAge:    new("720h"),
			MaxBytes:  new("10GB"),
			MaxChunks: new(int64(100)),
		}

		if err := s.PutRetentionPolicy(ctx, rp); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetRetentionPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected retention policy, got nil")
		}
		assertStringPtr(t, "MaxAge", got.MaxAge, "720h")
		assertStringPtr(t, "MaxBytes", got.MaxBytes, "10GB")
		assertInt64Ptr(t, "MaxChunks", got.MaxChunks, 100)
	})

	t.Run("PutRetentionPolicyUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		rp1 := config.RetentionPolicyConfig{ID: id, Name: "p1", MaxAge: new("24h")}
		if err := s.PutRetentionPolicy(ctx, rp1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		rp2 := config.RetentionPolicyConfig{ID: id, Name: "p1", MaxAge: new("48h")}
		if err := s.PutRetentionPolicy(ctx, rp2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetRetentionPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		assertStringPtr(t, "MaxAge", got.MaxAge, "48h")

		all, err := s.ListRetentionPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("expected 1 policy after upsert, got %d", len(all))
		}
	})

	t.Run("ListRetentionPolicies", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		all, err := s.ListRetentionPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("expected 0, got %d", len(all))
		}

		idA := newID()
		idB := newID()
		if err := s.PutRetentionPolicy(ctx, config.RetentionPolicyConfig{ID: idA, Name: "a", MaxAge: new("1h")}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutRetentionPolicy(ctx, config.RetentionPolicyConfig{ID: idB, Name: "b", MaxChunks: new(int64(5))}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListRetentionPolicies(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}
		ids := map[uuid.UUID]bool{}
		for _, rp := range all {
			ids[rp.ID] = true
		}
		if !ids[idA] || !ids[idB] {
			t.Errorf("expected policies %s and %s, got %v", idA, idB, ids)
		}
	})

	t.Run("DeleteRetentionPolicy", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		if err := s.PutRetentionPolicy(ctx, config.RetentionPolicyConfig{ID: id, Name: "del", MaxAge: new("5m")}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteRetentionPolicy(ctx, id); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetRetentionPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		if err := s.DeleteRetentionPolicy(ctx, uuid.Must(uuid.NewV7())); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilRetentionOptionalFields", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		rp := config.RetentionPolicyConfig{ID: id, Name: "empty"}
		if err := s.PutRetentionPolicy(ctx, rp); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetRetentionPolicy(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected retention policy, got nil")
		}
		if got.MaxAge != nil {
			t.Errorf("expected nil MaxAge, got %v", *got.MaxAge)
		}
		if got.MaxBytes != nil {
			t.Errorf("expected nil MaxBytes, got %v", *got.MaxBytes)
		}
		if got.MaxChunks != nil {
			t.Errorf("expected nil MaxChunks, got %v", *got.MaxChunks)
		}
	})

	// Vault retention rules
	t.Run("VaultRetentionRules", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		retID := newID()
		dstID := newID()
		v := config.VaultConfig{
			ID:   id,
			Name: "main",
			Type: "file",
			RetentionRules: []config.RetentionRule{
				{RetentionPolicyID: retID, Action: config.RetentionActionExpire},
				{RetentionPolicyID: retID, Action: config.RetentionActionMigrate, Destination: &dstID},
			},
		}
		if err := s.PutVault(ctx, v); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetVault(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.RetentionRules) != 2 {
			t.Fatalf("expected 2 rules, got %d", len(got.RetentionRules))
		}
		if got.RetentionRules[0].RetentionPolicyID != retID {
			t.Errorf("rule[0] policy: got %s, want %s", got.RetentionRules[0].RetentionPolicyID, retID)
		}
		if got.RetentionRules[0].Action != config.RetentionActionExpire {
			t.Errorf("rule[0] action: got %q, want %q", got.RetentionRules[0].Action, config.RetentionActionExpire)
		}
		if got.RetentionRules[1].Action != config.RetentionActionMigrate {
			t.Errorf("rule[1] action: got %q, want %q", got.RetentionRules[1].Action, config.RetentionActionMigrate)
		}
		if got.RetentionRules[1].Destination == nil || *got.RetentionRules[1].Destination != dstID {
			t.Errorf("rule[1] destination: got %v, want %s", got.RetentionRules[1].Destination, dstID)
		}
	})

	// Vaults
	t.Run("PutGetVault", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		policyID := newID()
		v := config.VaultConfig{
			ID:     id,
			Name:   "main",
			Type:   "file",
			Policy: &policyID,
			Params: map[string]string{"dir": "/var/log"},
		}

		if err := s.PutVault(ctx, v); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetVault(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected vault, got nil")
		}
		if got.ID != id {
			t.Errorf("ID: expected %s, got %s", id, got.ID)
		}
		if got.Name != "main" {
			t.Errorf("Name: expected %q, got %q", "main", got.Name)
		}
		if got.Type != "file" {
			t.Errorf("Type: expected %q, got %q", "file", got.Type)
		}
		assertUUIDPtr(t, "Policy", got.Policy, policyID)
		if got.Params["dir"] != "/var/log" {
			t.Errorf("Params[dir]: expected %q, got %q", "/var/log", got.Params["dir"])
		}
	})

	t.Run("PutVaultUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		v1 := config.VaultConfig{ID: id, Name: "s1", Type: "file"}
		if err := s.PutVault(ctx, v1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		v2 := config.VaultConfig{ID: id, Name: "s1", Type: "memory"}
		if err := s.PutVault(ctx, v2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetVault(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Type != "memory" {
			t.Errorf("Type after upsert: expected %q, got %q", "memory", got.Type)
		}

		all, err := s.ListVaults(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 1 {
			t.Fatalf("expected 1 vault after upsert, got %d", len(all))
		}
	})

	t.Run("ListVaults", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		all, err := s.ListVaults(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("expected 0, got %d", len(all))
		}

		idA := newID()
		idB := newID()
		if err := s.PutVault(ctx, config.VaultConfig{ID: idA, Name: "a", Type: "file"}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutVault(ctx, config.VaultConfig{ID: idB, Name: "b", Type: "memory"}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListVaults(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}

		ids := map[uuid.UUID]bool{}
		for _, v := range all {
			ids[v.ID] = true
		}
		if !ids[idA] || !ids[idB] {
			t.Errorf("expected vaults %s and %s, got %v", idA, idB, ids)
		}
	})

	t.Run("DeleteVault", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		if err := s.PutVault(ctx, config.VaultConfig{ID: id, Name: "del", Type: "file"}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteVault(ctx, id); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetVault(ctx, id)
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		// Delete non-existent is a no-op.
		if err := s.DeleteVault(ctx, uuid.Must(uuid.NewV7())); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilVaultParams", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		v := config.VaultConfig{ID: id, Name: "s1", Type: "memory", Params: nil}
		if err := s.PutVault(ctx, v); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetVault(ctx, id)
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

		id := newID()
		ing := config.IngesterConfig{
			ID:      id,
			Name:    "syslog1",
			Type:    "syslog-udp",
			Enabled: true,
			Params:  map[string]string{"port": "514"},
		}

		if err := s.PutIngester(ctx, ing); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetIngester(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected ingester, got nil")
		}
		if got.ID != id {
			t.Errorf("ID: expected %s, got %s", id, got.ID)
		}
		if got.Name != "syslog1" {
			t.Errorf("Name: expected %q, got %q", "syslog1", got.Name)
		}
		if got.Type != "syslog-udp" {
			t.Errorf("Type: expected %q, got %q", "syslog-udp", got.Type)
		}
		if !got.Enabled {
			t.Error("Enabled: expected true, got false")
		}
		if got.Params["port"] != "514" {
			t.Errorf("Params[port]: expected %q, got %q", "514", got.Params["port"])
		}
	})

	t.Run("PutIngesterUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		ing1 := config.IngesterConfig{ID: id, Name: "i1", Type: "syslog-udp"}
		if err := s.PutIngester(ctx, ing1); err != nil {
			t.Fatalf("Put: %v", err)
		}

		ing2 := config.IngesterConfig{ID: id, Name: "i1", Type: "file"}
		if err := s.PutIngester(ctx, ing2); err != nil {
			t.Fatalf("Put upsert: %v", err)
		}

		got, err := s.GetIngester(ctx, id)
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

		idA := newID()
		idB := newID()
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: idA, Name: "a", Type: "syslog-udp"}); err != nil {
			t.Fatalf("Put a: %v", err)
		}
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: idB, Name: "b", Type: "file"}); err != nil {
			t.Fatalf("Put b: %v", err)
		}

		all, err = s.ListIngesters(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2, got %d", len(all))
		}

		ids := map[uuid.UUID]bool{}
		for _, ing := range all {
			ids[ing.ID] = true
		}
		if !ids[idA] || !ids[idB] {
			t.Errorf("expected ingesters %s and %s, got %v", idA, idB, ids)
		}
	})

	t.Run("DeleteIngester", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: id, Name: "del", Type: "test"}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		if err := s.DeleteIngester(ctx, id); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		got, err := s.GetIngester(ctx, id)
		if err != nil {
			t.Fatalf("Get after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		// Delete non-existent is a no-op.
		if err := s.DeleteIngester(ctx, uuid.Must(uuid.NewV7())); err != nil {
			t.Fatalf("Delete non-existent: %v", err)
		}
	})

	t.Run("NilIngesterParams", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		ing := config.IngesterConfig{ID: id, Name: "i1", Type: "test", Params: nil}
		if err := s.PutIngester(ctx, ing); err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := s.GetIngester(ctx, id)
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

		rpFastID := newID()
		rpSlowID := newID()
		vaultID := newID()
		ing1ID := newID()
		ing2ID := newID()

		// Put several entities via CRUD.
		if err := s.PutRotationPolicy(ctx, config.RotationPolicyConfig{ID: rpFastID, Name: "fast", MaxAge: new("5m")}); err != nil {
			t.Fatalf("PutRotationPolicy: %v", err)
		}
		if err := s.PutRotationPolicy(ctx, config.RotationPolicyConfig{ID: rpSlowID, Name: "slow", MaxAge: new("1h")}); err != nil {
			t.Fatalf("PutRotationPolicy: %v", err)
		}
		if err := s.PutVault(ctx, config.VaultConfig{ID: vaultID, Name: "main", Type: "file", Policy: &rpFastID}); err != nil {
			t.Fatalf("PutVault: %v", err)
		}
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: ing1ID, Name: "sys1", Type: "syslog-udp", Enabled: true, Params: map[string]string{"port": "514"}}); err != nil {
			t.Fatalf("PutIngester: %v", err)
		}
		if err := s.PutIngester(ctx, config.IngesterConfig{ID: ing2ID, Name: "file1", Type: "file", Enabled: true, Params: map[string]string{"path": "/var/log/app.log"}}); err != nil {
			t.Fatalf("PutIngester: %v", err)
		}

		if err := s.SaveServerSettings(ctx, config.ServerSettings{Auth: config.AuthConfig{JWTSecret: "s3cret"}}); err != nil {
			t.Fatalf("SaveServerSettings: %v", err)
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
		if len(cfg.Vaults) != 1 {
			t.Errorf("expected 1 vault, got %d", len(cfg.Vaults))
		}
		if len(cfg.Ingesters) != 2 {
			t.Errorf("expected 2 ingesters, got %d", len(cfg.Ingesters))
		}
		if cfg.Auth.JWTSecret != "s3cret" {
			t.Errorf("expected Auth.JWTSecret %q, got %q", "s3cret", cfg.Auth.JWTSecret)
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		nope := uuid.Must(uuid.NewV7())

		rp, err := s.GetRotationPolicy(ctx, nope)
		if err != nil {
			t.Fatalf("GetRotationPolicy: %v", err)
		}
		if rp != nil {
			t.Errorf("expected nil, got %+v", rp)
		}

		v, err := s.GetVault(ctx, nope)
		if err != nil {
			t.Fatalf("GetVault: %v", err)
		}
		if v != nil {
			t.Errorf("expected nil, got %+v", v)
		}

		ing, err := s.GetIngester(ctx, nope)
		if err != nil {
			t.Fatalf("GetIngester: %v", err)
		}
		if ing != nil {
			t.Errorf("expected nil, got %+v", ing)
		}
	})

	// Server Settings
	t.Run("LoadSaveServerSettings", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		wantAuth := config.AuthConfig{JWTSecret: "test-secret"}
		wantQuery := config.QueryConfig{Timeout: "30s"}
		if err := s.SaveServerSettings(ctx, config.ServerSettings{Auth: wantAuth, Query: wantQuery}); err != nil {
			t.Fatalf("Save: %v", err)
		}

		ss, err := s.LoadServerSettings(ctx)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if ss.Auth.JWTSecret != wantAuth.JWTSecret {
			t.Errorf("JWTSecret: got %q, want %q", ss.Auth.JWTSecret, wantAuth.JWTSecret)
		}
		if ss.Query.Timeout != wantQuery.Timeout {
			t.Errorf("Timeout: got %q, want %q", ss.Query.Timeout, wantQuery.Timeout)
		}
	})

	t.Run("ServerSettingsUpsert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		if err := s.SaveServerSettings(ctx, config.ServerSettings{Auth: config.AuthConfig{JWTSecret: "old"}}); err != nil {
			t.Fatalf("Save: %v", err)
		}

		if err := s.SaveServerSettings(ctx, config.ServerSettings{Auth: config.AuthConfig{JWTSecret: "new-secret"}}); err != nil {
			t.Fatalf("Save upsert: %v", err)
		}

		ss, err := s.LoadServerSettings(ctx)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if ss.Auth.JWTSecret != "new-secret" {
			t.Errorf("expected %q, got %q", "new-secret", ss.Auth.JWTSecret)
		}
	})

	// Users

	t.Run("CreateGetUser", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		now := time.Now().UTC().Truncate(time.Second)
		user := config.User{
			ID:           id,
			Username:     "alice",
			PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$fakesalt$fakehash",
			Role:         "admin",
			CreatedAt:    now,
			UpdatedAt:    now,
		}

		if err := s.CreateUser(ctx, user); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		got, err := s.GetUser(ctx, id)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got == nil {
			t.Fatal("expected user, got nil")
		}
		if got.Username != "alice" {
			t.Errorf("Username: expected %q, got %q", "alice", got.Username)
		}
		if got.PasswordHash != user.PasswordHash {
			t.Errorf("PasswordHash: expected %q, got %q", user.PasswordHash, got.PasswordHash)
		}
		if got.Role != "admin" {
			t.Errorf("Role: expected %q, got %q", "admin", got.Role)
		}
		if got.CreatedAt.Truncate(time.Second) != now {
			t.Errorf("CreatedAt: expected %v, got %v", now, got.CreatedAt)
		}
		if got.UpdatedAt.Truncate(time.Second) != now {
			t.Errorf("UpdatedAt: expected %v, got %v", now, got.UpdatedAt)
		}

		// GetUserByUsername should also find the user.
		byName, err := s.GetUserByUsername(ctx, "alice")
		if err != nil {
			t.Fatalf("GetUserByUsername: %v", err)
		}
		if byName == nil {
			t.Fatal("expected user by username, got nil")
		}
		if byName.ID != id {
			t.Errorf("GetUserByUsername ID: expected %s, got %s", id, byName.ID)
		}
	})

	t.Run("CreateUserDuplicate", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		user := config.User{
			ID:           newID(),
			Username:     "bob",
			PasswordHash: "hash1",
			Role:         "user",
			CreatedAt:    now,
			UpdatedAt:    now,
		}

		if err := s.CreateUser(ctx, user); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		// Second create with same username should fail.
		user2 := user
		user2.ID = newID() // different ID, same username
		err := s.CreateUser(ctx, user2)
		if err == nil {
			t.Fatal("expected error creating duplicate user, got nil")
		}
	})

	t.Run("GetUserNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		got, err := s.GetUser(ctx, uuid.Must(uuid.NewV7()))
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil for non-existent user, got %+v", got)
		}
	})

	t.Run("GetUserByUsernameNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		got, err := s.GetUserByUsername(ctx, "nobody")
		if err != nil {
			t.Fatalf("GetUserByUsername: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil for non-existent user, got %+v", got)
		}
	})

	t.Run("UpdatePassword", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		now := time.Now().UTC()
		user := config.User{
			ID:           id,
			Username:     "carol",
			PasswordHash: "old-hash",
			Role:         "user",
			CreatedAt:    now,
			UpdatedAt:    now,
		}

		if err := s.CreateUser(ctx, user); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		if err := s.UpdatePassword(ctx, id, "new-hash"); err != nil {
			t.Fatalf("UpdatePassword: %v", err)
		}

		got, err := s.GetUser(ctx, id)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got.PasswordHash != "new-hash" {
			t.Errorf("PasswordHash: expected %q, got %q", "new-hash", got.PasswordHash)
		}
		// UpdatedAt should have changed.
		if !got.UpdatedAt.After(now.Add(-time.Second)) {
			t.Errorf("UpdatedAt should be recent, got %v", got.UpdatedAt)
		}
	})

	t.Run("UpdatePasswordNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		err := s.UpdatePassword(ctx, uuid.Must(uuid.NewV7()), "hash")
		if err == nil {
			t.Fatal("expected error updating non-existent user, got nil")
		}
	})

	t.Run("CountUsers", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		count, err := s.CountUsers(ctx)
		if err != nil {
			t.Fatalf("CountUsers: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 users, got %d", count)
		}

		now := time.Now().UTC()
		if err := s.CreateUser(ctx, config.User{
			ID: newID(), Username: "u1", PasswordHash: "h1", Role: "admin",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser u1: %v", err)
		}

		count, err = s.CountUsers(ctx)
		if err != nil {
			t.Fatalf("CountUsers: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 user, got %d", count)
		}

		if err := s.CreateUser(ctx, config.User{
			ID: newID(), Username: "u2", PasswordHash: "h2", Role: "user",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser u2: %v", err)
		}

		count, err = s.CountUsers(ctx)
		if err != nil {
			t.Fatalf("CountUsers: %v", err)
		}
		if count != 2 {
			t.Fatalf("expected 2 users, got %d", count)
		}
	})

	t.Run("ListUsers", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// Empty vault returns empty list.
		users, err := s.ListUsers(ctx)
		if err != nil {
			t.Fatalf("ListUsers: %v", err)
		}
		if len(users) != 0 {
			t.Fatalf("expected 0 users, got %d", len(users))
		}

		now := time.Now().UTC()
		if err := s.CreateUser(ctx, config.User{
			ID: newID(), Username: "alice", PasswordHash: "h1", Role: "admin",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser alice: %v", err)
		}
		if err := s.CreateUser(ctx, config.User{
			ID: newID(), Username: "bob", PasswordHash: "h2", Role: "user",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser bob: %v", err)
		}

		users, err = s.ListUsers(ctx)
		if err != nil {
			t.Fatalf("ListUsers: %v", err)
		}
		if len(users) != 2 {
			t.Fatalf("expected 2 users, got %d", len(users))
		}
	})

	t.Run("UpdateUserRole", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		now := time.Now().UTC()
		if err := s.CreateUser(ctx, config.User{
			ID: id, Username: "alice", PasswordHash: "h1", Role: "user",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		if err := s.UpdateUserRole(ctx, id, "admin"); err != nil {
			t.Fatalf("UpdateUserRole: %v", err)
		}

		got, err := s.GetUser(ctx, id)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got.Role != "admin" {
			t.Errorf("Role: expected %q, got %q", "admin", got.Role)
		}
	})

	t.Run("UpdateUserRoleNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		err := s.UpdateUserRole(ctx, uuid.Must(uuid.NewV7()), "admin")
		if err == nil {
			t.Fatal("expected error updating non-existent user, got nil")
		}
	})

	t.Run("DeleteUser", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		now := time.Now().UTC()
		if err := s.CreateUser(ctx, config.User{
			ID: id, Username: "alice", PasswordHash: "h1", Role: "admin",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		if err := s.DeleteUser(ctx, id); err != nil {
			t.Fatalf("DeleteUser: %v", err)
		}

		got, err := s.GetUser(ctx, id)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}

		count, err := s.CountUsers(ctx)
		if err != nil {
			t.Fatalf("CountUsers: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 users after delete, got %d", count)
		}
	})

	t.Run("DeleteUserNonExistent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		err := s.DeleteUser(ctx, uuid.Must(uuid.NewV7()))
		if err == nil {
			t.Fatal("expected error deleting non-existent user, got nil")
		}
	})

	t.Run("UserPreferences", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		now := time.Now().UTC()
		if err := s.CreateUser(ctx, config.User{
			ID: id, Username: "alice", PasswordHash: "h1", Role: "user",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		// Initially nil.
		got, err := s.GetUserPreferences(ctx, id)
		if err != nil {
			t.Fatalf("GetUserPreferences: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil preferences initially, got %q", *got)
		}

		// Put preferences.
		prefs := `{"theme":"dark","saved_queries":[{"name":"errors","query":"level=error"}]}`
		if err := s.PutUserPreferences(ctx, id, prefs); err != nil {
			t.Fatalf("PutUserPreferences: %v", err)
		}

		got, err = s.GetUserPreferences(ctx, id)
		if err != nil {
			t.Fatalf("GetUserPreferences: %v", err)
		}
		if got == nil {
			t.Fatal("expected preferences, got nil")
		}
		if *got != prefs {
			t.Errorf("expected %q, got %q", prefs, *got)
		}

		// Non-existent user returns nil.
		got, err = s.GetUserPreferences(ctx, uuid.Must(uuid.NewV7()))
		if err != nil {
			t.Fatalf("GetUserPreferences non-existent: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil for non-existent user, got %q", *got)
		}

		// Put for non-existent user errors.
		err = s.PutUserPreferences(ctx, uuid.Must(uuid.NewV7()), `{}`)
		if err == nil {
			t.Fatal("expected error putting preferences for non-existent user")
		}

		// Preferences are deleted with the user.
		if err := s.DeleteUser(ctx, id); err != nil {
			t.Fatalf("DeleteUser: %v", err)
		}
		got, err = s.GetUserPreferences(ctx, id)
		if err != nil {
			t.Fatalf("GetUserPreferences after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after user delete, got %q", *got)
		}
	})

	t.Run("InvalidateTokens", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		id := newID()
		now := time.Now().UTC().Truncate(time.Second)
		if err := s.CreateUser(ctx, config.User{
			ID: id, Username: "alice", PasswordHash: "h1", Role: "user",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		// Initially zero.
		user, err := s.GetUser(ctx, id)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if !user.TokenInvalidatedAt.IsZero() {
			t.Errorf("expected zero TokenInvalidatedAt, got %v", user.TokenInvalidatedAt)
		}

		// Invalidate.
		invalidAt := time.Now().UTC().Truncate(time.Second)
		if err := s.InvalidateTokens(ctx, id, invalidAt); err != nil {
			t.Fatalf("InvalidateTokens: %v", err)
		}

		user, err = s.GetUser(ctx, id)
		if err != nil {
			t.Fatalf("GetUser after invalidate: %v", err)
		}
		got := user.TokenInvalidatedAt.Truncate(time.Second)
		if !got.Equal(invalidAt) {
			t.Errorf("TokenInvalidatedAt: expected %v, got %v", invalidAt, got)
		}

		// Non-existent user returns error.
		if err := s.InvalidateTokens(ctx, uuid.Must(uuid.NewV7()), invalidAt); err == nil {
			t.Fatal("expected error invalidating non-existent user")
		}
	})

	// Refresh tokens

	t.Run("CreateGetRefreshToken", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		tokenID := newID()
		userID := newID()
		now := time.Now().UTC().Truncate(time.Second)
		rt := config.RefreshToken{
			ID:        tokenID,
			UserID:    userID,
			TokenHash: "sha256-hash-abc123",
			ExpiresAt: now.Add(7 * 24 * time.Hour),
			CreatedAt: now,
		}

		if err := s.CreateRefreshToken(ctx, rt); err != nil {
			t.Fatalf("CreateRefreshToken: %v", err)
		}

		got, err := s.GetRefreshTokenByHash(ctx, "sha256-hash-abc123")
		if err != nil {
			t.Fatalf("GetRefreshTokenByHash: %v", err)
		}
		if got == nil {
			t.Fatal("expected refresh token, got nil")
		}
		if got.ID != tokenID {
			t.Errorf("ID: expected %s, got %s", tokenID, got.ID)
		}
		if got.UserID != userID {
			t.Errorf("UserID: expected %s, got %s", userID, got.UserID)
		}
		if got.TokenHash != "sha256-hash-abc123" {
			t.Errorf("TokenHash: expected %q, got %q", "sha256-hash-abc123", got.TokenHash)
		}
		if got.ExpiresAt.Truncate(time.Second) != rt.ExpiresAt.Truncate(time.Second) {
			t.Errorf("ExpiresAt: expected %v, got %v", rt.ExpiresAt, got.ExpiresAt)
		}
		if got.CreatedAt.Truncate(time.Second) != now {
			t.Errorf("CreatedAt: expected %v, got %v", now, got.CreatedAt)
		}
	})

	t.Run("GetRefreshTokenByHashNotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		got, err := s.GetRefreshTokenByHash(ctx, "nonexistent-hash")
		if err != nil {
			t.Fatalf("GetRefreshTokenByHash: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil for unknown hash, got %+v", got)
		}
	})

	t.Run("DeleteRefreshToken", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		tokenID := newID()
		rt := config.RefreshToken{
			ID:        tokenID,
			UserID:    newID(),
			TokenHash: "hash-to-delete",
			ExpiresAt: time.Now().Add(time.Hour),
			CreatedAt: time.Now(),
		}

		if err := s.CreateRefreshToken(ctx, rt); err != nil {
			t.Fatalf("CreateRefreshToken: %v", err)
		}

		if err := s.DeleteRefreshToken(ctx, tokenID); err != nil {
			t.Fatalf("DeleteRefreshToken: %v", err)
		}

		got, err := s.GetRefreshTokenByHash(ctx, "hash-to-delete")
		if err != nil {
			t.Fatalf("GetRefreshTokenByHash after delete: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil after delete, got %+v", got)
		}
	})

	t.Run("DeleteUserRefreshTokens", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		userID := newID()
		otherUserID := newID()

		// Create two tokens for the target user and one for another.
		for i, hash := range []string{"user-hash-1", "user-hash-2"} {
			rt := config.RefreshToken{
				ID:        newID(),
				UserID:    userID,
				TokenHash: hash,
				ExpiresAt: time.Now().Add(time.Hour),
				CreatedAt: time.Now().Add(time.Duration(i) * time.Second),
			}
			if err := s.CreateRefreshToken(ctx, rt); err != nil {
				t.Fatalf("CreateRefreshToken %d: %v", i, err)
			}
		}
		otherRT := config.RefreshToken{
			ID:        newID(),
			UserID:    otherUserID,
			TokenHash: "other-user-hash",
			ExpiresAt: time.Now().Add(time.Hour),
			CreatedAt: time.Now(),
		}
		if err := s.CreateRefreshToken(ctx, otherRT); err != nil {
			t.Fatalf("CreateRefreshToken other: %v", err)
		}

		// Delete all tokens for the target user.
		if err := s.DeleteUserRefreshTokens(ctx, userID); err != nil {
			t.Fatalf("DeleteUserRefreshTokens: %v", err)
		}

		// Target user's tokens should be gone.
		for _, hash := range []string{"user-hash-1", "user-hash-2"} {
			got, err := s.GetRefreshTokenByHash(ctx, hash)
			if err != nil {
				t.Fatalf("GetRefreshTokenByHash %q: %v", hash, err)
			}
			if got != nil {
				t.Errorf("expected nil for deleted user token %q, got %+v", hash, got)
			}
		}

		// Other user's token should still exist.
		got, err := s.GetRefreshTokenByHash(ctx, "other-user-hash")
		if err != nil {
			t.Fatalf("GetRefreshTokenByHash other: %v", err)
		}
		if got == nil {
			t.Fatal("expected other user's token to survive, got nil")
		}
	})

	t.Run("ListRefreshTokens", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// Empty list.
		tokens, err := s.ListRefreshTokens(ctx)
		if err != nil {
			t.Fatalf("ListRefreshTokens empty: %v", err)
		}
		if len(tokens) != 0 {
			t.Fatalf("expected 0 tokens, got %d", len(tokens))
		}

		// Create two tokens.
		rt1 := config.RefreshToken{
			ID:        newID(),
			UserID:    newID(),
			TokenHash: "list-hash-1",
			ExpiresAt: time.Now().Add(time.Hour).Truncate(time.Second).UTC(),
			CreatedAt: time.Now().Truncate(time.Second).UTC(),
		}
		rt2 := config.RefreshToken{
			ID:        newID(),
			UserID:    newID(),
			TokenHash: "list-hash-2",
			ExpiresAt: time.Now().Add(2 * time.Hour).Truncate(time.Second).UTC(),
			CreatedAt: time.Now().Truncate(time.Second).UTC(),
		}
		if err := s.CreateRefreshToken(ctx, rt1); err != nil {
			t.Fatalf("CreateRefreshToken 1: %v", err)
		}
		if err := s.CreateRefreshToken(ctx, rt2); err != nil {
			t.Fatalf("CreateRefreshToken 2: %v", err)
		}

		tokens, err = s.ListRefreshTokens(ctx)
		if err != nil {
			t.Fatalf("ListRefreshTokens: %v", err)
		}
		if len(tokens) != 2 {
			t.Fatalf("expected 2 tokens, got %d", len(tokens))
		}

		// Verify both tokens are present (order may vary).
		found := map[string]bool{}
		for _, t := range tokens {
			found[t.TokenHash] = true
		}
		if !found["list-hash-1"] || !found["list-hash-2"] {
			t.Errorf("expected both tokens, found: %v", found)
		}
	})

	t.Run("UsersNotInLoad", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		now := time.Now().UTC()
		if err := s.CreateUser(ctx, config.User{
			ID: newID(), Username: "admin", PasswordHash: "hash", Role: "admin",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateUser: %v", err)
		}

		// Users are operational data — Load should NOT return them.
		// With only a user and no config entities, Load should return nil.
		cfg, err := s.Load(ctx)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg != nil {
			t.Errorf("expected nil config (users are not config entities), got %+v", cfg)
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

func assertUUIDPtr(t *testing.T, name string, got *uuid.UUID, want uuid.UUID) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: expected %s, got nil", name, want)
		return
	}
	if *got != want {
		t.Errorf("%s: expected %s, got %s", name, want, *got)
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
