package server_test

// Config writes are read-modify-write: the handler reads the current entity
// to preserve what the request omits, and lists siblings to reject a
// duplicate name. The handler runs on whichever node received the request,
// which may not have applied the write it is about to modify. These tests
// hold a node stale on purpose and require that neither the read nor the
// list mistakes replication lag for absence.

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// A service created on one node and edited on another that has not applied
// it yet must keep its credentials. Reads redact them, so the edit carries
// none; a merge against an empty local view stores the service with its
// credentials wiped and locks the cluster out of sealed chunks already in
// the object store.
func TestMultiNode_PutCloudServiceOnStaleNodeKeepsCredentials(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithManualReplication())

	id := glid.New()
	coord := mnSystemClientFor(t, h, "coord")
	if err := putCloudService(ctx, coord, s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create on coord: %v", err)
	}

	// Premise: the credentials are really stored, and data-1 really is
	// stale. Without both, the assertion below passes against broken code.
	stored, err := h.store(t, "coord").GetCloudService(ctx, id)
	if err != nil || stored == nil {
		t.Fatalf("load service on coord: %v", err)
	}
	if stored.AccessKey != "AKIAEXAMPLE" || stored.SecretKey != "s3cr3t-key-value" {
		t.Fatalf("create did not store credentials: access=%q secret=%q", stored.AccessKey, stored.SecretKey)
	}
	if s, _ := h.store(t, "data-1").GetCloudService(ctx, id); s != nil {
		t.Fatal("data-1 already applied the service; the test cannot exercise a stale view")
	}

	// What a UI sends back after reading the service: every field it was
	// given, which never included credentials, plus one ordinary edit.
	edit := s3ServiceWithCredentials(id.ToProto(), "archive")
	edit.AccessKey = ""
	edit.SecretKey = ""
	edit.Region = "eu-west-1"
	if err := putCloudService(ctx, mnSystemClientFor(t, h, "data-1"), edit); err != nil {
		t.Fatalf("edit on stale data-1: %v", err)
	}

	h.deliverAll()
	after, err := h.store(t, "coord").GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload service: %v", err)
	}
	if after.AccessKey != "AKIAEXAMPLE" || after.SecretKey != "s3cr3t-key-value" {
		t.Fatalf("edit on a stale node wiped credentials: access=%q secret=%q", after.AccessKey, after.SecretKey)
	}
	if after.Region != "eu-west-1" {
		t.Fatalf("edit did not apply: region=%q", after.Region)
	}
}

// The duplicate-name check has the same shape and the same exposure: a node
// that has not applied the create sees no conflict and lets a second entity
// take a name the cluster already gave away.
func TestMultiNode_DuplicateNameRejectedByStaleNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithManualReplication())

	taken := glid.New()
	if err := putCloudService(ctx, mnSystemClientFor(t, h, "coord"),
		s3ServiceWithCredentials(taken.ToProto(), "archive")); err != nil {
		t.Fatalf("create on coord: %v", err)
	}
	if s, _ := h.store(t, "data-1").GetCloudService(ctx, taken); s != nil {
		t.Fatal("data-1 already applied the service; the test cannot exercise a stale view")
	}

	err := putCloudService(ctx, mnSystemClientFor(t, h, "data-1"),
		s3ServiceWithCredentials(glid.New().ToProto(), "archive"))
	if err == nil {
		t.Fatal("stale node accepted a name the cluster had already given away")
	}
	if got := connect.CodeOf(err); got != connect.CodeAlreadyExists {
		t.Fatalf("duplicate name rejected with %v, want %v (%v)", got, connect.CodeAlreadyExists, err)
	}

	// The rejection must not have been a blanket refusal: a free name still
	// goes through from the same node.
	if err := putCloudService(ctx, mnSystemClientFor(t, h, "data-1"),
		s3ServiceWithCredentials(glid.New().ToProto(), "archive-2")); err != nil {
		t.Fatalf("free name rejected on data-1: %v", err)
	}
}

// A node whose view cannot be brought current must refuse the write. Storing
// against a view known to be stale is how the credentials get wiped, so a
// failed catch-up is a failed request, not a silent fallback to local state.
func TestMultiNode_ConfigWriteFailsWhenCatchUpFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithManualReplication())

	id := glid.New()
	if err := putCloudService(ctx, mnSystemClientFor(t, h, "coord"),
		s3ServiceWithCredentials(id.ToProto(), "archive")); err != nil {
		t.Fatalf("create on coord: %v", err)
	}

	h.stallNode(t, "data-1")

	edit := s3ServiceWithCredentials(id.ToProto(), "archive")
	edit.AccessKey = ""
	edit.SecretKey = ""
	if err := putCloudService(ctx, mnSystemClientFor(t, h, "data-1"), edit); err == nil {
		t.Fatal("stalled node accepted a write it could not base on current state")
	}

	// Nothing was written, so the credentials the cluster holds are intact.
	h.resumeNode(t, "data-1")
	h.deliverAll()
	after, err := h.store(t, "coord").GetCloudService(ctx, id)
	if err != nil || after == nil {
		t.Fatalf("reload service: %v", err)
	}
	if after.AccessKey != "AKIAEXAMPLE" || after.SecretKey != "s3cr3t-key-value" {
		t.Fatalf("refused write still wiped credentials: access=%q secret=%q", after.AccessKey, after.SecretKey)
	}
}

// Entities other than cloud services share the helper, so the guarantee is
// checked on a second kind: a vault name taken on one node is not free on a
// node that has not applied it.
func TestMultiNode_StaleNodeRejectsTakenVaultName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := setupMultiNode(t, []string{"coord", "data-1"}, WithManualReplication())

	name := "shared-name"
	if err := h.store(t, "coord").PutVault(ctx, system.VaultConfig{
		ID: glid.New(), Name: name, Type: "memory",
	}); err != nil {
		t.Fatalf("create vault on coord: %v", err)
	}
	vaults, err := h.store(t, "data-1").ListVaults(ctx)
	if err != nil {
		t.Fatalf("list vaults on data-1: %v", err)
	}
	for _, v := range vaults {
		if v.Name == name {
			t.Fatal("data-1 already applied the vault; the test cannot exercise a stale view")
		}
	}

	_, err = mnSystemClientFor(t, h, "data-1").PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
		Config: &gastrologv1.VaultConfig{Id: glid.New().ToProto(), Name: name, Type: gastrologv1.VaultType_VAULT_TYPE_MEMORY},
	}))
	if err == nil {
		t.Fatal("stale node accepted a vault name the cluster had already given away")
	}
	if got := connect.CodeOf(err); got != connect.CodeAlreadyExists {
		t.Fatalf("duplicate vault name rejected with %v, want %v (%v)", got, connect.CodeAlreadyExists, err)
	}
}
