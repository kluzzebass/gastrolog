package server_test

// Coverage for the vault's disk-claim bound, which lives on
// RetentionPolicyConfig.max_size and means BOTH things at once: it drains
// oldest sealed chunks past the bound, and — when the policy's refuse flag
// is explicitly on (refuse defaults off) — also refuses admission while the
// vault's local claim is at/over it. PutRetentionPolicy parse-checks it —
// must parse, must be > 0 when set; an absent max_size is not defaulted
// here (no per-policy stamping) — the default floor is applied
// downstream by the disk-guard resolver (orchestrator.resolveVaultSizeBound)
// only when NO attached policy carries a bound at all, and that floor is
// refuse-only (a default must never destroy data).

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

func getStoredVault(t *testing.T, store system.Store, id glid.GLID) system.VaultConfig {
	t.Helper()
	v, err := store.GetVault(context.Background(), id)
	if err != nil {
		t.Fatalf("GetVault: %v", err)
	}
	if v == nil {
		t.Fatalf("vault %s not found", id)
	}
	return *v
}

func getStoredRetentionPolicy(t *testing.T, store system.Store, id glid.GLID) system.RetentionPolicyConfig {
	t.Helper()
	p, err := store.GetRetentionPolicy(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRetentionPolicy: %v", err)
	}
	if p == nil {
		t.Fatalf("retention policy %s not found", id)
	}
	return *p
}

func strPtrVal(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// An unset max-size is left nil — no default is stamped onto the policy.
// This is a deliberate divergence from the old vault max-size behavior: the
// creation-default floor now lives in the disk-guard resolver, not on the
// stored config, because it must apply per-vault (across possibly zero
// attached policies), not per-policy.
func TestPutRetentionPolicyLeavesMaxSizeUnset(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:        id.Bytes(),
			Name:      "max-size-unset",
			MaxChunks: 10, // needs at least one condition to pass IsEmpty
			// MaxSize omitted → unset.
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	if got := getStoredRetentionPolicy(t, store, id).MaxSize; got != nil {
		t.Fatalf("stored max-size = %q, want nil (no default stamped)", strPtrVal(got))
	}
}

// An explicit "0" is a real error, not a silent accept-nothing — it would
// mean "no bound", the unrepresentable state this model exists to prevent,
// now that max_size is also the refuse bound.
func TestPutRetentionPolicyRejectsExplicitZeroMaxSize(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:      glid.New().Bytes(),
			Name:    "max-size-zero",
			MaxSize: "0",
		},
	}))
	if err == nil {
		t.Fatal("expected an error for an explicit max-size of 0, got nil")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}
}

// A set value is stored verbatim — the operator's own expression, echoed back.
func TestPutRetentionPolicyStoresMaxSizeVerbatim(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:      id.Bytes(),
			Name:    "max-size-verbatim",
			MaxSize: "100TiB", // effectively unlimited, said explicitly
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	if got := getStoredRetentionPolicy(t, store, id).MaxSize; got == nil || *got != "100TiB" {
		t.Fatalf("stored max-size = %q, want %q verbatim", strPtrVal(got), "100TiB")
	}
}

// Unparseable and percentage expressions are rejected at the write boundary,
// not at use. max_size is an absolute disk-claim bound, not a
// volume-relative threshold, so a percentage is just an unparseable size —
// no special-cased "percentage" message like the disk-free thresholds.
func TestPutRetentionPolicyRejectsUnparseableMaxSize(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	for _, bad := range []string{"gigabytes-please", "10%", "-5GB"} {
		_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
			Config: &gastrologv1.RetentionPolicyConfig{
				Id:      glid.New().Bytes(),
				Name:    "max-size-bad-" + bad,
				MaxSize: bad,
			},
		}))
		if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("max-size %q: want InvalidArgument, got %v", bad, err)
		}
	}
}

// TestPutRetentionPolicyUnparseableMaxSizeOnOtherwiseEmptyPolicyGetsParseError
// pins the validation ORDER: a policy that sets no maxAge/maxChunks and
// ONLY an unparseable maxSize must report the actual parse failure, not the
// generic "must set at least one" no-op-policy message. IsEmpty()'s
// positiveSize check treats an unparseable expression the same as absent,
// so the parse check has to run first or this case falls through to the
// wrong, less actionable error.
func TestPutRetentionPolicyUnparseableMaxSizeOnOtherwiseEmptyPolicyGetsParseError(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:      glid.New().Bytes(),
			Name:    "max-size-only-bad",
			MaxSize: "gigabytes-please",
			// No MaxAge or MaxChunks: otherwise entirely empty.
		},
	}))
	if err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
	if !strings.Contains(err.Error(), "max-size") {
		t.Fatalf("want the max-size parse error, got the wrong diagnostic: %v", err)
	}
	if strings.Contains(err.Error(), "must set at least one") {
		t.Fatalf("must not fall through to the generic empty-policy error: %v", err)
	}
}

// A valid size is accepted alongside another trigger.
func TestPutRetentionPolicyAcceptsValidMaxSizeWithAge(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:      id.Bytes(),
			Name:    "max-size-and-age",
			MaxAge:  "3d",
			MaxSize: "50GB",
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy: %v", err)
	}
	stored := getStoredRetentionPolicy(t, store, id)
	if stored.MaxSize == nil || *stored.MaxSize != "50GB" {
		t.Fatalf("stored max-size = %q, want %q", strPtrVal(stored.MaxSize), "50GB")
	}
	if stored.MaxAge == nil || *stored.MaxAge != "3d" {
		t.Fatalf("stored max-age = %v, want %q", stored.MaxAge, "3d")
	}
}

// A policy that sets ONLY max_size (no maxAge/maxChunks) is legal and
// meaningful — it drains oldest sealed chunks past the bound and refuses
// admission while over it. It must not be rejected by the empty-policy
// check.
func TestPutRetentionPolicyAcceptsMaxSizeOnlyPolicy(t *testing.T) {
	client, store, _ := newConfigTestSetup(t)
	ctx := context.Background()
	id := glid.New()

	_, err := client.PutRetentionPolicy(ctx, connect.NewRequest(&gastrologv1.PutRetentionPolicyRequest{
		Config: &gastrologv1.RetentionPolicyConfig{
			Id:      id.Bytes(),
			Name:    "max-size-only",
			MaxSize: "50GB",
			// No MaxAge or MaxChunks.
		},
	}))
	if err != nil {
		t.Fatalf("PutRetentionPolicy must accept a max-size-only policy: %v", err)
	}
	stored := getStoredRetentionPolicy(t, store, id)
	if stored.MaxSize == nil || *stored.MaxSize != "50GB" {
		t.Fatalf("stored max-size = %q, want %q", strPtrVal(stored.MaxSize), "50GB")
	}
}
