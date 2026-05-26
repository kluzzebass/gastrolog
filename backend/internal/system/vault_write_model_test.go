package system

import "testing"

func TestVaultConfigResolveWriteModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want VaultWriteModel
	}{
		{in: "", want: VaultWriteModelV1},
		{in: "v1", want: VaultWriteModelV1},
		{in: "v2", want: VaultWriteModelV2},
		{in: "bogus", want: VaultWriteModelV1},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			v := VaultConfig{WriteModel: tt.in}
			if got := v.ResolveWriteModel(); got != tt.want {
				t.Fatalf("ResolveWriteModel() = %q, want %q", got, tt.want)
			}
			if v.UsesV2WriteModel() != (tt.want == VaultWriteModelV2) {
				t.Fatalf("UsesV2WriteModel() = %v, want %v", v.UsesV2WriteModel(), tt.want == VaultWriteModelV2)
			}
		})
	}
}

func TestVaultConfigValidateWriteModel(t *testing.T) {
	t.Parallel()
	if err := (VaultConfig{Name: "ok", WriteModel: "v2"}).ValidateWriteModel(); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if err := (VaultConfig{Name: "bad", WriteModel: "v3"}).ValidateWriteModel(); err == nil {
		t.Fatal("expected error for unknown write model")
	}
}
