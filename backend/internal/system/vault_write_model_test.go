package system

import "testing"

func TestVaultConfigResolveWriteModel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want VaultWriteModel
	}{
		{in: "", want: VaultWriteModelChunkAppend},
		{in: "chunk_append", want: VaultWriteModelChunkAppend},
		{in: "sequenced", want: VaultWriteModelSequenced},
		{in: "bogus", want: VaultWriteModelChunkAppend},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			v := VaultConfig{WriteModel: tt.in}
			if got := v.ResolveWriteModel(); got != tt.want {
				t.Fatalf("ResolveWriteModel() = %q, want %q", got, tt.want)
			}
			if v.UsesSequencedWriteModel() != (tt.want == VaultWriteModelSequenced) {
				t.Fatalf("UsesSequencedWriteModel() = %v, want %v", v.UsesSequencedWriteModel(), tt.want == VaultWriteModelSequenced)
			}
		})
	}
}

func TestVaultConfigValidateWriteModel(t *testing.T) {
	t.Parallel()
	for _, wm := range []string{"sequenced", "chunk_append", ""} {
		if err := (VaultConfig{Name: "ok", WriteModel: wm}).ValidateWriteModel(); err != nil {
			t.Fatalf("writeModel %q: %v", wm, err)
		}
	}
	for _, wm := range []string{"v1", "v2", "v3"} {
		if err := (VaultConfig{Name: "bad", WriteModel: wm}).ValidateWriteModel(); err == nil {
			t.Fatalf("expected error for writeModel %q", wm)
		}
	}
}
