package cli

import (
	"fmt"

	"gastrolog/internal/system"
)

// parseWriteModel validates and normalizes a CLI write-model flag value.
func parseWriteModel(s string) (string, error) {
	switch s {
	case "", string(system.VaultWriteModelChunkAppend):
		return string(system.VaultWriteModelChunkAppend), nil
	case string(system.VaultWriteModelSequenced):
		return string(system.VaultWriteModelSequenced), nil
	default:
		return "", fmt.Errorf("invalid write model %q (valid: chunk_append, sequenced)", s)
	}
}

// writeModelDisplay renders a vault config write_model for operator output.
func writeModelDisplay(s string) string {
	cfg := system.VaultConfig{WriteModel: s}
	if cfg.UsesSequencedWriteModel() {
		return string(system.VaultWriteModelSequenced)
	}
	if s == "" {
		return "chunk_append (default)"
	}
	return string(system.VaultWriteModelChunkAppend)
}

// usesSequencedWriteModel reports whether config opts into sequenced ingest.
func usesSequencedWriteModel(s string) bool {
	cfg := system.VaultConfig{WriteModel: s}
	return cfg.UsesSequencedWriteModel()
}
