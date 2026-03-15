package query

import (
	"testing"

	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

func FuzzExtractVaultFilter(f *testing.F) {
	// Seed corpus: query strings that exercise various vault filter shapes.
	seeds := []string{
		"error",
		`vault_id=` + uuid.New().String(),
		`vault_id=` + uuid.New().String() + ` AND error`,
		`vault_id=` + uuid.New().String() + ` OR vault_id=` + uuid.New().String(),
		`NOT vault_id=` + uuid.New().String(),
		`(vault_id=` + uuid.New().String() + ` AND error) OR (vault_id=` + uuid.New().String() + ` AND warn)`,
		`level=error AND status=500`,
		`vault_id=not-a-uuid`,
		"error AND warn AND NOT debug",
		`(a OR b) AND vault_id=` + uuid.New().String(),
	}

	for _, s := range seeds {
		f.Add(s)
	}

	// Fixed set of "all vaults" for extraction.
	allVaults := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
	}

	f.Fuzz(func(t *testing.T, input string) {
		expr, err := querylang.Parse(input)
		if err != nil {
			return // unparseable, skip
		}

		// ExtractVaultFilter must not panic on any valid AST.
		vaults, remaining := ExtractVaultFilter(expr, allVaults)

		// Basic sanity checks (must not panic).
		_ = len(vaults)
		if remaining != nil {
			_ = remaining.String()
		}
	})
}
