package query

import (
	"gastrolog/internal/glid"
	"testing"

	"gastrolog/internal/querylang"
)

func FuzzExtractVaultFilter(f *testing.F) {
	// Seed corpus: query strings that exercise various vault filter shapes.
	seeds := []string{
		"error",
		`vault_id=` + glid.New().String(),
		`vault_id=` + glid.New().String() + ` AND error`,
		`vault_id=` + glid.New().String() + ` OR vault_id=` + glid.New().String(),
		`NOT vault_id=` + glid.New().String(),
		`(vault_id=` + glid.New().String() + ` AND error) OR (vault_id=` + glid.New().String() + ` AND warn)`,
		`level=error AND status=500`,
		`vault_id=not-a-uuid`,
		"error AND warn AND NOT debug",
		`(a OR b) AND vault_id=` + glid.New().String(),
	}

	for _, s := range seeds {
		f.Add(s)
	}

	// Fixed set of "all vaults" for extraction.
	allVaults := []glid.GLID{
		glid.MustParse("00000000-0000-0000-0000-000000000001"),
		glid.MustParse("00000000-0000-0000-0000-000000000002"),
		glid.MustParse("00000000-0000-0000-0000-000000000003"),
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
