package query

import (
	"testing"
	"time"

	"gastrolog/internal/querylang"
)

func FuzzQueryNormalize(f *testing.F) {
	// Seed corpus: combinations of token count, kv count, and boolExpr presence.
	// We encode as: nTokens, nKV, queryStr (for BoolExpr), startUnix, endUnix, reverse.
	f.Add(uint8(0), uint8(0), "", int64(0), int64(0), false)
	f.Add(uint8(1), uint8(0), "", int64(1000000000), int64(2000000000), false)
	f.Add(uint8(0), uint8(1), "", int64(0), int64(0), true)
	f.Add(uint8(2), uint8(2), "", int64(1000000000), int64(2000000000), true)
	f.Add(uint8(0), uint8(0), "error AND warn", int64(0), int64(0), false)
	f.Add(uint8(3), uint8(0), "", int64(0), int64(0), false)
	f.Add(uint8(0), uint8(3), "", int64(0), int64(0), false)

	f.Fuzz(func(t *testing.T, nTokens, nKV uint8, queryStr string, startUnix, endUnix int64, reverse bool) {
		q := Query{
			IsReverse: reverse,
		}

		// Clamp to avoid absurd allocations.
		nt := int(nTokens % 16)
		nk := int(nKV % 16)

		if startUnix != 0 {
			q.Start = time.Unix(startUnix, 0)
		}
		if endUnix != 0 {
			q.End = time.Unix(endUnix, 0)
		}

		for i := range nt {
			q.Tokens = append(q.Tokens, string(rune('a'+i%26)))
		}
		for i := range nk {
			q.KV = append(q.KV, KeyValueFilter{
				Key:   string(rune('k' + i%10)),
				Value: string(rune('v' + i%10)),
			})
		}

		if queryStr != "" {
			expr, err := querylang.Parse(queryStr)
			if err == nil {
				q.BoolExpr = expr
			}
		}

		// Normalize must not panic.
		normalized := q.Normalize()

		// String must not panic.
		_ = normalized.String()

		// TimeBounds must not panic.
		_, _ = normalized.TimeBounds()

		// Reverse must not panic.
		_ = normalized.Reverse()
	})
}
