package orchestrator

import (
	"gastrolog/internal/glid"
	"testing"

	"gastrolog/internal/chunk"

)

func FuzzCompileFilter(f *testing.F) {
	f.Add("")
	f.Add("*")
	f.Add("+")
	f.Add(`env="prod"`)
	f.Add(`env="prod" AND level="error"`)
	f.Add(`env="prod" OR env="staging"`)
	f.Add(`NOT env="dev"`)
	f.Add(`host="web-*"`)
	f.Add(`level="error" AND NOT env="dev"`)
	f.Add(`(env="prod" OR env="staging") AND level="error"`)
	f.Add(`key_exists("host")`)
	f.Add(`value_exists("error")`)
	f.Add("bogus expression ][")
	f.Add(`env = "prod"`)
	f.Add(`a="b" AND c="d" OR e="f"`)
	f.Add("error")
	f.Add(`/regex/`)
	f.Add(`env="prod" AND /pattern/`)

	f.Fuzz(func(t *testing.T, filter string) {
		vid := glid.New()
		// Must not panic on any input.
		_, _ = CompileFilter(vid, filter)
	})
}

func FuzzFilterSetMatch(f *testing.F) {
	// Seed corpus: (filter expression, attribute key, attribute value)
	f.Add("*", "env", "prod")
	f.Add("+", "env", "prod")
	f.Add("", "env", "prod")
	f.Add(`env="prod"`, "env", "prod")
	f.Add(`env="prod"`, "env", "staging")
	f.Add(`env="prod"`, "level", "error")
	f.Add(`env="prod" AND level="error"`, "env", "prod")
	f.Add(`NOT env="dev"`, "env", "prod")
	f.Add(`NOT env="dev"`, "env", "dev")
	f.Add(`key_exists("host")`, "host", "web-1")
	f.Add(`key_exists("host")`, "env", "prod")

	f.Fuzz(func(t *testing.T, filter, key, value string) {
		vid := glid.New()
		cf, err := CompileFilter(vid, filter)
		if err != nil {
			return // invalid filter expressions are expected
		}

		fs := NewFilterSet([]*CompiledFilter{cf})

		attrs := chunk.Attributes{}
		if key != "" {
			attrs[key] = value
		}

		// Must not panic on any input.
		_ = fs.Match(attrs)
		_ = fs.MatchWithNode(attrs)
	})
}
