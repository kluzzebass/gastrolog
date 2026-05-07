package orchestrator

import (
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

func FuzzCompileRoute(f *testing.F) {
	f.Add("")
	f.Add("*")
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

	f.Fuzz(func(t *testing.T, expr string) {
		// Must not panic on any input.
		_, _ = CompileRoute(glid.New(), "fuzz", 0, expr, []RouteDestination{{VaultID: glid.New()}}, "fanout")
	})
}

func FuzzRouteSetMatch(f *testing.F) {
	// Seed corpus: (route expression, attribute key, attribute value)
	f.Add("*", "env", "prod")
	f.Add("", "env", "prod")
	f.Add(`env="prod"`, "env", "prod")
	f.Add(`env="prod"`, "env", "staging")
	f.Add(`env="prod"`, "level", "error")
	f.Add(`env="prod" AND level="error"`, "env", "prod")
	f.Add(`NOT env="dev"`, "env", "prod")
	f.Add(`NOT env="dev"`, "env", "dev")
	f.Add(`key_exists("host")`, "host", "web-1")
	f.Add(`key_exists("host")`, "env", "prod")

	f.Fuzz(func(t *testing.T, expr, key, value string) {
		cr, err := CompileRoute(glid.New(), "fuzz", 0, expr, []RouteDestination{{VaultID: glid.New()}}, "fanout")
		if err != nil {
			return // invalid expressions are expected
		}
		rs := NewRouteSet([]*CompiledRoute{cr})

		attrs := chunk.Attributes{}
		if key != "" {
			attrs[key] = value
		}

		// Must not panic on any input.
		_ = rs.Match(attrs)
	})
}
