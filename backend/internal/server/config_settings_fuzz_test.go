package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/config"
)

func FuzzValidateTokenDurations(f *testing.F) {
	f.Add("", "")
	f.Add("15m", "168h")
	f.Add("1h", "24h")
	f.Add("1m", "1h")
	f.Add("30s", "1h")         // token too short
	f.Add("2h", "1h")          // refresh shorter than token
	f.Add("1h", "1h")          // equal durations
	f.Add("not-a-duration", "") // invalid token duration
	f.Add("", "bad")           // invalid refresh duration
	f.Add("1h", "30m")         // refresh shorter than 1h minimum
	f.Add("5m", "")            // only token set
	f.Add("", "48h")           // only refresh set
	f.Add("999999h", "9999999h")

	f.Fuzz(func(t *testing.T, tokenDur, refreshDur string) {
		auth := config.AuthConfig{
			TokenDuration:        tokenDur,
			RefreshTokenDuration: refreshDur,
		}
		// Must not panic on any input.
		_ = validateTokenDurations(auth)
	})
}

func FuzzValidateLookupNames(f *testing.F) {
	// Each seed provides 4 lookup names (one per type). Empty means "no lookup of that type".
	f.Add("geo", "hosts", "city", "ips")
	f.Add("dup", "dup", "", "")   // duplicate name across types
	f.Add("", "", "", "")         // all empty — no lookups
	f.Add("a", "b", "c", "d")    // all unique
	f.Add("same", "", "", "same") // duplicate http + csv
	f.Add("x", "x", "x", "x")   // all four the same

	f.Fuzz(func(t *testing.T, httpName, jsonName, mmdbName, csvName string) {
		lc := config.LookupConfig{}
		if httpName != "" {
			lc.HTTPLookups = []config.HTTPLookupConfig{{Name: httpName, URLTemplate: "http://example.com"}}
		}
		if jsonName != "" {
			lc.JSONFileLookups = []config.JSONFileLookupConfig{{Name: jsonName, FileID: "f"}}
		}
		if mmdbName != "" {
			lc.MMDBLookups = []config.MMDBLookupConfig{{Name: mmdbName}}
		}
		if csvName != "" {
			lc.CSVLookups = []config.CSVLookupConfig{{Name: csvName}}
		}
		// Must not panic on any input.
		_ = validateLookupNames(lc)
	})
}

func FuzzMergeCluster(f *testing.F) {
	f.Add("5s")
	f.Add("1m")
	f.Add("")
	f.Add("not-a-duration")
	f.Add("0s")
	f.Add("-1h")
	f.Add("999999h")

	f.Fuzz(func(t *testing.T, interval string) {
		cluster := config.ClusterConfig{}
		msg := &apiv1.PutClusterSettings{
			BroadcastInterval: &interval,
		}
		// Must not panic on any input.
		_ = mergeCluster(msg, &cluster)
	})
}
