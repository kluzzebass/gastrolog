package system

import "testing"

func FuzzRotationPolicyConfigToRotationPolicy(f *testing.F) {
	// Seed corpus: (maxBytes, maxAge, maxRecords, cron)
	// Empty string means nil for the pointer field.
	f.Add("64MB", "1h", int64(10000), "")
	f.Add("1GB", "", int64(0), "")
	f.Add("", "30m", int64(0), "")
	f.Add("", "", int64(5000), "")
	f.Add("", "", int64(0), "0 * * * *")
	f.Add("", "", int64(0), "30 0 * * * *")
	f.Add("bad-bytes", "", int64(0), "")
	f.Add("", "not-a-duration", int64(0), "")
	f.Add("", "-1h", int64(0), "")
	f.Add("", "0s", int64(0), "")
	f.Add("", "", int64(-1), "")
	f.Add("", "", int64(0), "invalid cron")
	f.Add("10KB", "5m", int64(100), "*/5 * * * *")
	f.Add("0B", "", int64(0), "")
	f.Add("", "", int64(0), "")

	f.Fuzz(func(t *testing.T, maxBytes, maxAge string, maxRecords int64, cron string) {
		cfg := RotationPolicyConfig{}
		if maxBytes != "" {
			cfg.MaxBytes = &maxBytes
		}
		if maxAge != "" {
			cfg.MaxAge = &maxAge
		}
		if maxRecords != 0 {
			cfg.MaxRecords = &maxRecords
		}
		if cron != "" {
			cfg.Cron = &cron
		}

		// Must not panic on any input.
		_, _ = cfg.ToRotationPolicy()
		_ = cfg.ValidateCron()
	})
}

func FuzzRetentionPolicyConfigToRetentionPolicy(f *testing.F) {
	// Seed corpus: (maxAge, maxBytes, maxChunks)
	f.Add("720h", "10GB", int64(100))
	f.Add("24h", "", int64(0))
	f.Add("", "500MB", int64(0))
	f.Add("", "", int64(50))
	f.Add("not-a-duration", "", int64(0))
	f.Add("", "bad-bytes", int64(0))
	f.Add("-1h", "", int64(0))
	f.Add("0s", "", int64(0))
	f.Add("", "", int64(-1))
	f.Add("", "", int64(0))
	f.Add("", "0B", int64(0))
	f.Add("1h", "1GB", int64(10))

	f.Fuzz(func(t *testing.T, maxAge, maxBytes string, maxChunks int64) {
		cfg := RetentionPolicyConfig{}
		if maxAge != "" {
			cfg.MaxAge = &maxAge
		}
		if maxBytes != "" {
			cfg.MaxBytes = &maxBytes
		}
		if maxChunks != 0 {
			cfg.MaxChunks = &maxChunks
		}

		// Must not panic on any input.
		_, _ = cfg.ToRetentionPolicy()
	})
}

func FuzzParseBytes(f *testing.F) {
	f.Add("0B")
	f.Add("1KB")
	f.Add("64MB")
	f.Add("1GB")
	f.Add("10TB")
	f.Add("100")
	f.Add("not-a-size")
	f.Add("")
	f.Add("-1MB")
	f.Add("1.5GB")
	f.Add("999999999GB")
	f.Add("1 MB")
	f.Add("1mb")
	f.Add("1MiB")

	f.Fuzz(func(t *testing.T, s string) {
		// Must not panic on any input.
		_, _ = ParseBytes(s)
	})
}
