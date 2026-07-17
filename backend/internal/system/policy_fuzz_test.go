package system

import "testing"

func FuzzRotationPolicyConfigToRotationPolicy(f *testing.F) {
	// Seed corpus: (maxBytes, maxAgeNanos, maxRecords, cron)
	// Zero means nil for the pointer field. Quantities are numeric at rest,
	// The quantity fields are free-form expressions now, so the fuzz space is
	// arbitrary size/duration strings plus cron — the parser must never panic.
	f.Add("", "", int64(0), "0 * * * *")
	f.Add("", "", int64(0), "30 0 * * * *")
	f.Add("", "-1h", int64(0), "")
	f.Add("", "", int64(-1), "")
	f.Add("", "", int64(0), "invalid cron")
	f.Add("10KB", "5m", int64(100), "*/5 * * * *")
	f.Add("64MiB", "-1h", int64(1<<62), "")
	f.Add("garbage", "1x", int64(0), "")
	f.Add("0", "0", int64(0), "")

	// Quantities are now free-form expressions, so this fuzzes the parser the
	// whole model depends on: it must never panic, whatever the operator types.
	f.Fuzz(func(t *testing.T, maxSize, maxAge string, maxRecords int64, cron string) {
		cfg := RotationPolicyConfig{}
		if maxSize != "" {
			cfg.MaxSize = &maxSize
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
		_ = cfg.IsEmpty()
	})
}

func FuzzRetentionPolicyConfigToRetentionPolicy(f *testing.F) {
	// Seed corpus: (maxAgeNanos, maxBytes, maxChunks); zero means nil.
	f.Add("720h", "10GB", int64(100))
	f.Add("24h", "", int64(0))
	f.Add("", "500MB", int64(0))
	f.Add("", "", int64(50))
	f.Add("-1h", "", int64(0))
	f.Add("", "", int64(-1))
	f.Add("garbage", "1x", int64(0))
	f.Add("1h", "1GB", int64(10))

	f.Fuzz(func(t *testing.T, maxAge, maxSize string, maxChunks int64) {
		cfg := RetentionPolicyConfig{}
		if maxAge != "" {
			cfg.MaxAge = &maxAge
		}
		if maxSize != "" {
			cfg.MaxSize = &maxSize
		}
		if maxChunks != 0 {
			cfg.MaxChunks = &maxChunks
		}

		// Must not panic on any input.
		_, _ = cfg.ToRetentionPolicy()
	})
}

func FuzzParseSize(f *testing.F) {
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
		_, _ = ParseSize(s)
	})
}
