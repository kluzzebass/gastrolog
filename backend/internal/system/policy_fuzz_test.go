package system

import "testing"

func FuzzRotationPolicyConfigToRotationPolicy(f *testing.F) {
	// Seed corpus: (maxBytes, maxAgeNanos, maxRecords, cron)
	// Zero means nil for the pointer field. Quantities are numeric at rest,
	// so the fuzz space is signs/extremes/cron strings — unparseable quantity
	// strings can no longer reach the config.
	f.Add(uint64(64_000_000), int64(3600e9), int64(10000), "")
	f.Add(uint64(1_000_000_000), int64(0), int64(0), "")
	f.Add(uint64(0), int64(1800e9), int64(0), "")
	f.Add(uint64(0), int64(0), int64(5000), "")
	f.Add(uint64(0), int64(0), int64(0), "0 * * * *")
	f.Add(uint64(0), int64(0), int64(0), "30 0 * * * *")
	f.Add(uint64(0), int64(-3600e9), int64(0), "")
	f.Add(uint64(0), int64(0), int64(-1), "")
	f.Add(uint64(0), int64(0), int64(0), "invalid cron")
	f.Add(uint64(10_000), int64(300e9), int64(100), "*/5 * * * *")
	f.Add(^uint64(0), int64(1<<62), int64(1<<62), "")

	f.Fuzz(func(t *testing.T, maxBytes uint64, maxAgeNanos, maxRecords int64, cron string) {
		cfg := RotationPolicyConfig{}
		if maxBytes != 0 {
			cfg.MaxBytes = &maxBytes
		}
		if maxAgeNanos != 0 {
			cfg.MaxAgeNanos = &maxAgeNanos
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
	// Seed corpus: (maxAgeNanos, maxBytes, maxChunks); zero means nil.
	f.Add(int64(720*3600e9), uint64(10_000_000_000), int64(100))
	f.Add(int64(24*3600e9), uint64(0), int64(0))
	f.Add(int64(0), uint64(500_000_000), int64(0))
	f.Add(int64(0), uint64(0), int64(50))
	f.Add(int64(-3600e9), uint64(0), int64(0))
	f.Add(int64(0), uint64(0), int64(-1))
	f.Add(int64(0), uint64(0), int64(0))
	f.Add(int64(3600e9), uint64(1_000_000_000), int64(10))
	f.Add(int64(1<<62), ^uint64(0), int64(1<<62))

	f.Fuzz(func(t *testing.T, maxAgeNanos int64, maxBytes uint64, maxChunks int64) {
		cfg := RetentionPolicyConfig{}
		if maxAgeNanos != 0 {
			cfg.MaxAgeNanos = &maxAgeNanos
		}
		if maxBytes != 0 {
			cfg.MaxBytes = &maxBytes
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
