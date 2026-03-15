package timestamp

import "testing"

func FuzzExtractTimestamp(f *testing.F) {
	// RFC 3339.
	f.Add([]byte("2024-01-15T10:30:45.123456Z some log line"))
	f.Add([]byte("2024-01-15T10:30:45+01:00 msg"))
	f.Add([]byte("2024-01-15T10:30:45.123+01:00"))

	// Apple unified log.
	f.Add([]byte("2024-01-15 10:30:45.123456-0800 process msg"))

	// Syslog BSD (RFC 3164).
	f.Add([]byte("Jan  5 15:04:02 host app: message"))
	f.Add([]byte("Oct 11 22:14:15 mymachine su: 'su root'"))
	f.Add([]byte("Feb  1 01:02:03 hostname"))

	// Common Log Format.
	f.Add([]byte(`10.0.0.1 - - [02/Jan/2006:15:04:05 -0700] "GET / HTTP/1.1"`))

	// Go/Ruby datestamp.
	f.Add([]byte("2024/01/15 10:30:45 INFO message"))

	// Ctime / BSD.
	f.Add([]byte("Fri Feb 13 17:49:50 2026 some message"))
	f.Add([]byte("Fri Feb 13 17:49:50.028 2026"))
	f.Add([]byte("Mon Jan  3 09:00:00 msg"))

	// Edge cases.
	f.Add([]byte(""))
	f.Add([]byte("no timestamp here"))
	f.Add([]byte("2024-13-40T99:99:99Z"))
	f.Add([]byte("<34>Oct 11 22:14:15 host app: msg"))
	f.Add([]byte("just some random bytes: @#$%^&*()"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		// Must not panic.
		ts := extractTimestamp(raw)
		_ = ts
	})
}
