package syslogparse

import "testing"

func FuzzParseMessage(f *testing.F) {
	// Seed corpus: real syslog messages in various formats.
	seeds := []string{
		// RFC 3164 (BSD)
		`<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8`,
		`<13>Feb  5 17:32:18 10.0.0.99 test: hello world`,
		`<165>Aug 24 05:34:00 CSC SFW[123]: msg`,
		// RFC 5424
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - 'su root' failed`,
		`<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts.`,
		`<14>1 2024-01-15T10:30:00Z host app 1234 msgid - message body here`,
		// Minimal / edge cases
		`<0>test`,
		`<191>test`,
		`test without priority`,
		``,
		`<`,
		`<>test`,
		`<999>test`,
		`<34>`,
		// Garbage
		"\x00\x01\x02\x03",
		"\xff\xfe\xfd",
		`<34>1 - - - - - -`,
		`<34>1 2024-01-15T10:30:00Z - - - - -`,
		// Long hostname/appname
		`<14>1 2024-01-15T10:30:00Z ` + string(make([]byte, 200)) + ` app 123 - - msg`,
		// Missing fields
		`<14>1`,
		`<14>1 2024-01-15T10:30:00Z`,
		`<14>1 2024-01-15T10:30:00Z host`,
	}

	for _, s := range seeds {
		f.Add([]byte(s), "10.0.0.1")
	}
	// Also test with empty remoteIP.
	f.Add([]byte(`<34>Oct 11 22:14:15 mymachine su: msg`), "")

	f.Fuzz(func(t *testing.T, data []byte, remoteIP string) {
		// ParseMessage must not panic on any input.
		attrs, _ := ParseMessage(data, remoteIP)
		if attrs == nil {
			t.Fatal("ParseMessage returned nil attrs map")
		}
	})
}

func FuzzParsePriority(f *testing.F) {
	seeds := []string{
		"<0>rest",
		"<34>rest",
		"<191>rest",
		"<999>rest",
		"<>rest",
		"<abc>rest",
		"",
		"<",
		"no priority",
	}

	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// ParsePriority must not panic on any input.
		_, _, _ = ParsePriority(data)
	})
}

func FuzzSplitFields(f *testing.F) {
	seeds := []string{
		"",
		"one",
		"one two three",
		"  leading spaces",
		"multiple   spaces   between",
		"a b c d e f g h i j",
	}

	for _, s := range seeds {
		f.Add([]byte(s), 7)
	}

	f.Fuzz(func(t *testing.T, data []byte, n int) {
		// Clamp n to a reasonable range to avoid degenerate cases.
		if n < 0 {
			n = 0
		}
		if n > 100 {
			n = 100
		}
		// SplitFields must not panic on any input.
		_ = SplitFields(data, n)
	})
}
