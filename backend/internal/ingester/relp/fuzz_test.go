package relp

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// FuzzReadFrame feeds random bytes into the RELP frame parser.
// The parser must never panic, regardless of input.
func FuzzReadFrame(f *testing.F) {
	// Valid RELP frames.
	f.Add([]byte("1 open 0\n"))
	f.Add([]byte("1 open 47 relp_version=0\nrelp_software=test\ncommands=syslog\n"))
	f.Add([]byte("2 syslog 35 <34>Jan 15 10:22:15 host app: hi\n"))
	f.Add([]byte("3 close 0\n"))

	// Edge cases.
	f.Add([]byte("0 x 0\n"))
	f.Add([]byte("999999999 syslog 5 hello\n"))
	f.Add([]byte("1 syslog 0\n"))
	f.Add([]byte("\n"))
	f.Add([]byte(""))
	f.Add([]byte("not a frame at all"))
	f.Add([]byte("1 syslog 100 short\n"))           // datalen exceeds actual data
	f.Add([]byte("1 syslog -1 bad\n"))               // negative datalen
	f.Add([]byte("abc syslog 5 hello\n"))             // non-numeric txnr
	f.Add([]byte("1 syslog notanum hello\n"))         // non-numeric datalen
	f.Add([]byte("1\n"))                              // truncated
	f.Add([]byte("1 \n"))                             // empty command then LF
	f.Add([]byte(strings.Repeat("A", 65536)))         // very long token with no delimiter

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		s := NewSession(r, io.Discard)

		// Try to read multiple frames from the input. The parser should
		// return errors for malformed input but must never panic.
		for i := 0; i < 10; i++ {
			_, err := s.readFrame()
			if err != nil {
				return
			}
		}
	})
}

// FuzzParseOffers feeds random strings into the RELP offer parser.
func FuzzParseOffers(f *testing.F) {
	f.Add("relp_version=0\nrelp_software=rsyslog\ncommands=syslog")
	f.Add("")
	f.Add("=")
	f.Add("key=")
	f.Add("=value")
	f.Add("\n\n\n")
	f.Add("a=b\nc=d\ne=f")
	f.Add(strings.Repeat("k=v\n", 10000))

	f.Fuzz(func(t *testing.T, data string) {
		m := parseOffers(data)
		// Must return a non-nil map and never panic.
		if m == nil {
			t.Fatal("parseOffers returned nil")
		}
	})
}
