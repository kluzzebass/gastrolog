package fluentfwd

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"gastrolog/internal/ingester/identitytest"
)

// TestEventIDIdentity pins gastrolog-44b9r for the Fluent Forward
// ingester. dialIngester hard-codes ID to "test-fwd"; round-trip a
// minimal Message-mode payload and assert.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	addr, out := dialIngester(t, 4)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(3)
	_ = enc.EncodeString("identity.probe")
	_ = enc.EncodeInt(1700000000)
	_ = enc.EncodeMap(map[string]any{"message": "probe"})
	conn := sendMsgpack(t, addr, buf.Bytes())
	defer conn.Close()

	identitytest.AssertHasIdentity(t, recv(t, out), "test-fwd")
}
