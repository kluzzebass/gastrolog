# Ingester / Untrusted Input Security Audit

Scope: `backend/internal/ingester/**` (syslog, syslogparse, relp, http, otlp, kafka,
mqtt, docker, fluentfwd, chatterbox, scatterbox, tail, metrics, self, bodyutil) and
the digester chain they feed. Read-only review — no code changes, no cluster
commands, no test runs.

All findings below are **verified by reading the code at the cited lines**, unless
explicitly marked "suspected." Severity considers both technical impact and how
much network access an attacker needs (all TCP/HTTP/gRPC ingesters below bind to
`:<port>`, i.e. all interfaces, by default, and none except RELP support any
authentication).

---

## Finding 1: RELP DATALEN is trusted without an upper bound — single-frame memory-exhaustion DoS

**Severity: Critical**

**File:** `backend/internal/ingester/relp/protocol.go:114-131`

```go
datalenStr, delim, err := s.readTokenOrLF()
...
datalen, err := strconv.Atoi(datalenStr)
...
if datalen > 0 {
    ...
    data = make([]byte, datalen)
    if _, err := io.ReadFull(s.r, data); err != nil {
```

`datalen` comes straight off the wire (the RELP `DATALEN` field) and is passed to
`make([]byte, datalen)` with no upper-bound check. `strconv.Atoi` on a 64-bit
platform accepts any value up to `math.MaxInt64` (roughly 9.2×10^18), so a client
can request an allocation of many gigabytes by sending a single crafted frame such
as:

```
1 syslog 9999999999 X
```

The allocation happens **before** `io.ReadFull` blocks trying to read that many
bytes, so the attacker doesn't even need to send the data — the `make([]byte, n)`
call alone can trigger runtime OOM / process kill, or at minimum force a huge GC
pause, from one connection and one frame. Repeating this on multiple connections
(no connection cap exists — see Finding 5) multiplies the effect. Compare with
`backend/internal/ingester/syslog/listen.go:334-354`, where the octet-counted
framing path for plain syslog does enforce a `1<<20` (1 MB) sanity cap — RELP has
no equivalent.

**Attacker requirement:** TCP reachability to the RELP port (default `:2514`,
`backend/internal/ingester/relp/listen.go:11`, bound on all interfaces). No
authentication is required unless the operator has opted into mutual TLS
(`tls_ca` param) — off by default. The RELP `open` handshake is public protocol
and trivial to complete.

**Existing fuzz coverage does not mitigate this.** `relp/fuzz_test.go`
(`FuzzReadFrame`) asserts the parser "must never panic" and its seed corpus
includes a large *TXNR* (`999999999 syslog 5 hello\n`) but no seed exercises a
large *DATALEN*. The fuzz target only proves crash-freedom on malformed short
input, not that resource consumption is bounded — it does not catch this class of
bug.

**Remediation:** Cap `datalen` (e.g. a few MB, matching the plain-syslog
octet-counted cap) before calling `make`, and reject/close the connection on
frames that exceed it.

---

## Finding 2: zstd decompression bomb — `bodyutil.ReadBody`'s `maxBytes` only bounds the compressed input, not the decompressed output

**Severity: Critical**

**File:** `backend/internal/ingester/bodyutil/bodyutil.go:29-56`

```go
case "zstd":
    compressed, err := io.ReadAll(io.LimitReader(body, maxBytes))
    ...
    decompressed, err := zstdDec.DecodeAll(compressed, nil)
    ...
    return decompressed, nil

case "gzip":
    gz, err := gzip.NewReader(body)
    ...
    return io.ReadAll(io.LimitReader(gz, maxBytes))
```

For `Content-Encoding: zstd`, `maxBytes` limits how many **compressed** bytes are
read, but `zstdDec.DecodeAll` then decompresses the *entire* buffer with no output
cap — `WithDecoderMaxMemory(10<<20)` (bodyutil.go:19) only bounds the decoder's
internal window size, it does not cap total decompressed output across multiple
blocks/frames referencing that window repeatedly. A small, well-crafted zstd
payload (well under the 10 MB/1 MB compressed-input cap actually passed by
callers) can decompress into gigabytes, exhausting node memory. This is the
textbook zstd/zip-bomb pattern.

The **gzip** branch, by contrast, is correctly bounded: it passes
`io.LimitReader(gz, maxBytes)` around the *decompressing* reader, so gzip bombs
are safe here.

**Reachable via:**
- `backend/internal/ingester/http/ingester.go:189` — Loki push endpoint
  (`POST /loki/api/v1/push`, default `:3100`, no auth), `maxBytes=10<<20`.
- `backend/internal/ingester/otlp/ingester.go:142` — OTLP/HTTP endpoint
  (`POST /v1/logs`, default `:4318`, no auth), `maxBytes=10<<20`.

Both are bound on all interfaces by default with zero authentication.

**Note on OTLP/gRPC:** the same zstd decompressor is also registered for gRPC
(`otlp/grpccomp.go:25-27`, also `WithDecoderMaxMemory(10<<20)` with no output
cap), but `grpc.NewServer()` is constructed with default options
(`otlp/ingester.go:113`), and grpc-go's own message-size enforcement
(`maxReceiveMessageSize`, default 4 MB) applies a `LimitReader` around
decompression when the compressor doesn't advertise a `DecompressedSize`, which
this custom compressor does not — so the gRPC path is *likely* bounded by
grpc-go's default even though the OTLP-specific decompressor itself has no cap.
This is **suspected, not verified by execution** — grpc-go's internal
`decompress()` behavior was reasoned from documented/known library semantics, not
traced line-by-line in vendored source. The HTTP paths (Finding 2's primary
claim) are verified directly from `bodyutil.go`.

**Fuzz coverage:** `bodyutil/bodyutil_fuzz_test.go` only asserts `ReadBody` "must
not panic" on garbage input — it never asserts an output-size bound, so it would
not catch this bug even under `-fuzz`.

**Remediation:** Wrap `zstdDec.DecodeAll`'s output through a bound, e.g. use
`zstdDec.DecodeAll(compressed, make([]byte, 0, maxBytes))` is not itself
sufficient (DecodeAll still grows the buffer past the initial capacity) — instead
decompress via `zstdDec.IOReadCloser(bytes.NewReader(compressed))` (or
`NewReader`) wrapped in `io.LimitReader(_, maxBytes)`, matching the gzip branch's
pattern, and treat hitting the limit as an error.

---

## Finding 3: Syslog TCP newline-delimited framing has no line-length bound — unbounded per-connection memory growth

**Severity: High**

**File:** `backend/internal/ingester/syslog/listen.go:310-317`

```go
func readNewlineDelimited(reader *bufio.Reader) ([]byte, error) {
    line, err := reader.ReadBytes('\n')
    ...
}
```

`bufio.Reader.ReadBytes('\n')` accumulates bytes into a growing internal slice
until it sees `\n` (or hits an error). There is no maximum line length. A TCP
client that never sends `\n` forces the server to keep appending to that buffer
for as long as the connection's read deadline allows. The read deadline
(`conn.SetReadDeadline(time.Now().Add(30 * time.Second))`, line 274) is reset once
per *frame*, not per underlying read, so a client that streams data continuously
without a newline can push a large amount of data into one unbounded buffer
within that 30-second window — bounded only by network throughput, not by any
application-level cap.

This is inconsistent with the same file's octet-counted framing path
(`readOctetCounted`, lines 336-360), which explicitly enforces `length > 1<<20 →
error`. The newline-delimited path (the default for any message not starting with
an ASCII digit) has no equivalent.

**Attacker requirement:** TCP reachability to the syslog TCP port (operator
configured via `tcp_addr`, no default but commonly `:514` or similar; no
authentication). Single connection suffices.

**Remediation:** Use `bufio.Reader.ReadSlice`/a size-tracking wrapper, or
`bufio.Scanner` with `Buffer(..., maxLine)`, to cap line length (e.g. matching
RELP/octet-count's implied ~1 MB ceiling) and drop/close the connection instead
of growing without bound.

---

## Finding 4: No `recover()` anywhere in the ingester package — a panic in most listener goroutines takes down the whole node, not just the connection

**Severity: High**

**Verified:** `grep -rln "recover()" backend/internal/ingester/` returns **no
matches** in the entire package.

In Go, an unrecovered panic in *any* goroutine terminates the whole process,
regardless of which goroutine it originated in. `net/http`'s own per-connection
goroutine (`net/http.conn.serve`) does install its own `recover()`, which shields
the **HTTP-transport** ingesters (`http` Loki-push, `otlp`'s `/v1/logs` HTTP
endpoint) from a panic inside their handler crashing the process — that part is
mitigated by the standard library, not by this codebase.

Everything else is not shielded:

- **RELP** — `handleConn` spawned via `wg.Go(func() { r.handleConn(ctx, conn, out) })`,
  `backend/internal/ingester/relp/ingester.go:130-132`. No recover.
- **Syslog TCP** — `handleTCPConn` spawned via `wg.Go(...)`,
  `backend/internal/ingester/syslog/listen.go:243-246`. No recover. (UDP read loop,
  same file, also has none.)
- **Fluent Forward** — `handleConn` spawned via `wg.Go(func() { ing.handleConn(ctx, conn) })`,
  `backend/internal/ingester/fluentfwd/ingester.go:127-129`. No recover.
- **OTLP gRPC** — `grpc.NewServer()` is constructed with **no interceptors**
  (`backend/internal/ingester/otlp/ingester.go:113`). grpc-go does not
  automatically recover panics raised inside a service handler (this is exactly
  why the community `grpc-ecosystem/go-grpc-middleware` `recovery` interceptor
  exists) — without one installed here, a panic inside `Export` (or anything it
  calls: `flattenKVList`, `anyValueToString`, etc.) crashes the process. This half
  of the finding is **reasoned from grpc-go's well-documented behavior**, not
  traced through vendored source line-by-line.
- **Docker, Kafka, MQTT** — all run their read/consume loops in bare goroutines
  with no recover.

Concretely reachable panic candidates were not found in the current parsers (all
byte-slice indexing I reviewed in `syslogparse`, `relp/protocol.go`, and
`docker/logstream.go` is length-checked first), so this is currently a
**defense-in-depth gap** rather than a demonstrated crash — but it means the
*next* off-by-one or unchecked type assertion added to any of these paths
(there is no panic-recovery net) will crash the entire node from a single hostile
packet, not just drop one connection.

**Remediation:** Wrap every per-connection/per-message goroutine body in a
`defer func() { if r := recover(); r != nil { log...; close conn } }()`, and add a
`grpc.UnaryInterceptor`/`grpc.StreamInterceptor` (or the standard
`go-grpc-middleware/recovery` interceptor) to the OTLP gRPC server.

---

## Finding 5: No connection limits or read deadlines on RELP and Fluent Forward — slowloris resource-hold

**Severity: Medium**

**Files:**
- `backend/internal/ingester/relp/ingester.go` — `grep -n "SetReadDeadline"` finds
  nothing in this file (only in test files `cn_verifier_test.go`). The accept
  loop sets an *accept* deadline (line 115) but the per-connection read path
  (`handleConn` → `session.ReceiveLog` → `readFrame`/`readToken`) never sets a
  deadline on the connection itself.
- `backend/internal/ingester/fluentfwd/ingester.go` — same: no
  `SetReadDeadline`/`SetDeadline` call anywhere in the file. `handleConn` reads
  via `msgpack.NewDecoder(conn)` directly off the raw connection.

Combined with `relp/protocol.go`'s `readToken`/`readTokenOrLF`
(lines 146-174), which append to an unbounded `[]byte` one byte at a time with no
length cap and no deadline, a client can open a connection, dribble one byte
every few seconds forever, and the server goroutine (plus its socket and any
partially-accumulated buffer) is held indefinitely. There is also no
max-concurrent-connections limit anywhere in the ingester package
(`grep -rn "MaxConn\|Limiter\|semaphore"` over `backend/internal/ingester/` finds
nothing) — an attacker can open many thousands of such connections, each holding
a goroutine + a socket, exhausting file descriptors / goroutines on the node.

**Attacker requirement:** TCP reachability to RELP (`:2514`) or Fluent Forward
(`:24224`), both unauthenticated by default, no auth option exists for Fluent
Forward at all.

**Remediation:** Set a read (and idle) deadline on every accepted connection in
both ingesters, refreshed per successful read rather than left unset, and add a
process-wide or per-ingester connection cap (reject/close beyond N concurrent
connections per listener).

---

## Finding 6: Field/attribute explosion — only the HTTP (Loki) ingester caps attribute count and length; OTLP, Fluent Forward, and Kafka do not

**Severity: Medium**

`backend/internal/ingester/http/ingester.go:22-27,325-338` defines and enforces
`maxAttrs=32`, `maxAttrKeyLen=64`, `maxAttrValueLen=256` via `addAttr`. No other
network-facing ingester has an equivalent, and the digester chain
(`backend/internal/digester/{level,timestamp}`) provides no downstream
attribute-cardinality guard — there is no second line of defense.

- **OTLP** — `backend/internal/ingester/otlp/ingester.go:222-227` builds `attrs`
  from `resourceAttrs` + `scopeAttrs` + per-record attributes via
  `flattenKVList`/`maps.Copy` with no cap on the number of `KeyValue` pairs or
  their key/value length. A single `ExportLogsServiceRequest` (bounded only by
  the 10 MB HTTP body cap, or grpc-go's default 4 MB message cap for the gRPC
  path) can carry an arbitrarily large number of attributes, each becoming a map
  entry.
- **Fluent Forward** — `backend/internal/ingester/fluentfwd/ingester.go:383-405`
  (`processRecord`) does `for k, v := range record { attrs[k] = fmt.Sprint(v) }`
  with no cap on map size or per-value length. msgpack map size itself is
  unbounded by the decoder. Combined with Finding 5 (no read deadline), a single
  record with a huge number of keys is a viable memory-amplification vector.
- **Kafka** — `backend/internal/ingester/kafka/ingester.go:184-192`
  (`buildMessage`) copies every Kafka record header into `attrs` with no cap.
  Lower priority since it requires the attacker to already be able to publish to
  the configured topic, not raw network reachability.

**Remediation:** Extract the `http` ingester's `addAttr`-style
count/key-length/value-length enforcement into a shared helper in a common
package (or `bodyutil`) and apply it uniformly to OTLP, Fluent Forward, and Kafka
attribute construction.

---

## Finding 7 (Low / informational): Docker frame length trusted without an explicit cap

**Severity: Low**

**File:** `backend/internal/ingester/docker/logstream.go:63-71`

```go
size := binary.BigEndian.Uint32(header[4:8])
...
payload := make([]byte, size)
if _, err := io.ReadFull(r, payload); err != nil {
```

`size` (up to 4 GB) is trusted without an explicit upper-bound check before
`make([]byte, size)`. In practice the Docker daemon's own `json-file` log driver
caps individual frames at 16 KB (noted in the surrounding comment), so this is
not attacker-reachable over the network — the trust boundary here is the local
Docker daemon (typically root-owned Unix socket), not an external network peer,
which is why this scored Low rather than Critical despite the structural
similarity to Finding 1. Still, there's no defensive cap if that assumption ever
changes (e.g. a compromised or non-standard log driver).

**Remediation (optional hardening):** Add a sanity cap (e.g. 1 MB) before the
`make([]byte, size)` call, consistent with the other framing protocols in this
audit.

---

## Finding 8 (Low): No authentication on any ingester except optional RELP mTLS

**Severity: Low (amplifying factor, not a standalone bug)**

Verified via `grep` for `Authorization`/`Bearer`/`apikey`/`tls_cert` across every
ingester's `factory.go`/`ingester.go`: only `relp` supports TLS (optional,
`tls=true` param) and optional mutual-TLS with CN allow-listing
(`relp/ingester.go:261-333`). The HTTP (Loki push), OTLP (HTTP+gRPC), syslog, and
Fluent Forward ingesters have no authentication mechanism at all — any host that
can reach the port can inject log records. This matches the corresponding
upstream protocols (Loki push, OTLP, syslog, Fluent Forward all commonly run
unauthenticated in the wild, often behind a private network or reverse proxy), so
it is flagged as context rather than a defect — but it means every finding above
requires *only* network reachability, not any credential, to exploit.

---

## Checked and sound

- **Fluent Forward gzip decompression** — `fluentfwd/ingester.go:499-521`
  (`gunzip`) explicitly caps decompressed output at `maxDecompressedFluentBytes =
  100<<20` via `io.LimitReader(r, cap+1)` and rejects payloads that hit the cap.
  Good pattern — should be the template for Finding 2's zstd fix.
- **`bodyutil.ReadBody`'s gzip branch** — correctly bounds decompressed gzip
  output via `io.LimitReader(gz, maxBytes)` (only the zstd branch, Finding 2, is
  broken).
- **Syslog UDP** — fixed 64 KB read buffer (`syslog/listen.go:154`), matches max
  UDP datagram size; no unbounded growth.
- **Syslog octet-counted TCP framing** — explicit `1<<20` cap
  (`syslog/listen.go:351`) before allocating the message buffer.
- **`syslogparse` package** — every byte-slice access I traced (`ParsePriority`,
  `ParseRFC3164`, `ParseRFC5424`, `parsePID`, `parseToken`, `SplitFields`) checks
  length/bounds before indexing; all string/attribute fields are also
  length-capped (64/16/64 chars) before being stored. Has fuzz coverage
  (`syslogparse/parse_fuzz_test.go`).
- **Tail ingester** — uses `bufio.Scanner` with an explicit 1 MB line cap
  (`tail/ingester.go:247`, `docker/logstream.go:127` for the TTY/raw path too).
  No unbounded accumulation.
- **No shell/exec surface** — `grep -rn "exec.Command"` across the whole package
  returns nothing; ingested record content never reaches a shell.
- **No path traversal via ingested content** — all `filepath.Join`/`os.Open`/
  `os.WriteFile` calls in the package use operator-supplied config (`id`,
  `stateDir`, tail glob patterns) as path components, never attacker-controlled
  record content.
- **Chatterbox / Scatterbox** — synthetic/self-generated traffic only, no
  network input, out of scope for untrusted-input risk.
- **MQTT / Kafka transport** — both are outbound clients (subscribe/consume from
  an operator-configured broker), not listeners; the untrusted-input surface is
  gated by the operator's choice of broker/topic, not raw network reachability.
  Attribute-explosion caveat still applies (Finding 6, Kafka only).
- **HTTP/OTLP request-header slowloris** — both `http.Server` instances set
  `ReadHeaderTimeout: 10 * time.Second` (`http/ingester.go:98`,
  `otlp/ingester.go:104`), which covers the classic header-slowloris case. (Body
  reads after headers have no `ReadTimeout`/`IdleTimeout`, which is a minor,
  lower-priority gap relative to Findings 1/5 — not separately scored since the
  body itself is already size-limited to 10 MB via `bodyutil.ReadBody`, just not
  time-limited.)
