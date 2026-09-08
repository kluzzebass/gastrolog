# Security audit — lens 02: TLS, transport security, inter-node trust

Read-only review. Every claim below is **verified by reading** the cited lines unless
explicitly marked *suspected*. No commands were run against the cluster, no tests executed.

Severity counts: **1 Critical, 4 High, 5 Medium, 4 Low**.

---

## C-1 (Critical) — Raft lane gRPC stacks require no client certificate: unauthenticated Raft RPC injection

**Files**
- `backend/internal/cluster/cluster.go:685-688` — raft lane servers built with `s.baseServerOpts(maxRaftLaneRecvBytes, false)`
- `backend/internal/cluster/cluster.go:709-727` — `fullInterceptors=false` skips `mTLSUnaryInterceptor` / `mTLSStreamInterceptor`
- `backend/internal/cluster/clustertls.go:146-158` — `ClientAuth: tls.VerifyClientCertIfGiven` on both the outer config and `GetConfigForClient`
- `backend/internal/cluster/lane_listener.go:119-135` — SNI demux routes `gastrolog-raft.<group>` straight to the lane listener, no checks
- `backend/internal/multiraft/group_lane_api.go:31-108` — the only inbound check is `checkGroupID`, never peer identity
- `backend/internal/multiraft/service.go:61-139` — handlers discard the interceptor argument (`_ grpc.UnaryServerInterceptor`)

**What it is.** The cluster port's TLS config uses `VerifyClientCertIfGiven`, so a TLS
handshake **completes with no client certificate at all**. On the service lane that is
deliberately compensated for by `requireClientCert` (`cluster.go:745-780`), which rejects
every method except `/Enroll` when `len(tlsInfo.State.VerifiedChains) == 0`. The per-group
**raft lanes never get that interceptor** — `EnsureRaftGroupLane` passes `fullInterceptors=false`
and then appends only the pause interceptors. `hashicorp/raft` assumes the transport
authenticates the peer; nothing here does.

**Attacker scenario.** Network reach to the cluster port (`--cluster-addr`, typically :4566)
is the *only* requirement — no credentials, no join token, no certificate. The attacker dials
TLS with SNI `gastrolog-raft.cluster-config` (or `gastrolog-raft.vault.<id>.ctl`; group IDs are
derivable and `gastrolog-raft` legacy SNI maps to the config group), ignores the server cert,
sends no client cert, and calls `MultiRaftTransportService`:
- `RequestVote` / `RequestPreVote` with an inflated term → forces repeated elections; a
  permanent cluster-wide consensus DoS with a few packets.
- `AppendEntries` posing as a leader at a higher term → **commits arbitrary FSM entries**
  into the cluster-config group: create/delete vaults, rewrite `NodeConfig`, rewrite
  `PutClusterTLS` (CA + join token). This is full control of the cluster.
- `TimeoutNow` → forces a target into candidate state on demand.
- `InstallSnapshot` → replaces a follower's entire FSM state.

**Remediation.** Two layers, and the interceptor one is *not* the one-liner it looks like.

Adding `s.mTLSUnaryInterceptor` / `s.mTLSStreamInterceptor` to the raft lane option set
changes nothing on its own: the `MultiRaftTransportService` handlers are hand-written
`grpc.MethodDesc` entries, and the unary ones discarded the `grpc.UnaryServerInterceptor`
argument gRPC hands them. gRPC applies stream interceptors itself but delegates unary ones
to the handler, so *every* server-level unary interceptor on that service was inert —
including the pause interceptor the lanes explicitly installed. The handlers must run
dispatch through that chain, the way the `ClusterService` handlers in `forward.go` already
did.

The gate that actually rejects the anonymous dial is the TLS layer: choose `ClientAuth`
per connection in `GetConfigForClient` from the ClientHello SNI. Raft lane SNIs get
`tls.RequireAndVerifyClientCert`; everything else reaches the service lane and stays
`VerifyClientCertIfGiven`, because Enroll is how a joining node obtains the certificate it
does not yet have. The SNI is already the demux key, so this needs no separate listener —
tightening the whole port instead would lock new nodes out. The mTLS interceptor is then
defence in depth, not the sole gate.

---

## H-1 (High) — Enrollment TOFU verifies a fingerprint but never verifies the chain

**File** `backend/internal/cluster/enrollclient.go:79-98` (called from `:40-46`)

```go
for _, cert := range peerCerts {
    if cert.IsCA { hash := sha256.Sum256(cert.Raw); if hex...== hex...(expectedHash) { return nil } }
}
for _, cert := range peerCerts { /* same, without the IsCA filter */ }
```

`verifyCAFingerprint` walks the chain the server sent and returns success if **any**
certificate in it hashes to the pinned value. It never checks that the *leaf* was issued by
that CA — no `x509.Certificate.Verify`, no root pool, no signature check, no expiry check on
the leaf, no name check. Combined with `InsecureSkipVerify: true` (line 41), the leaf is
entirely unvalidated.

The real leader deliberately appends its CA cert to the served chain
(`clustertls.go:66-69`), so the CA DER is public to anyone who can complete a handshake with
any cluster node — which requires no credentials (see C-1).

**Attacker scenario.** An attacker on the joiner→leader path (ARP/DNS spoofing, hostile
sidecar, compromised service mesh, a `--join-addr` that resolves attacker-side) fetches the
genuine CA cert from any node, then presents `[attacker-leaf-signed-by-attacker-CA,
genuine-CA-DER]`. The first loop matches on the genuine CA and returns nil. The joiner then
sends its **join token secret in cleartext-to-the-attacker** and accepts whatever TLS
material comes back. The stolen token is a permanent cluster credential (see H-3), so the
MITM converts into a full cluster join at leisure. The `VerifyConnection`-vs-
`VerifyPeerCertificate` fix here is correct and does hold on resumed sessions — but the
callback it protects does not actually verify anything.

**Remediation.** Parse the pinned CA into an `x509.CertPool` and verify the leaf against it:
```go
roots := x509.NewCertPool(); roots.AddCert(pinnedCA)
_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
    Roots: roots, Intermediates: poolFrom(cs.PeerCertificates[1:]), DNSName: cluster.SNIServiceLane,
})
```
where `pinnedCA` is the chain member whose SHA-256 equals the token hash. Also drop the
second (`IsCA`-less) fallback loop — it lets a pinned *leaf* fingerprint stand in for a CA.
Use `subtle.ConstantTimeCompare` on the raw digests instead of comparing hex strings.

---

## H-2 (High) — One certificate and one private key for the entire cluster; no node identity exists

**Files**
- `backend/internal/app/cluster.go:222-226` — Enroll hands the joiner `tls.ClusterCertPEM` **and `tls.ClusterKeyPEM`**, i.e. the bootstrap node's own keypair, verbatim
- `backend/internal/cluster/tlsutil/tlsutil.go:112-124` — every cluster cert is `CN=gastrolog-cluster`, SANs `localhost, 127.0.0.1, ::1, gastrolog-cluster, gastrolog-raft`, both `ServerAuth` and `ClientAuth`
- `backend/internal/cluster/cluster.go:763-780` — `requireClientCert` checks only that *a* verified chain exists
- `backend/internal/cluster/clustertls.go:191-198` — raft-lane `VerifyConnection` checks chain only, no name/identity
- `backend/internal/cluster/peer_bytes.go:20, 233-241` — the caller's node ID is a client-asserted metadata header, used only for byte accounting

**What it is.** There is no per-node certificate. Enrollment copies the *same* cert and
*same* private key to every joining node. Identity checking is therefore not merely absent,
it is impossible: authentication is exactly "presents the one cluster certificate". The
`x-gastrolog-node-id` header, the only per-node signal on the wire, is unauthenticated.

**Attacker scenario (post-compromise blast radius).** Compromise of any single node — a
crashed pod's volume, a stolen `cluster_tls.json` (`clustertls.go:87-109`), an attacker who
enrolled once with a leaked token — yields the private key **for every node**. Any peer can
then impersonate the leader to any follower on any lane, and nothing distinguishes node A
from node B. There is also no revocation path: no CRL, no OCSP, no serial deny-list; removing
a rogue node from Raft membership does not stop it reconnecting, because its credential is
the cluster credential. Rotating requires re-keying all nodes at once.

**Remediation.** Issue a **per-node** cert at enrollment (leader holds the CA key, signs a
CSR — or at minimum generates a distinct keypair per node) with the node ID in the CN and a
SAN, and never transmit an existing node's private key. Then enforce identity: on the service
lane, check the presented cert's node ID against the Raft configuration; on the raft lane,
extend the `VerifyConnection` chain check with the expected peer node ID. Derive the peer
node ID from the certificate rather than from the `x-gastrolog-node-id` header.

---

## H-3 (High) — The join token is a static, never-expiring, never-rotating secret, printed to logs on every boot

**Files**
- `backend/internal/app/cluster.go:163` — `logger.Info("cluster join token (use --join-token to join)", "token", token)`
- `backend/internal/app/cluster.go:188-189` — logged **again on every restart**, together with the CA hash
- `backend/internal/cluster/tlsutil/tlsutil.go:142-158` — 32 random bytes, no expiry, no nonce, no use counter
- `backend/internal/app/cluster.go:213` — the only check the Enroll handler performs
- No caller of `PutClusterTLS` other than first-boot bootstrap (`app/cluster.go:144`) — **there is no rotation path**

**What it is.** The whole trust bootstrap reduces to one bearer secret with unlimited
lifetime and unlimited uses, and that secret is written to the process's own log stream at
`Info` on every single startup. It is additionally readable via `GetClusterStatus`
(`server/lifecycle.go:296-299`) — admin-gated (`auth/interceptor.go:98`), which is correct —
and via the Unix socket, which injects a synthetic admin (`auth/interceptor.go:16-24`,
`server/server_tls.go:119-164`; socket is 0600, acceptable).

**Attacker scenario.** Anyone who can read the node's stderr/container logs/journald — a
log-shipping sidecar, a Loki/CloudWatch reader, a colleague with `kubectl logs`, a support
bundle — obtains a permanent cluster credential. Presenting it to `/Enroll` returns the
cluster cert **and private key** (H-2), i.e. full peer authority (M-1) forever. GastroLog's
own self ingester defaults to `min_level: warn` (`ingester/self/factory.go:17-20`), so the
`Info` line is not captured into a vault by default — but an operator raising the self
ingester to `info` puts a live cluster credential into a searchable vault, where
`QueryService.Search` is **not** admin-gated (`auth/interceptor.go:88-149`), making it
readable by any authenticated user. *(That last step is suspected — I did not trace the
search authorization path end to end in this lens.)*

**Remediation.** Do not log the token; log a fingerprint or a "token available via
`gastrolog cluster join-token`" pointer. Give tokens a TTL and a use limit (single-use
preferred, minted on demand by an admin), and add a rotation path that re-runs
`GenerateJoinToken` and commits via `PutClusterTLS`.

---

## H-4 (High) — The cluster CA private key is replicated to every node in the system FSM

**Files**
- `backend/internal/system/config.go:209-216` — `ClusterTLS{CACertPEM, CAKeyPEM, ClusterCertPEM, ClusterKeyPEM, JoinToken}`
- `backend/internal/system/command/command.go:804-828` — `PutClusterTLSCommand` carries `CaKeyPem` and `ClusterKeyPem` through the Raft log
- `backend/internal/app/cluster.go:144-152` — bootstrap writes it via `cfgStore.PutClusterTLS`

**What it is.** The CA signing key, the cluster private key, and the join token travel as a
replicated FSM command. Every node that replays the log or restores a snapshot therefore
holds the CA key on disk in the Raft WAL and snapshots, unencrypted. A joining node inherits
the ability to mint certificates for the cluster it just joined.

**Attacker scenario.** Read access to any node's raft directory (backup, snapshot copy,
detached PV, `raftDir + ".bak.<ts>"` left behind by `makeJoinClusterFunc`
`app/cluster.go:366-373` and the eviction handler `:475-484` — those backups are never
deleted) yields the CA key. The holder mints certificates that every node accepts, forever,
with no revocation available (H-2).

**Remediation.** Keep `CAKeyPEM` off the replicated FSM — the CA key is only needed where
certificates are signed. If signing must be available on whichever node is leader, encrypt it
at rest with a key supplied out of band, or move signing behind an explicit leader-only
secret store. At minimum, stop replicating `CAKeyPEM` and `JoinToken` to non-voters and
document the `.bak.*` raft directories as credential-bearing.

---

## M-1 (Medium) — A peer certificate implies unrestricted FSM write authority

**Files**
- `backend/internal/cluster/forward.go:768-777` — `forwardApply` passes `req.GetCommand()` (opaque bytes) straight to `s.applyFn`, which is `raftStore.ApplyRaw` (`app/cluster.go:258-260, 284-286`)
- `backend/internal/cluster/forward.go:785-794` — `forwardVaultApply` does the same for any vault-ctl group
- `backend/internal/cluster/cluster.go:763-780` — the only authorization is "a verified chain exists"

**What it is.** There is no per-RPC authorization on the cluster service. Every method —
`ForwardApply`, `ForwardVaultApply`, `ForwardRemoveNode`, `ForwardSetNodeSuffrage`,
`NotifyEviction`, chunk pulls, segment pulls, `ForwardRPC` — is available to anything holding
the (single, shared) cluster certificate. `ForwardApply` in particular is an "apply arbitrary
bytes to the cluster FSM" primitive.

**Attacker scenario.** One compromised or rogue-enrolled node can delete every vault, rewrite
placements, evict every other node, demote voters, and read any chunk from any vault on any
node. This is the direct multiplier on C-1, H-1, H-2 and H-3: each of those yields peer
status, and peer status yields everything.

**Remediation.** Not all peer RPCs deserve equal trust. Gate membership-changing RPCs
(`ForwardRemoveNode`, `ForwardSetNodeSuffrage`, `NotifyEviction`) on the caller being a
current Raft member with a verified node identity (needs H-2 first). Validate/typed-decode
`ForwardApply` commands rather than treating them as opaque bytes, so a peer cannot submit a
command class the forwarding path is not supposed to originate.

---

## M-2 (Medium) — Bootstrap token endpoint and fetch permit plaintext HTTP

**Files**
- `backend/internal/server/server.go:344-380` — `/cluster/bootstrap-token` registered on the shared mux, served by the plain HTTP listener as well as HTTPS
- `backend/internal/app/bootstrap_token.go:206-237` — the joiner GETs an operator-supplied URL with no scheme requirement, sending the shared secret in a header

The handler itself is well built: GET-only, `subtle.ConstantTimeCompare` on the secret
(`server.go:361`), `Cache-Control: no-store`, rejects with 401. But over `http://` both the
shared secret and the returned join token cross the network in the clear.

**Attacker scenario.** A passive network observer on the bootstrap path captures the token
(and the gating secret, which is then reusable) and joins the cluster. Requires being on the
path during bootstrap or any later joiner's poll — the joiner polls repeatedly with backoff
(`:174-201`), widening the window.

**Remediation.** Reject non-`https` `--bootstrap-token-url` unless an explicit
`--bootstrap-token-insecure` opt-in is given, and register the endpoint only on the HTTPS
listener when TLS is enabled.

---

## M-3 (Medium) — Enroll token comparison is not constant-time and is not rate limited

**File** `backend/internal/app/cluster.go:209-216`

```go
if req.GetTokenSecret() != storedSecret { ... return nil, errors.New("invalid join token") }
```

Plain string comparison, where the rest of the codebase correctly reaches for
`subtle.ConstantTimeCompare` (`server/server.go:361`). There is also no attempt counter, no
backoff, and no rate limit on `/Enroll` — the one RPC deliberately exempted from mTLS
(`cluster.go:764`).

**Attacker scenario.** Brute force of 32 random bytes is infeasible, and a remote timing
attack on Go string comparison across a network is impractical — so exploitability is low on
its own. It matters as an unauthenticated, unmetered endpoint: an attacker with network reach
can hammer `/Enroll` indefinitely, generating an unbounded stream of
`logger.Warn("enroll: invalid token secret", ...)` lines with attacker-controlled `node_id`
(log flooding into the node's own pipeline), and gets a free oracle for "is this node a
cluster bootstrap node".

**Remediation.** `subtle.ConstantTimeCompare`, plus a per-source-IP rate limit and a bounded
`node_id` length before it reaches the log line.

---

## M-4 (Medium) — Docker ingester allows disabling TLS verification

**Files** `backend/internal/ingester/docker/docker.go:237-243`, `backend/internal/ingester/docker/factory.go:151-164`

`InsecureSkipVerify: !cfg.Verify`, driven by the `tls_verify` ingester param. Default is
verify-on (`factory.go:159` only flips it for the literal string `"false"`), and there is no
compensating callback when it is off — verification is genuinely disabled.

**Attacker scenario.** An operator who sets `tls_verify=false` on a TCP Docker endpoint
(a common copy-paste for self-signed daemon certs) exposes the connection to a MITM who can
then feed forged container logs into a vault, and read whatever the client sends. Requires
operator misconfiguration plus network position.

**Remediation.** Acceptable as an explicit operator opt-in, but the param should be
documented as unsafe and the ingester should emit a startup warning when it is set. The
better answer is that the CA-name path (`tls_ca`, already supported via the cert store)
covers the self-signed case, so `tls_verify=false` is rarely necessary.

---

## M-5 (Medium) — Several ingesters offer no TLS at all

**Verified by exhaustion:** the only non-test files under `backend/internal/ingester/`
importing `crypto/tls` are `docker/docker.go`, `relp/ingester.go`, `kafka/ingester.go`,
`mqtt/v3.go`, `mqtt/v5.go`. The `syslog`, `http`, `otlp`, `fluentfwd`, `tail`, `chatterbox`
and `scatterbox` ingesters have no TLS path.

**Attacker scenario.** Log data — which routinely carries credentials, tokens, PII and
internal hostnames — crosses the network in cleartext to the `syslog`, `http`, `otlp` and
`fluentfwd` listeners, and can be both read and **injected** (forged log records, an
integrity problem for anything using GastroLog for audit). Requires network position on the
producer→ingester path.

**Remediation.** Extend the pattern already proven in `relp.BuildTLSConfig` — cert-manager
`GetCertificate` callback, optional `tls_ca` for mutual TLS, optional CN ACL via
`VerifyConnection` — to the TCP/HTTP-based ingesters. It is written to be reusable almost
verbatim.

---

## L-1 (Low) — Enroll mTLS exemption is a method-name suffix match

`backend/internal/cluster/cluster.go:763-766`: `strings.HasSuffix(method, "/Enroll")` exempts
any method on any registered service whose name ends in `Enroll`. Today only
`/gastrolog.v1.ClusterService/Enroll` matches, so this is not currently exploitable, but the
guard silently widens the moment someone adds an `Enroll`-suffixed RPC (e.g. `ReEnroll`).
Compare against the full method string.

## L-2 (Low) — Predictable temp path when persisting cluster TLS

`backend/internal/cluster/clustertls.go:100-108` writes to `path + ".tmp"` with `0600` then
renames. `os.WriteFile` does not chmod an existing file, so a pre-created world-readable
`cluster_tls.json.tmp` in the home dir would receive the private key at its existing mode.
Requires local write access to the node home (`0750`, `home/home.go:146`). Use
`os.CreateTemp` + `Chmod` as `writeBootstrapTokenAtomic` already does
(`app/bootstrap_token.go:79-90`).

## L-3 (Low) — Abandoned raft directory backups are never cleaned up

`app/cluster.go:366-373` (runtime join) and `:475-484` (eviction reinit) rename the raft dir
to `raft.bak.<millis>` and never remove it. Those directories contain the replicated
`ClusterTLS` record, i.e. the CA private key and the join token (H-4), and accumulate on every
join/eviction cycle. Not a vulnerability by itself; it widens the window for H-4.

## L-4 (Low) — Kafka/MQTT TLS supports neither a custom CA nor client certificates

`kafka/ingester.go:81-83`, `mqtt/v3.go:31-34`, `mqtt/v5.go:82-86` build
`&tls.Config{MinVersion: tls.VersionTLS12}` — full verification against the system trust
store, `ServerName` supplied by the client libraries from the broker address. Correct as far
as it goes, but there is no way to pin a private CA or present a client certificate, so
operators with an internal broker CA have no option inside GastroLog. A functionality gap
that pushes operators toward unsafe workarounds rather than a flaw in itself.

---

## Checked and sound

- **`VerifyPeerCertificate` sweep — clean.** There are **zero** non-test uses of
  `VerifyPeerCertificate` anywhere in the repo. The recent migration is complete: both
  surviving custom-verification call sites use `VerifyConnection`
  (`cluster/clustertls.go:191`, `cluster/enrollclient.go:42`), as does the RELP CN ACL
  (`relp/ingester.go:306-309`). The `relp/cn_verifier_test.go:190-215, 280-310` tests pin the
  resumed-session behaviour explicitly, which is the right way to keep it from regressing.
  (H-1 is a flaw in what the enroll callback *does*, not in when it runs.)
- **`InsecureSkipVerify` sweep — 3 non-test sites, all accounted for.**
  `cluster/clustertls.go:190` (raft lane, compensated by a real chain verification at
  `:191-198` — sound: `Verify` against the cluster CA pool, correct `x509.VerifyOptions`,
  expiry enforced, chain re-verified on resumption); `cluster/enrollclient.go:41` (H-1);
  `ingester/docker/docker.go:243` (M-4). No `InsecureSkipVerify`, `--insecure`, or
  `NODE_TLS_REJECT_UNAUTHORIZED` anywhere in the frontend, deploy manifests, or scripts.
- **Minimum TLS version.** Cluster lanes and enrollment pin `tls.VersionTLS13`
  (`clustertls.go:155, 159, 182`, `enrollclient.go:45`) — the strongest choice, and it means
  no cipher-suite selection is needed (TLS 1.3 suites are not configurable in Go and the
  defaults are all AEAD). The HTTPS listener pins TLS 1.2 with
  `CurvePreferences: {X25519, CurveP256}` (`server/server_tls.go:73-74`); ingesters pin
  TLS 1.2. No `MaxVersion` downgrade, no SSLv3/TLS1.0 anywhere, no custom `CipherSuites` list
  that could reintroduce a weak suite.
- **`requireClientCert` on the service lane** (`cluster.go:745-780`) is correct: it reads
  `VerifiedChains` (which is only populated after the runtime verified the chain against
  `ClientCAs`), not merely `PeerCertificates`, and is chained onto both unary and stream
  paths. Its one gap is that it is not applied to the raft lanes (C-1).
- **RELP mutual TLS** (`relp/ingester.go:261-333`) is the model the other ingesters should
  follow: `RequireAndVerifyClientCert` whenever a CA is configured, a hot-reloading
  `GetCertificate` callback, config-time validation that the named cert exists, and an
  optional CN ACL implemented via `VerifyConnection` so it survives session resumption.
- **Cluster TLS file persistence** — `SaveFile` (`clustertls.go:87-109`) writes `0600` and
  renames atomically; `LoadFile` fails closed on a parse error. Node home is `0750`
  (`home/home.go:146`).
- **Bootstrap token file handling** (`app/bootstrap_token.go:74-107, 145-164`) — `CreateTemp`
  + explicit `Chmod(0600)` before the write, atomic rename, bounded reads
  (`maxBootstrapTokenBytes`) on both the file and HTTP sources so a hostile source cannot
  force unbounded allocation.
- **Bootstrap token endpoint authorization** (`server/server.go:355-379`) — GET-only,
  `subtle.ConstantTimeCompare`, `no-store`, 401 on mismatch, and the endpoint is not
  registered at all unless the operator sets a secret. (Its transport is M-2.)
- **Private keys are not exposed via the public API**: `SystemServer.GetCertificate`
  explicitly returns `KeyPem: ""` (`server/system_certs.go:89-96`), and the whole certificate
  RPC family is admin-gated (`auth/interceptor.go:140-143`). `GetClusterStatus`, which does
  return the join token, is likewise admin-gated (`auth/interceptor.go:98`).
- **Cert manager** (`cert/manager.go`) — keys held only as parsed `tls.Certificate` behind an
  `atomic.Pointer`, never logged (`:118, 127, 212, 217` log the cert *name* and the error,
  never PEM); `GetCertificate` fails closed by returning a nil certificate, which Go turns
  into a handshake failure rather than a fallback.
- **SNI demux parser** (`lane_listener.go:151-261`) — hand-rolled ClientHello parsing, but
  every step bounds-checks before indexing (`:180-196, 206-239, 242-260`) and it only selects
  a lane; it makes no trust decision. `Deliver` closes the connection when no lane matches
  (`:126-132`).
- **Raft group-ID confinement** (`multiraft/group_lane_api.go:31-37`) — each lane rejects RPCs
  whose `group_id` does not match the lane, so a peer cannot use one group's lane to drive
  another group's Raft. (This is confinement, not authentication — see C-1.)
- **Kafka/MQTT client TLS** — verification is on, TLS 1.2 minimum, `ServerName` supplied by
  the client libraries from the broker address; no `InsecureSkipVerify`. (L-4 is a
  capability gap, not a verification flaw.)

---

## Suggested fix order

1. **C-1** — per-SNI `ClientAuth` plus interceptor plumbing in the hand-written
   `MultiRaftTransportService` handlers; removes an unauthenticated
   remote-code-of-consensus path. Do this first. (Scoped as one-line-scale when filed —
   `fullInterceptors=true` alone turns out to change nothing; see the finding.)
2. **H-1** — replace `verifyCAFingerprint` with a real chain verification against the pinned CA.
3. **H-3** — stop logging the token; add expiry/single-use and a rotation path.
4. **H-2** — per-node certificates and identity enforcement. The largest change, and the
   prerequisite for M-1's authorization work and for any revocation story.
5. **H-4** — get the CA private key out of the replicated FSM.
