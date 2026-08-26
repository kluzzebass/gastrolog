# Synthesis — what the findings mean together

The eight lenses ran independently, but their findings collapse into six themes. Reading them as themes rather than as 68 separate defects is what makes the remediation order obvious: most of the Critical findings are consequences of two missing systems, not two dozen unrelated mistakes.

---

## Theme 1 — There is authentication, but no authorization

This is the systemic finding and it explains four of the seven distinct Criticals.

The entire authorization model is one allowlist map in one interceptor ([`auth/interceptor.go:88`](../../backend/internal/auth/interceptor.go)). Everything about it is fragile:

- **~20 privileged RPCs are simply absent from it** — `SetNodeState`, `YieldLeadership`, `PutCloudService`, `SetNodeStorageConfig`, `ArchiveChunk`, `TestHTTPLookup` — so any `role: "user"` account calls them freely.
- **There is no data-level authorization at all.** Any authenticated user reads every vault on the cluster via Search/Follow. `docs/vision.md:389` states "RBAC for authorization" and line 391 anticipates multi-tenant deployments, so this is a gap against stated intent, not merely an unbuilt feature.
- **An allowlist fails open by construction.** A new privileged RPC is unprotected unless someone remembers to add it. The next such RPC will have the same bug for the same reason.

The fix is not "add the missing entries." It is to invert the model — deny by default, declare the requirement at the handler, and add a vault-scoped check for data reads — so that forgetting is not a vulnerability.

## Theme 2 — The highest-privilege transport has no authentication

The per-group Raft lanes are built with `fullInterceptors=false` ([`cluster/cluster.go:685`](../../backend/internal/cluster/cluster.go)), which is exactly the flag that installs the mTLS interceptor ([`cluster.go:716-721`](../../backend/internal/cluster/cluster.go)), and the listener's TLS config uses `tls.VerifyClientCertIfGiven` ([`cluster/clustertls.go:154`](../../backend/internal/cluster/clustertls.go)) — which accepts a connection presenting no client certificate at all.

hashicorp/raft does not authenticate peers; it trusts its transport completely. For these lanes that trust is unfounded. Anyone with TCP reach commits arbitrary entries to the replicated FSM — users, roles, cloud credentials, the JWT signing secret — or destroys availability through forged elections.

Two lenses found this independently. It is the single most serious finding in the audit.

Around it sits a weak identity story (Theme 2b): one certificate and one private key for the whole cluster, so no node has a distinct identity and any peer certificate implies unrestricted FSM authority; a static, never-rotating join token printed to logs on every boot; the cluster CA private key replicated to every node in the FSM; and an enrollment callback that fingerprint-matches a chain member without verifying the leaf against it. Fixing the missing interceptor closes the open door; fixing identity is what makes the lock meaningful.

## Theme 3 — Untrusted input is unbounded, and nothing contains a panic

Every ingester is an unauthenticated network listener (only RELP offers optional mTLS). Across them:

- Wire-supplied lengths reach allocators unchecked — RELP's `DATALEN` ([`relp/protocol.go:130`](../../backend/internal/ingester/relp/protocol.go)) is the clearest case.
- `bodyutil.ReadBody`'s `maxBytes` bounds the *compressed* input only, so a zstd bomb expands without limit.
- Syslog TCP framing has no line-length bound.
- Only the HTTP (Loki) ingester caps attribute count and length; OTLP, Fluent Forward, and Kafka do not.
- RELP and Fluent Forward have no connection limits and no read deadlines.

And the multiplier: **there is no `recover()` anywhere in the ingester package**, nor in the detached query goroutines. A panic in one connection handler does not drop that connection — it terminates the process, and with it every vault, every Raft group, and every other tenant's ingest on that node. This turns a family of Medium memory bugs into remote unauthenticated node-kill.

Two independent remediations: bound the inputs, and contain the blast radius so the next unbounded input that slips through costs one connection instead of the node.

## Theme 4 — Secrets are handed out and stored in the clear

The same file demonstrates both the right and wrong pattern. MaxMind credentials are returned as a boolean `LicenseConfigured` flag ([`server/system.go:242`](../../backend/internal/server/system.go)) — correct. Cloud storage credentials two structs away ([`system.go:452-456`](../../backend/internal/server/system.go)) return `AccessKey`, `SecretKey`, `ConnectionString`, and `CredentialsJson` in plaintext to any authenticated caller, on every config read *and* every config write.

Combined with Theme 1, a low-privilege account is a credential-exfiltration path to the object store. Supporting findings: the JWT signing secret and TLS private keys sit unencrypted in the replicated config store (and therefore in every node's WAL, snapshots, and backups), secrets pass as literal CLI flags visible in `ps`, and the file permissions the code's own comment cites as mitigation are not actually applied.

## Theme 5 — Log content reaches operators unescaped

Log records are attacker-controlled data by definition — that is the product's entire purpose. Every ECharts tooltip formatter injects log-derived values into `innerHTML` without escaping, so anyone who can get a line ingested runs script in an operator's browser. The JWT and refresh token live in `localStorage`, readable by that script, and there is no Content-Security-Policy to blunt it. `react-markdown` additionally runs with URL sanitization disabled.

This is the classic log-viewer XSS, and it is worth stating plainly: for a log aggregator, "untrusted input renders in the operator UI" is not an edge case, it is the main path.

## Theme 6 — Integrity is corruption-detection, not tamper-evidence

CRC32 on WAL records and unkeyed SHA-256 on GLCB chunks detect *accidental* corruption. Neither is evidence against an adversary with disk or blob-store write access, who recomputes them freely. Sharpest instance: peer-to-peer replica pulls accept a chunk on matching record count alone ([`glcb_catchup.go:281`](../../backend/internal/orchestrator/)), never checking the SHA-256 the manifest already carries — a compromised peer serves tampered content undetected, and the digest to catch it is *already there, unused*.

Whether to go further (keyed MACs, signed manifests) is a product decision about threat model. Using the digest that already exists is not.

---

## Remediation order

Ranked by exploitability × blast radius, not by finding count.

### P0 — remote or low-privilege compromise, fix before anything else

1. **Raft lanes require no client certificate** (Theme 2). Pass `fullInterceptors=true` for the lane servers and move the listener to `RequireAndVerifyClientCert`. Small change, largest payoff. *Interim mitigation available today: firewall the cluster ports, which currently bind to all interfaces.*
2. **Chart-tooltip XSS → token theft** (Theme 5). Escape log-derived values in every formatter; add CSP; reconsider `localStorage` for tokens.
3. **Unauthenticated remote node kill** (Theme 3). Bound RELP `DATALEN`, cap decompressed size, bound syslog lines — and add panic containment at every listener goroutine and detached query goroutine.
4. **Cloud credentials returned in plaintext** (Theme 4). Apply the `LicenseConfigured` pattern already used for MaxMind.

### P1 — authorization and identity, the systems whose absence caused P0

5. **Invert the authorization model** (Theme 1): deny by default, per-handler declared requirements, plus vault-scoped read authorization.
6. **Cluster identity** (Theme 2b): per-node certificates, rotating join tokens, CA key handling, enrollment leaf verification.
7. **Resource bounds for authenticated users** (Theme 3/7): aggregation memory caps, HTTP server timeouts, SSRF validation on `TestHTTPLookup` and HTTP lookup tables.
8. **Dependency vulnerabilities**: 10 reachable Go CVEs including a gRPC authorization bypass fixed one patch release away; plus `mermaid`/`echarts` advisories.

### P2 — hardening and hygiene

9. Integrity: use the existing digest on peer pulls; decide the tamper-evidence threat model deliberately (Theme 6).
10. Config-store encryption at rest, file modes, secrets off the command line.
11. Response headers (CSP, HSTS), error-text leakage, login timing side-channel, open redirect, CI `permissions:` blocks.

---

## What was checked and found sound

Recorded so the next reviewer knows where not to spend effort. Each lens file carries its full list; the highlights:

- No remaining `VerifyPeerCertificate` uses anywhere in the repo — the resumption-bypass class that triggered this audit is closed.
- Password hashing, JWT algorithm handling, and signature verification are sound.
- Regex handling is bounded; RE2 plus the existing limits make regex DoS a non-issue.
- Query-to-storage paths do not construct shell commands or unescaped external calls.
- Vault path construction is scoped; no cross-vault data leakage through chunk or index paths was found (the leakage is in *authorization*, not in path handling).
- TLS 1.3 minimum is enforced on cluster transports.
- The `admin`/`admin123` dev default cannot reach a production manifest.
