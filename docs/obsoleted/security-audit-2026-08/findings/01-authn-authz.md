# Security Audit — Lens 01: Authentication & Authorization

Read-only review. Every claim below is cited `file:line` and marked **Verified by reading the code** or **Suspected — needs a test**.

Scope covered: `backend/internal/auth/**`, `backend/internal/server/**` handler surface (all 104 Connect procedures enumerated and cross-checked against the interceptor's access maps), `backend/internal/app/bootstrap_token.go`, `backend/internal/app/initial_admin.go`, `backend/internal/cluster/**` peer identity, and the non-Connect HTTP routes.

**Threat model note used for severity:** the Connect/HTTP API listens on `:4564` by default (`backend/cmd/gastrolog/main.go:180`) — all interfaces. "Any authenticated user" below means a holder of a `role: "user"` account, i.e. the lowest-privilege identity the product can issue (`backend/internal/server/auth.go:440-443`).

---

## Severity summary

| Severity | Count |
|---|---|
| Critical | 1 |
| High | 4 |
| Medium | 5 |
| Low | 5 |

---

## CRITICAL

### C-1. Raft transport lanes accept connections with no client certificate — unauthenticated Raft injection

**Verified by reading the code.**

Evidence:
- `backend/internal/cluster/clustertls.go:154` and `:158` — the cluster server's `tls.Config` uses `ClientAuth: tls.VerifyClientCertIfGiven`. A client presenting **no** certificate completes the handshake; only a client presenting a *bad* certificate is rejected.
- `backend/internal/cluster/cluster.go:709-728` — `baseServerOpts(maxRecv, fullInterceptors bool)`. The mTLS-enforcing interceptors are attached **only when `fullInterceptors` is true** (`cluster.go:716-721`).
- `backend/internal/cluster/cluster.go:685-687` — `EnsureRaftGroupLane` calls `s.baseServerOpts(maxRaftLaneRecvBytes, **false**)` and then chains only `pauseUnaryInterceptor` / `pauseStreamInterceptor`.
- `backend/internal/cluster/cluster.go:660` — `EnsureRaftGroupLane(ConfigGroupID)` is called for the cluster-ctl group at startup, and one lane is created per vault-ctl Raft group.
- Contrast: the service lane does enforce it — `cluster.go:633` and `:649` pass `true`, and `requireClientCert` (`cluster.go:762-780`) checks `len(tlsInfo.State.VerifiedChains) == 0`.

So the service lane (ForwardRPC, ForwardApply, chunk transfer, managed-file streaming) is properly mTLS-gated, but **every Raft lane is not**. The SNI demuxer routes on the ClientHello ServerName (`gastrolog-raft.<group>`), which is attacker-chosen and requires no credential.

**What an attacker does:** From any host with TCP reach to the cluster port (default `:4566`, `main.go:183`), open a TLS connection with SNI `gastrolog-raft.config`, present no client certificate, and speak the Raft transport gRPC service directly: `RequestVote` with a high term to force the real leader to step down (cluster-wide availability loss), or `AppendEntries`/`InstallSnapshot` to write arbitrary entries into the cluster-ctl FSM — which is where users, roles, cloud credentials, vault placement and the JWT secret live. That is full cluster compromise from network reach alone, with no credential of any kind.

**Exploitability:** requires network reach to the cluster port. In the local dev layout and in a flat Kubernetes pod network that is any workload in the cluster; it is *not* limited to already-root-on-a-node. This is the highest-impact finding in this lens.

**Remediation:** put the peer-cert requirement in the TLS config, chosen per connection from the ClientHello SNI in `GetConfigForClient`. Raft lane SNIs get `ClientAuth: tls.RequireAndVerifyClientCert`; every other SNI reaches the service lane and keeps `VerifyClientCertIfGiven`, because the relaxation exists for `Enroll` — how a joining node obtains the certificate it does not yet have. SNI is already the demux key, so the two policies coexist on one port; tightening the whole listener would lock new nodes out.

Attaching `mTLSUnaryInterceptor` / `mTLSStreamInterceptor` to the Raft lanes is worth doing as defence in depth, but **it is not sufficient by itself** and is not the one-line change it appears to be: the `MultiRaftTransportService` handlers are hand-written `grpc.MethodDesc` entries whose unary handlers discarded the interceptor argument gRPC passes them, leaving every server-level unary interceptor on that service inert. Fix the handlers to dispatch through the chain (as `forward.go`'s `ClusterService` handlers do) or the interceptor never runs.

Add a test that dials a Raft lane with no client cert and asserts the handshake or first RPC fails — and, because the TLS layer now rejects first, a separate test that pins the interceptor chain itself, or this exact defect regresses silently.

---

## HIGH

### H-1. The interceptor's admin allowlist is the *only* authorization in the system, and ~20 privileged RPCs are missing from it

**Verified by reading the code.**

Evidence:
- `backend/internal/auth/interceptor.go:210` — `if i.admin[procedure] && claims.Role != "admin"`. This is an **allowlist**: a procedure absent from the `admin` map requires only a valid token, any role.
- `grep 'claims.Role|Role != "admin"' backend/internal --exclude _test.go` returns exactly one production hit: `interceptor.go:210`. **No handler performs its own role check.** A missing map entry is therefore a total loss of authorization for that RPC, with nothing behind it.

Cross-checking all 104 generated procedure constants (`backend/api/gen/gastrolog/v1/gastrologv1connect/*.go`) against the `public` and `admin` maps, the following privileged procedures fall through to "any authenticated user":

Cluster control:
- `LifecycleServiceSetNodeStateProcedure` — handler at `backend/internal/server/lifecycle.go:333-359`. Writes node state to the cluster-ctl Raft store with no role check; accepts `DECOMMISSIONING` / `DRAINING` for **any** node ID.
- `LifecycleServiceYieldLeadershipProcedure` — `backend/internal/server/lifecycle.go:469-488`. Forces a Raft leadership transfer.

Infrastructure and credential mutation:
- `SystemServicePutCloudServiceProcedure` — `backend/internal/server/system_storage.go:21-46`. Writes cloud-storage credentials (bucket, endpoint, access key, secret key) with no role check.
- `SystemServiceDeleteCloudServiceProcedure`, `SystemServiceTestCloudServiceProcedure`.
- `SystemServiceSetNodeStorageConfigProcedure` — `backend/internal/server/system_storage.go:124-149`. Rewrites a node's storage layout.
- `SystemServicePutLogLevelsProcedure`, `SystemServiceDeleteLookupProcedure`.

Data-plane destructive:
- `VaultServiceArchiveChunkProcedure`, `VaultServiceRestoreChunkProcedure`, `VaultServiceRepatriateOrphanProcedure`, `VaultServiceRetryUnreadableChunksProcedure`, `VaultServiceReconcileCloudIndexProcedure`.

The asymmetry is itself the tell: `SystemServicePutVaultProcedure`, `PutIngesterProcedure`, `PutCertificateProcedure`, `PutLookupSettingsProcedure` **are** admin-gated (`interceptor.go:123-146`) while their peers above are not. This reads as drift — new RPCs were added without a corresponding map entry — not as a deliberate privilege model.

**What an attacker does:** with any `role: "user"` account (the role an operator hands to a developer who only needs to read logs), call `PutCloudService` to repoint archival storage at an attacker-controlled S3 endpoint and exfiltrate every archived chunk; or call `SetNodeState` on each node in turn to decommission the cluster; or `ArchiveChunk`/`RepatriateOrphan` to move data around. None of these require admin.

**Exploitability:** any low-privilege account holder, over the network. High.

**Remediation:** invert the model — default-deny. Make the interceptor require an explicit access-level declaration per procedure and **fail closed** (reject with `Unimplemented`/`PermissionDenied`) when a procedure is in neither map, then add a test that enumerates the generated procedure constants and asserts every one is classified. That converts "someone forgot" from a silent authz hole into a failing test. `backend/internal/server/routing/routes.go` already demonstrates the per-procedure-table pattern in this codebase.

### H-2. `--no-auth` grants unauthenticated synthetic admin on the public listener, with no guard against non-dev use

**Verified by reading the code.**

Evidence:
- `backend/cmd/gastrolog/main.go:182` — `serverCmd.Flags().Bool("no-auth", false, "disable authentication (all requests treated as admin)")`. A plain boolean; no environment gate, no loopback-only requirement, no refusal to combine with a non-loopback `--listen`.
- `backend/cmd/gastrolog/main.go:180` — `--listen` defaults to `:4564` (all interfaces).
- `backend/internal/app/app.go:1458-1462` — `buildAuthTokens` returns `(nil, nil)` when `noAuth`, logging only at `Info` level.
- `backend/internal/server/server.go:599-602` — `case s.noAuth:` installs `&auth.NoAuthInterceptor{}` on the **public TCP mux**, ahead of the `s.tokens != nil` case.
- `backend/internal/auth/interceptor.go:20-27` — `noAuthClaims()` returns `Role: "admin"`, `Subject: "admin"`, `ExpiresAt: 2099-01-01` for **every** request.
- `scripts/cluster.sh:113` — the dev cluster passes `--no-auth` by default (`cluster.sh:17`).

**What an attacker does:** if the flag reaches a non-dev environment (copied systemd unit, container `command:` carried from a compose file, a `just` recipe reused in staging), anyone who can reach `:4564` is admin: read all logs, dump cloud credentials via `GetSystem` (`system.go:445-462` returns `AccessKey`/`SecretKey`/`ConnectionString`/`CredentialsJson` in plaintext), create users, shut down nodes.

**Exploitability:** requires operator misconfiguration, but the flag is designed to be routinely used (the project's own dev cluster runs with it) and nothing distinguishes a dev invocation from a production one. High as a defense-in-depth failure.

**Remediation:** refuse to start when `--no-auth` is combined with a `--listen` that resolves to anything but loopback, unless a second explicit flag (e.g. `--i-understand-no-auth-is-insecure`) is also present; log the disablement at `Warn`/`Error`, not `Info`; and surface it in `/readyz` output and the UI banner (`AuthDisabled` is already plumbed at `backend/internal/server/auth.go:405-410` — use it more loudly).

### H-3. Managed-file upload endpoint: no role check, no revocation check

**Verified by reading the code.**

Evidence: `backend/internal/server/upload.go:35-52`, registered at `upload.go:256` as `POST /api/v1/managed-files/upload`.

```go
if !s.noAuth && s.tokens != nil {
    token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
    if token == "" { ... 401 }
    if _, err := s.tokens.Verify(token); err != nil { ... 401 }
}
```

Three defects in nine lines:
1. **No role check.** The Connect twins are admin-gated — `SystemServiceListManagedFilesProcedure` and `SystemServiceDeleteManagedFileProcedure` are in the admin map (`interceptor.go:145-146`). So a `user` can *write* managed files but not list or delete them. Uploads are committed to Raft and replicated cluster-wide (`upload.go:102`, `RegisterFile`).
2. **No revocation check.** It calls `s.tokens.Verify` directly and never consults `TokenValidator.IsTokenValid` (`backend/internal/server/server_client.go:21-37`). A token invalidated by logout-equivalent flows, password reset, role downgrade, or user deletion still uploads until its natural expiry.
3. **`strings.TrimPrefix` instead of `strings.CutPrefix`.** Unlike the interceptor (`interceptor.go:224`), a header with no `Bearer ` prefix is not rejected — the raw header value is passed to `Verify` as the token. Harmless in practice (it will fail to parse), but it is a divergence from the one correct implementation.
4. **Fails open when `s.tokens == nil`** — see L-2.

**What an attacker does:** a low-privilege user, or anyone holding a token that was supposed to have been revoked, uploads 256 MB files (`maxUploadSize`, `upload.go:24`) repeatedly. Each is written to `<home>/managed-files` and its metadata replicated to every node. Disk exhaustion is cluster-wide, and the uploader cannot clean up afterwards (delete is admin-only), so recovery needs an operator.

**Remediation:** route this endpoint's auth through the same code path as the interceptor — extract `verifiedClaims` into a reusable helper and require `role == "admin"` to match `ListManagedFiles`/`DeleteManagedFile`.

### H-4. `GetSettings` is a public procedure that returns secrets to any authenticated caller

**Verified by reading the code.**

Evidence:
- `backend/internal/auth/interceptor.go:86` — `SystemServiceGetSettingsProcedure: true` in the **public** map, with the comment "password policy needed on register page".
- `backend/internal/auth/interceptor.go:186-188` — public procedures take the `bestEffortClaims` path, which attaches claims when a valid token is present but **never checks role**.
- `backend/internal/server/system.go:508-522` — unauthenticated callers correctly get only the password policy. Good.
- `backend/internal/server/system.go:524` — **any** caller with claims (role `user` included) reaches `buildFullSettingsResponse(ctx, req.Msg.IncludeSecrets)` with a client-controlled `IncludeSecrets`.
- `backend/internal/server/system.go:262-265` — `if includeSecrets { mm.AccountId = ...; mm.LicenseKey = ss.MaxMind.LicenseKey }`.
- `backend/internal/server/system.go:283-290` and `httpLookupsToProto` at `system.go:1052-1074` — the response also carries `Headers: l.Headers` for every configured HTTP lookup. Lookup headers are the natural place for `Authorization: Bearer <api-key>` on a third-party enrichment API.

Note the asymmetry: `PutLookupSettings` and `PutMaxMindSettings` **are** admin-gated (`interceptor.go:128-129`), so a non-admin cannot write these but can read them back.

**What an attacker does:** any `user` account calls `GetSettings{include_secrets: true}` and receives the MaxMind account ID and license key plus every HTTP-lookup header value, TLS cert names, and the full cluster settings block.

**Exploitability:** any low-privilege account, one RPC, no preconditions. High.

**Remediation:** split the procedure. Keep a genuinely public `GetPasswordPolicy` for the register page, and move `GetSettings` into the admin map. If a non-admin genuinely needs some settings, return a separately-built, explicitly-enumerated non-secret projection rather than gating a superset on a client-supplied boolean.

---

## MEDIUM

### M-1. `Logout` does not revoke the access token

**Verified by reading the code.**

Evidence: `backend/internal/server/auth.go:660-684`. `Logout` deletes only the refresh token row matching the token the client sent; it never calls `InvalidateTokens`. The revocation machinery exists and is used by `ChangePassword` (`auth.go:390`), `ResetPassword` (`auth.go:570`), `UpdateUserRole` (`auth.go:522`) and `RenameUser` (`auth.go:615`) — but not by logout.

This directly contradicts the documented contract at `backend/internal/auth/interceptor.go:54-56`: "used for server-side token revocation (e.g. after **logout**, password change, or role change)."

**What an attacker does:** having captured a JWT (shared workstation, browser storage, a proxy log), continues to use it after the victim clicks "log out" — for the full remaining lifetime of the token, up to `TokenDuration` (default `1h`, `backend/internal/system/bootstrap.go:122`, but operator-settable to anything ≥ 1 minute per `system.go:1331`).

**Exploitability:** requires prior token capture. The severity is that logout does not do what the user and the code's own comment believe it does.

**Remediation:** the honest fix is to make `Logout` a per-session revocation. Since `TokenInvalidatedAt` is per-user and would kill all sessions, either accept that (simplest, matches the other flows) or bind the access token to its refresh-token session ID (add a `sid` claim, check it against the live refresh-token row) so a single session can be revoked. Do not leave the comment claiming logout revokes when it does not.

### M-2. `TestHTTPLookup` is a full SSRF primitive available to any authenticated user

**Verified by reading the code.**

Evidence: `backend/internal/server/system.go:1480-1516`. `UrlTemplate` and `Headers` come straight from the request, are handed to `lookup.NewHTTP(lcfg)` and fetched with no scheme, host, or private-range restriction. `result := h.TestFetch(ctx, req.Msg.Values)` and the extracted fields are returned to the caller at `system.go:1510-1515`. The procedure is absent from the admin map (see H-1).

**What an attacker does:** a `user` account calls `TestHTTPLookup{url_template: "http://169.254.169.254/latest/meta-data/iam/security-credentials/..."}` and reads the response through `ResponsePaths`. On EC2/GCE this yields node IAM credentials; on any deployment it enumerates internal services reachable from the node.

**Exploitability:** any low-privilege account. Rated Medium rather than High only because the response is filtered through `ResponsePaths` (JSON path extraction), which constrains but does not prevent exfiltration.

**Remediation:** admin-gate it (fixes the authorization half), and independently block requests to loopback, link-local (`169.254.0.0/16`, `fd00::/8`), and RFC1918 destinations unless an operator opts in — the SSRF risk exists for admins too.

### M-3. `WatchIngesterStatus` is non-admin while `GetIngesterStatus` is admin

**Verified by reading the code.**

Evidence: `backend/internal/auth/interceptor.go:118` gates `SystemServiceGetIngesterStatusProcedure` as admin; `SystemServiceWatchIngesterStatusProcedure` appears in neither map. Same shape as `GetSystem` (admin, `interceptor.go:117`) vs `WatchSystem` (ungated).

Mitigating: I read `WatchSystem` at `backend/internal/server/system.go:872-904` and it streams **only** `ClusterCtlRaftIndex` — a monotonic counter, not config. So `WatchSystem` itself leaks only "configuration changed, and how often". `WatchIngesterStatus` I did not read to the same depth.

**Suspected — needs a test:** whether `WatchIngesterStatus` streams the same payload `GetIngesterStatus` returns. If it does, this is an authz bypass by streaming twin and should be re-rated.

**Remediation:** classify streaming procedures at the same access level as their unary twin, as a rule. The default-deny change in H-1 makes this automatic.

### M-4. Revocation check is second-granularity and can miss a same-second invalidation

**Verified by reading the code.**

Evidence:
- `backend/internal/auth/jwt.go:63` — `IssuedAt: jwt.NewNumericDate(now)`. JWT `NumericDate` serializes to **integer seconds**; sub-second precision is lost on the round trip.
- `backend/internal/server/server_client.go:33` — `if !user.TokenInvalidatedAt.IsZero() && !issuedAt.After(user.TokenInvalidatedAt)`.

`TokenInvalidatedAt` is stored with full `time.Time` precision (`auth.go:389` `time.Now().UTC()`). A token issued at `12:00:00.900` serializes `iat = 12:00:00.000`; an invalidation at `12:00:00.100` then satisfies `!issuedAt.After(invalidatedAt)` — so this direction fails **closed** (the token is rejected), which is correct.

The reverse: invalidation at `12:00:00.900`, token issued at `12:00:00.100` → `iat = 12:00:00`, `12:00:00.After(12:00:00.900)` is false → rejected. Also closed.

A token issued at `12:00:01.100` after an invalidation at `12:00:00.900` → `iat = 12:00:01`, `After` is true → accepted. Correct.

**Conclusion: the comparison direction is right and the truncation biases toward rejection.** I am recording this as checked-and-sound rather than a vulnerability, but flagging it Medium-visibility because the invariant is load-bearing, subtle, and undocumented — the `!issuedAt.After(...)` (rather than `issuedAt.Before(...)`) is precisely what makes the truncation safe, and a future "cleanup" to `Before` would silently open a one-second window.

**Remediation:** add a comment at `server_client.go:33` stating that `iat` is second-truncated and the non-strict comparison is deliberate, plus a test pinning the same-second case.

### M-5. `RefreshToken` rotation is not atomic — a concurrent-use race

**Suspected — needs a test.**

Evidence: `backend/internal/server/auth.go:268-339`. The flow is read (`GetRefreshTokenByHash`, `:279`), several validations, then delete (`DeleteRefreshToken`, `:317`), then issue new (`:322`, `:328`). There is no compare-and-delete and no transaction spanning the lookup and the delete.

Two concurrent requests carrying the same refresh token can both pass the `stored == nil` check at `:283` before either reaches the delete at `:317`, and both would then be issued fresh access+refresh pairs. That converts a stolen refresh token from "detectably used once" into "silently usable twice", and defeats the reuse-detection property that token rotation exists to provide (a legitimate rotation and a stolen-token rotation both succeed, so neither is noticed).

**Exploitability:** requires the attacker to hold a stolen refresh token and race the legitimate client. Medium.

**Remediation:** make the delete the concurrency gate — a delete-if-exists that reports whether it removed a row, and only proceed when it did. If the store cannot express that, add a token-family/reuse-detection column and invalidate the whole family when a consumed token is presented again. Needs a concurrent test to confirm the race is real against the actual Raft-backed store.

---

## LOW

### L-1. `--initial-admin-password` passed as a command-line flag

**Verified by reading the code.** `backend/cmd/gastrolog/main.go:201`. The password appears in `/proc/<pid>/cmdline`, `ps` output, and any process-listing telemetry, readable by every local user. The file-based path (`--initial-admin-file`, `main.go:199`, read at `backend/internal/app/initial_admin.go:149-185`) is the safe alternative and is already implemented.

**Exploitability:** requires local-process-listing access on the node. Low.

**Remediation:** document `--initial-admin-file` as the supported path and log a warning when the flag form is used. Consider reading from an env var instead, which at least is not in `ps` by default.

### L-2. Both auth-enforcement sites fail **open** when `tokens == nil`

**Verified by reading the code.**

- `backend/internal/server/server.go:608-615` — the `default:` branch attaches an error-logging interceptor and (in cluster mode) the routing interceptor, but **no auth interceptor at all**. Every procedure is then unauthenticated.
- `backend/internal/server/upload.go:42` — `if !s.noAuth && s.tokens != nil` skips the check entirely when `tokens` is nil.

In the production path this is currently unreachable: `buildAuthTokens` (`app.go:1458-1468`) returns a non-nil service or an error whenever `NoAuth` is false, and `buildTokenService` errors out on a missing secret (`app.go:1837-1839`). The comment at `server.go:609` says "tests without NoAuth flag".

**Exploitability:** none today — this is a latent fail-open, not a live hole. Low. But it is the wrong default: a future refactor that lets `tokens` be nil on any production path silently disables all authentication rather than refusing to start.

**Remediation:** make `server.New` reject a configuration with `NoAuth == false && tokens == nil`, and have tests pass an explicit `NoAuth: true` (most already do — e.g. `cmd/gastrolog/cli/cloud_service_test.go:36`).

### L-3. Rate limiter keys on `RemoteAddr`, ignoring proxy headers — both directions are wrong behind a proxy

**Verified by reading the code.** `backend/internal/server/ratelimit.go:105-108` splits `r.RemoteAddr`. Limits are 5 req/min, burst 5, on `Login` and `Register` only (`ratelimit.go:17-20`, `server.go:340`).

Behind a reverse proxy every request shares the proxy's IP, so (a) one attacker's brute-force attempts lock out all legitimate users from one shared bucket, and (b) the per-attacker limit is effectively meaningless since the bucket is shared anyway. Not honoring `X-Forwarded-For` is the *correct* default when the proxy is untrusted, so this is a deployment-documentation gap rather than a code bug.

**Exploitability:** DoS of the login endpoint for co-located users. Low.

**Remediation:** document that GastroLog must not sit behind an untrusted proxy for rate limiting to be meaningful, or add an explicit `--trusted-proxies` CIDR list that, when matched, permits `X-Forwarded-For` parsing. Also consider a per-username limiter alongside the per-IP one so distributed credential-stuffing against one account is bounded.

### L-4. JWT secret is stored unencrypted in the config store

**Verified by reading the code.** `backend/internal/system/bootstrap.go:100-115` documents this explicitly and states the mitigation (filesystem permissions, full-disk encryption). Read access to the Raft config store lets an attacker forge tokens for any user and role.

**Exploitability:** requires read access to the node's data directory — i.e. already-on-the-box. **Low by the stated rule.** Recording it because the accepted-risk note is present in the code and the audit should confirm it is a conscious decision, which it is.

Positive notes on the surrounding machinery: the secret is 32 bytes from `crypto/rand` (`bootstrap.go:110-113`), `RegenerateJwtSecret` (`backend/internal/server/system.go:737-784`) swaps the live secret atomically via `TokenService.SetSecret` (`jwt.go:48-51`, `atomic.Pointer`) **and** stamps `TokenInvalidatedAt` on every user, so rotation is complete rather than cosmetic. That is well done.

### L-5. Node ID arrives in a client-supplied gRPC metadata header

**Verified by reading the code.** `backend/internal/cluster/peer_bytes.go:20` defines `NodeIDMetadataKey = "x-gastrolog-node-id"`; it is set client-side at `peer_conn_manager.go:796-802` and read server-side by `peerIDFromIncoming` (`peer_bytes.go:233-241`).

I traced every consumer: the value is used **only** for per-peer byte-transfer accounting in `peer_bytes.go`. No authorization, routing, or Raft decision reads it. A peer that has already passed mTLS could misattribute its own byte counters to another node — a metrics-integrity issue, not an authz one.

**Exploitability:** requires a valid cluster client certificate. Low.

**Remediation:** none required. If the header ever starts feeding a trust decision, derive the node ID from the peer certificate's SAN instead.

---

## What I checked and found sound

Recording the negative space so it does not get re-audited from scratch.

- **JWT algorithm confusion — sound.** `backend/internal/auth/jwt.go:79-84`: the keyfunc asserts `t.Method.(*jwt.SigningMethodHMAC)` before returning the secret, so `alg: none` and RS256-key-confusion are both rejected. The library is `golang-jwt/jwt/v5`, whose `ParseWithClaims` validates `exp` and `nbf` by default and rejects a token whose signature does not verify. `token.Valid` is re-checked at `jwt.go:90`. Tokens are HS256 (`jwt.go:67`) with a 32-byte random secret.
- **`Register` cannot be re-triggered — sound, belt and braces.** `backend/internal/server/auth.go:157-165` returns `FailedPrecondition` when `CountUsers > 0`, independently of the interceptor. The interceptor's first-boot branch (`interceptor.go:196-201`) blocks every other procedure while `count == 0`. Both layers agree.
- **Password hashing — sound.** `backend/internal/auth/password.go:15-21`: argon2id, m=64 MiB, t=3, p=4, 32-byte key, 16-byte `crypto/rand` salt — at or above current OWASP guidance. `VerifyPassword` uses `subtle.ConstantTimeCompare` (`password.go:49`). `parsePHC` (`password.go:53-91`) rejects a non-`argon2id` algorithm, zero-valued parameters, and empty salt/hash — so an attacker who could write a password hash still cannot force a trivially-cheap or always-matching verification.
- **Login username enumeration — sound.** `backend/internal/server/auth.go:236` and `:244` both return the identical `"invalid credentials"` for unknown-user and wrong-password. (Timing does differ — the unknown-user path skips argon2 entirely — but at 64 MiB/t=3 the argon2 cost is large and constant, so the signal is a clean binary rather than something requiring statistical extraction. Worth a constant-time dummy hash eventually; not filed as a finding.)
- **Cluster service lane mTLS — sound.** `backend/internal/cluster/cluster.go:762-780`: `requireClientCert` checks `len(tlsInfo.State.VerifiedChains) == 0`, correctly rejecting the no-certificate case that `VerifyClientCertIfGiven` allows through the handshake. The `/Enroll` exemption is a suffix match on `info.FullMethod`, which is safe because gRPC dispatches only registered method names — an attacker cannot invent a method ending in `/Enroll`. Applied to both unary and streaming (`cluster.go:745-760`). See C-1 for the lanes where this interceptor is *not* attached.
- **Bootstrap token endpoint — sound.** `backend/internal/server/server.go:349-380`: the endpoint is registered only when the operator sets both the secret and the token function (`server.go:350-352`); the shared secret is compared with `subtle.ConstantTimeCompare` (`server.go:361`); rejections are logged with the remote address; the response sets `Cache-Control: no-store`. The client side (`backend/internal/app/bootstrap_token.go:206-237`) bounds the response read at 1 KiB and treats 401/403 as fatal rather than retrying forever. File delivery writes atomically at mode `0600` (`bootstrap_token.go:74-107`).
- **Enrollment TOFU — sound by design.** `backend/internal/cluster/enrollclient.go:39-49` uses `InsecureSkipVerify` with a `VerifyConnection` callback that pins the CA fingerprint carried in the join token. That is the correct construction for first-contact enrollment: the fingerprint, not the hostname, is the trust anchor. The comment at `enrollclient.go:39` explains why `VerifyConnection` rather than `VerifyPeerCertificate` — accurate.
- **Interceptor covers streaming.** `backend/internal/auth/interceptor.go:165-173` wraps `StreamingHandler` with the same `authenticate` call as unary (`:154-162`), so streaming RPCs are not an authentication bypass. (They can still be an *authorization* bypass via a missing admin-map entry — see M-3.)
- **Auth runs before routing.** `backend/internal/server/server.go:604-607` builds the interceptor slice as `[errorLog, auth, routing…]`; Connect applies the first as outermost, so the auth check completes before the routing interceptor can forward the call to a peer. Forwarded calls execute on the target under `NoAuthInterceptor` (`server.go:813-822`), which is acceptable *because* the entry node already enforced the role — but it means H-1's missing entries are exploitable cluster-wide from any node, not just the one holding the data.
- **Per-user data is scoped by claims, not by request field.** `backend/internal/server/system_user.go:59-160` — `GetPreferences`, `PutPreferences`, `GetSavedQueries`, `PutSavedQuery` all derive the user from `auth.ClaimsFromContext` and reject a nil-claims context. No client-supplied user ID is trusted.
- **`ChangePassword` prefers claims over the request field.** `backend/internal/server/auth.go:349-352`: `username` starts from `req.Msg.Username` but is overwritten by `claims.Username()` whenever claims exist. Since `ChangePassword` is not in the public map, claims are always present under the real interceptor, so the request field is dead. It still verifies the old password (`auth.go:370-376`) and invalidates all tokens on success (`auth.go:390-395`). Sound — though the request-field fallback should be deleted, as it is only live in the fail-open configuration of L-2.
- **`DeleteUser` self-deletion guard — sound.** `backend/internal/server/auth.go:642-645`, compared against `claims.UserID`, prevents an admin locking the system out of itself. Refresh tokens are cleaned up before the user row (`auth.go:648-650`).
- **Role changes invalidate tokens.** `UpdateUserRole` (`auth.go:522-527`), `ResetPassword` (`auth.go:570-575`), `RenameUser` (`auth.go:615-620`) each call both `InvalidateTokens` and `DeleteUserRefreshTokens`. So a demoted admin's cached `role: "admin"` claim is not honored after the change — the claim is read from the token (`interceptor.go:210`) without a re-check, but the token itself is revoked. This is the correct pairing and it is applied consistently.
- **Refresh tokens are opaque and stored hashed.** `backend/internal/auth/refresh.go:14-28`: 32 bytes from `crypto/rand`, base64url, stored as hex SHA-256. Lookup is by hash (`auth.go:278-279`), so a config-store read does not yield usable refresh tokens. Expiry, user-existence, and `TokenInvalidatedAt` are all checked before issuance (`auth.go:288-314`), and expired/orphaned/revoked rows are deleted on discovery. (SHA-256 without a salt is fine here — the input is 256 bits of entropy, so there is nothing to brute-force.)
- **Unix socket bypass — sound.** `backend/internal/server/server_tls.go:123-144`: the socket is created, then `chmod 0600`, and the listener is torn down if the chmod fails. OS file permissions are the access control, and `NoAuthInterceptor` on that mux is a deliberate, documented consequence. Stale-socket removal at `:125` is a local-user race in principle but the subsequent `chmod` failure path closes it.
- **CORS — sound.** `backend/internal/server/server.go:529-571`: the `Origin` is never reflected unless it exactly matches `scheme://r.Host`, with a loopback-only relaxation for the Vite dev proxy that requires *both* the request host and the origin host to be loopback (`isOriginAllowed:557-570`). No wildcard, no `Access-Control-Allow-Credentials`, no substring/suffix origin matching. Security headers are set on every response (`backend/internal/server/headers.go:6-14`).
- **Inbound message size is bounded.** `backend/internal/server/server.go:592` — `connect.WithReadMaxBytes(4 << 20)` on all Connect handlers.
- **`multiraft.DialerPeerPool` insecure credentials are test-only.** `backend/internal/multiraft/dialer_peer_pool.go:73` uses `insecure.NewCredentials()`, but the type is a test harness (its own error string at `dialer_peer_pool.go:57` reads "multiraft test pool: no dialer for %q") reached only through an injected dialer map. It is not on any production path. Worth moving into a `_test.go` file so it cannot be wired up by accident.

---

## Recommended order of work

1. **C-1** — attach the mTLS interceptors to the Raft lanes (`cluster.go:685`). One-line fix, removes unauthenticated cluster takeover.
2. **H-1** — convert the interceptor to default-deny plus an exhaustiveness test over the generated procedure constants. This one change closes ~20 holes and prevents the next twenty.
3. **H-4** and **M-2** — move `GetSettings` and `TestHTTPLookup` behind admin (partly subsumed by H-1, but `GetSettings` needs the public/admin split, which H-1 alone does not give).
4. **H-3** — route the upload endpoint through the shared auth helper.
5. **H-2** — guard `--no-auth` against non-loopback listeners.
6. **M-1**, **M-5**, **L-2** — session-lifecycle correctness.
