# HTTP / API Surface Audit — GastroLog

Scope: `backend/internal/server/**` (handlers, middleware, upload, static/frontend serving), the Connect RPC surface, and anything reachable over the TCP listen port or the Unix socket. Method: read-only (grep + read). No code was modified, no cluster commands were run.

All line numbers verified by direct reading on 2026-08-26.

---

## Finding 1 — Managed-file upload endpoint has no role check; can poison lookup data sources by name collision

**Severity: High**
**Exploitability: authenticated, non-admin ("user" role) account, remote (network position: anyone who can reach the TCP listener with valid creds)**

**Files:**
- `backend/internal/server/upload.go:35-52` (`handleManagedFileUpload` auth check)
- `backend/internal/server/upload.go:203-227` (`ResolveManagedFilePath` — name-based, "latest wins")
- `backend/internal/server/upload.go:23-29,254-257` (size cap, route registration)
- `backend/internal/auth/interceptor.go:88-149` (admin allowlist — `ListManagedFiles`/`DeleteManagedFile` require admin, but there is no equivalent for upload)

**Detail:**

`/api/v1/managed-files/upload` is a plain `net/http` route (`mux.HandleFunc`, not a Connect RPC), so it sits outside the `AuthInterceptor`'s per-procedure admin allowlist entirely. Its own auth check is:

```go
// upload.go:42-52
if !s.noAuth && s.tokens != nil {
    token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
    if token == "" { ... 401 }
    if _, err := s.tokens.Verify(token); err != nil { ... 401 }
}
```

This verifies the JWT signature/expiry only — it never checks `claims.Role`. Any account with role `"user"` (created via `CreateUser`, itself admin-gated, but also the kind of low-trust account an operator hands out for read-only search access) can POST an arbitrary file up to 256 MB (`maxUploadSize`, upload.go:24) and have it registered as a `ManagedFileConfig` via `RegisterFile`.

Compare this to the RPCs that manage the *same* entity: `SystemServiceListManagedFilesProcedure` and `SystemServiceDeleteManagedFileProcedure` are both in the `admin` map (interceptor.go:145-146). The write path (upload) is less protected than the read/delete paths — a broken-access-control asymmetry.

**Why it matters beyond disk exhaustion:** `ResolveManagedFilePath` (upload.go:203-227) resolves a managed file **by display name**, walking entries newest-first (UUIDv7 creation order) and returning the first name match:

```go
for i := range slices.Backward(files) {
    if files[i].Name == filename { ... return path }
}
```

The auto-download GeoIP path (`server_lookup.go:96-98`) and any JSON/YAML/CSV lookup configured without a pinned `file_id` resolve by exactly this mechanism. A non-admin user can upload a file named `GeoLite2-City.mmdb` (or whatever name an admin's lookup table expects) and, being the most recently created match, it wins over the legitimate file the next time lookups reload — silently substituting attacker-controlled content into a query-time enrichment path (parsed by the MMDB reader, JSON/YAML unmarshalers, or CSV/jq pipeline) without the admin taking any action or being notified.

**Remediation:** Require `claims.Role == "admin"` in `handleManagedFileUpload` (the claims are already attached to the request context by the interceptor chain before this handler runs, or can be verified inline the same way the token itself is). Secondary hardening: make managed-file lookup resolution require an explicit `file_id` instead of falling back to mutable name matching, so a same-named upload can never silently supersede a pinned reference.

---

## Finding 2 — HTTP lookup tables: no SSRF destination validation; unescaped `:`/`@` in URL-template substitution enables host hijack

**Severity: Medium**
**Exploitability: primary vector requires admin (`SystemServicePutLookupSettingsProcedure`, admin-gated) to configure; secondary vector is attacker-controlled log field values reaching the request host when an admin's template embeds a placeholder in the host segment**

**Files:**
- `backend/internal/server/server_lookup.go:132-164` (`registerHTTPLookups` — admin-controlled `URLTemplate`/`Headers`, no validation)
- `backend/internal/lookup/http.go:216-274` (`LookupValues`/`doFetch` — the actual outbound fetch)

**Detail:**

`HTTPConfig.URLTemplate` and `Headers` are taken verbatim from admin-supplied system config (`system.LookupConfig.HTTPLookups`) with no allowlist, denylist, or scheme restriction. `doFetch` (http.go:277-333) does a plain `http.Client{Timeout: ...}.Do(req)` against whatever URL results from template substitution — nothing prevents the target from being `http://169.254.169.254/latest/meta-data/...` or any RFC1918/loopback address reachable from the node.

This is consistent with the app's design (an admin-configured integration is expected to reach wherever the admin points it, similar to a webhook), so the primary path is intentional and admin-only. It's flagged because there's no destination guard at all — a compromised or malicious admin account (or a config-import from an untrusted source, since config import/export exists elsewhere in the system) gets an unrestricted internal-network probe/fetch primitive with the response partially reflected back into log records (`flattenScalars`/`mergeNode`, http.go:337-420).

**Secondary, more interesting issue — parameter substitution is unescaped for host-breaking characters:**

```go
// http.go:242-245
reqURL := h.urlTemplate
for k, v := range values {
    reqURL = strings.ReplaceAll(reqURL, "{"+k+"}", url.PathEscape(v))
}
```

`url.PathEscape` escapes characters that are unsafe in a URL *path segment* (including `/`), but it deliberately leaves `:` and `@` unescaped (both are valid `pchar` per RFC 3986). If an admin's template places a substitution placeholder anywhere in or adjacent to the host component — e.g. `http://{tenant}.geo.internal/lookup` where `{tenant}` is populated from an ingested log field the admin trusted to be a simple identifier — a value such as `trusted.internal@evil.com` is not escaped and, after string substitution, changes the *parsed* destination host entirely (`trusted.internal` becomes URL userinfo, `evil.com` becomes the actual host). Since parameter values are drawn from record fields (`LookupValues(ctx, values map[string]string)`, called at query/ingest enrichment time), and those fields can originate from external, untrusted log sources, this turns a config-time admin decision into a runtime, attacker-influenced SSRF/exfiltration primitive whenever a template happens to interpolate into the host position — which the admin has no clear warning against, since `PathEscape` looks like proper escaping.

**Remediation:** Validate/parse the fully-substituted URL with `net/url` and re-check that `Host` still matches the template's original host before dispatching (i.e., substitute only into path/query positions, never allow substitution to change scheme/host/userinfo). Optionally offer a destination policy (block loopback/link-local/private ranges) for defense in depth, consistent with the project's existing "no invented forks" preference — this could reuse whatever URL-fetch guard exists elsewhere, or be a one-time helper shared by any future outbound-fetch feature.

---

## Finding 3 — No `ReadTimeout`/`WriteTimeout`/`IdleTimeout` on any of the three `http.Server`s

**Severity: Medium**
**Exploitability: unauthenticated, remote, network position = anyone who can open a TCP connection to the listen port (HTTP or HTTPS)**

**Files:**
- `backend/internal/server/server.go:758-761` (primary HTTP server)
- `backend/internal/server/server_tls.go:78-81` (HTTPS server)
- `backend/internal/server/server_tls.go:150-153` (Unix socket server — lower risk given 0600 perms, listed for completeness)

**Detail:** All three `http.Server` instances set only:

```go
ReadHeaderTimeout: readHeaderTimeout, // 10s
```

`ReadHeaderTimeout` bounds only the time to read request *headers* — it correctly closes the classic slowloris (drip-feed headers) vector, which the codebase is clearly already aware of (the constant's doc comment). There is no `ReadTimeout` (caps time to read the full request, headers + body), no `WriteTimeout` (caps time to write the response), and no `IdleTimeout` (caps how long a keep-alive connection may sit idle between requests). Body size is capped for Connect RPCs (`WithReadMaxBytes(4<<20)`, server.go:592) and for the upload endpoint (`http.MaxBytesReader`, upload.go:57), but **size caps don't bound time** — an attacker can open a connection, send a slow trickle of body bytes (or read the response one byte at a time) and hold a server goroutine + file descriptor for an arbitrary duration, well past what a legitimate client needs. With no `IdleTimeout`, a completed request's keep-alive connection can also be held open indefinitely by simply never sending the next request.

This is a resource-exhaustion (goroutine/FD) vector distinct from the header-timeout case that's already mitigated; low sophistication required (many slow HTTP client tools implement exactly this).

**Remediation:** Add `ReadTimeout`, `WriteTimeout` (long enough not to break the long-poll/stream RPCs — `Follow`/`WatchConfig`/`WatchSystemStatus` — or scope `WriteTimeout` per-connection via `http.ResponseController` on those handlers specifically), and `IdleTimeout` to all three `http.Server` values.

---

## Finding 4 — `Login` has a measurable timing side-channel that leaks username existence

**Severity: Low**
**Exploitability: unauthenticated, remote; rate-limited to 5 req/min/IP but not eliminated (statistical attack over many IPs or long duration)**

**File:** `backend/internal/server/auth.go:224-245`

**Detail:**

```go
user, err := s.cfgStore.GetUserByUsername(ctx, username)
...
if user == nil {
    return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid credentials"))
}
ok, err := auth.VerifyPassword(password, user.PasswordHash)
```

Both the "no such user" and "wrong password" cases return the identical error message ("invalid credentials") — good, no message-based enumeration. But when `user == nil`, the handler returns immediately; when the user exists, it additionally calls `auth.VerifyPassword`, which is presumably a deliberately-slow hash comparison (bcrypt/argon2-class). This produces a measurable latency delta between "username doesn't exist" and "username exists, wrong password," letting an attacker enumerate valid usernames by timing. `Login` is rate-limited (5/min/IP, ratelimit.go:17-19), which raises the cost but doesn't close the channel (distributed source IPs, or simply patience, both work against a per-IP limiter).

**Remediation:** Perform a dummy/constant-time password verification (e.g. against a fixed dummy hash) on the `user == nil` path so both branches take comparable time before returning.

---

## Finding 5 — Internal error text is returned verbatim to API clients, including on pre-authentication endpoints

**Severity: Low**
**Exploitability: varies by endpoint; worst case is unauthenticated (public endpoints `Login`, `Register`, `GetAuthStatus`, `RefreshToken`, `SystemServiceGetSettings` per `interceptor.go:80-87`)**

**Files:** `backend/internal/server/errors.go:18-23` (`errInternal` helper) and its many call sites, e.g. `auth.go:233` (`Login`: `fmt.Errorf("get user: %w", err)` wrapped as `CodeInternal` — reachable pre-auth).

**Detail:** The `errInternal(err)` pattern is used throughout the server package to surface the underlying Go error's `.Error()` text directly in the Connect error `message` field sent to the client. This isn't a stack trace or a crash dump, but wrapped errors frequently carry implementation detail (store/backend error strings, and in other subsystems, filesystem paths) that a well-behaved API shouldn't hand to an unauthenticated caller. It's a low-grade information-disclosure smell rather than an exploitable primitive by itself.

**Remediation:** For handlers reachable before authentication, log the detailed error server-side (the RPC error interceptor already does this — `rpc_error_log_interceptor.go` — so the information isn't lost) and return a generic message to the client; reserve detailed messages for admin-authenticated surfaces where the caller is already trusted.

---

## Finding 6 (confirmed, pre-existing) — Open redirect via unvalidated `Host` header in `redirectMiddleware`

**Severity: Medium**
**Exploitability: unauthenticated, remote; requires the operator to have TLS + `HTTPToHTTPSRedirect` enabled (`ss.TLS.HTTPToHTTPSRedirect`)**

**File:** `backend/internal/server/server.go:784-810`

**Detail (confirming the already-known finding):**

```go
host, _, _ := net.SplitHostPort(r.Host)
if host == "" { host = r.Host }
if isLoopback(host) { next.ServeHTTP(w, r); return }
// This reflects the client-supplied Host header into the redirect
// target unvalidated: an open redirect. ...
httpsURL := "https://" + host + ":" + port + r.URL.RequestURI()
http.Redirect(w, r, httpsURL, http.StatusTemporaryRedirect)
```

Confirmed as described: `r.Host` is attacker-controlled (any client can send an arbitrary `Host:` header over HTTP/1.1, or `:authority` over HTTP/2), and it is spliced directly into the `Location` of a same-origin-looking 307 redirect with no allowlist of configured hostnames or TLS SANs. Scope: this is a classic open redirect (phishing / trust-abuse vector — a link to the legitimate node with a forged `Host` header redirects the victim's browser to an attacker's HTTPS endpoint that can then visually impersonate the login page), not a token/header-leaking vulnerability, since it's a redirect rather than a proxied fetch — no `Authorization` header or cookie is forwarded to the attacker's host by this mechanism itself. Only reachable when TLS + HTTP→HTTPS redirect are both enabled; the loopback bypass (`isLoopback`) means local dev setups are unaffected.

**Remediation (as previously noted):** Build the target host from a trusted source (configured public hostname(s) or the cert's SANs) rather than from `r.Host`, falling back to a fixed/default host when the request's `Host` doesn't match anything trusted.

---

## Finding 7 — No Content-Security-Policy or Strict-Transport-Security headers

**Severity: Low**
**File:** `backend/internal/server/headers.go`

**Detail:** `securityHeadersMiddleware` sets `X-Content-Type-Options`, `X-Frame-Options: DENY`, `Referrer-Policy`, and a restrictive `Permissions-Policy`, but no `Content-Security-Policy` and no `Strict-Transport-Security`. For the embedded SPA this raises the blast radius of any future XSS (no CSP to restrict script/connect sources) and, combined with Finding 6, the absence of HSTS means a client that has never visited over HTTPS has no browser-enforced protection against being kept on plain HTTP by a stripping proxy or the open-redirect issue above.

**Remediation:** Add a CSP scoped to the SPA's actual needs (self-hosted scripts/styles, `connect-src 'self'` for the Connect RPC calls) and `Strict-Transport-Security` (guarded so it's only sent on the HTTPS listener, to avoid breaking the documented loopback/dev HTTP flow).

---

## Checked and found sound

- **CORS**: strict same-origin-or-loopback allowlist, never reflects an arbitrary `Origin`. `corsMiddleware`/`isOriginAllowed`, server.go:525-571.
- **Body size limits**: Connect RPCs capped at 4 MB (`connect.WithReadMaxBytes(4<<20)`, server.go:591-593); managed-file upload capped at 256 MB total via `http.MaxBytesReader`, with a separate 32 MB in-memory multipart buffer bound (upload.go:24-29,57-65).
- **Upload filename handling**: the client-supplied filename is sanitized with `filepath.Base` and used only as *display metadata*; the on-disk path is always `<home>/managed-files/<generated-id>/data` — no path traversal is possible through the filename. upload.go:100-101, 161-173.
- **Upload temp files**: written via `os.CreateTemp` (random suffix, default 0600 mode) inside a 0750 directory, and removed via a deferred `os.Remove` that becomes a no-op after a successful rename. No predictable names, no lingering world-readable temp files on success or failure. upload.go:77-98.
- **No public managed-file download route**: `ManagedFileReader` (upload.go:288-323) is wired only into the cluster's node-to-node transfer path (`cluster.Server.SetManagedFileReader`, mTLS-authenticated peer channel) — there is no HTTP route that lets a client download an arbitrary managed file by ID/name.
- **Unix socket**: created with `0o600` permissions (owner-only), consistent with the documented "OS file permissions are the access control" model; the mux built for it uses `NoAuthInterceptor` only for that listener, not globally. server_tls.go:119-164.
- **Bootstrap-token endpoint**: shared-secret check uses `subtle.ConstantTimeCompare` (no timing leak on the secret itself), restricted to `GET`, and sets `Cache-Control: no-store`. server.go:349-380.
- **TLS hardening**: `MinVersion: tls.VersionTLS12` and an explicit `CurvePreferences` list are applied to the server-side TLS config. server_tls.go:70-74.
- **Auth rate limiting**: `Login`/`Register` are limited to 5 req/min/IP (burst 5) with a proper `429` + `Retry-After: 60` + Connect-shaped JSON error body. ratelimit.go.
- **RBAC**: centrally enforced, allowlist-based (`public`/`admin` maps keyed by exact Connect procedure name) in `AuthInterceptor.authenticate`, applied uniformly to every registered Connect service — user management, lifecycle/cluster mutation, vault/system config mutation, and certificate management are all correctly admin-gated at the RPC layer. auth/interceptor.go:71-215. (The one gap found is Finding 1, which is outside this RPC-based enforcement because the upload route bypasses the Connect layer entirely.)
- **Streaming/"websocket" endpoints** (`Follow`, `WatchConfig`, `WatchSystemStatus`): these are Connect HTTP/2 server-streams, not raw WebSockets — they inherit the same CORS/auth interceptor stack as unary RPCs (no separate, weaker code path), and `Follow` is time-bounded by `maxFollowDuration` when configured (query_follow.go:24-28).
- **Password/username validation**: username regex-bounded (`^[a-zA-Z0-9_-]{3,64}$`), configurable password policy enforced server-side on every password-setting path (Register/CreateUser/ChangePassword/ResetPassword), not just client-side.
