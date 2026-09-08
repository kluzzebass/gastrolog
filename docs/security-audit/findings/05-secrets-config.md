# Security audit — secrets, credentials, and configuration safety

Scope: bootstrap token delivery, JWT signing secret, TLS private key material, cloud-storage
credentials, the Raft-backed config store, dotenv/CLI handling, and operator-facing surfaces
that echo config back (GetSystem/GetSettings RPCs, `config export`, cluster dev scripts).

Method: static read-only review (grep + read). No cluster commands, no tests executed, no
secret values printed. All claims verified by reading the cited file/line unless marked
"suspected."

---

## Finding 1 — Cloud storage credentials returned in plaintext on every config mutation, unredacted, no `includeSecrets` gate

**Severity: Critical**

**Files:**
- `backend/internal/server/system.go:432-467` (`loadConfigCloudServices`) — populates `AccessKey`, `SecretKey`, `ConnectionString`, `CredentialsJson` from the stored `CloudService` verbatim into the `apiv1.CloudService` proto, lines 452-456.
- `backend/internal/server/system.go:189-217` (`buildFullSystem`) — calls `loadConfigCloudServices` unconditionally; this is the body of `GetSystem` (line 178-187) and is also called inline by **every** config-mutation handler to build its response.
- Confirmed fan-out example: `backend/internal/server/system_storage.go:68-72` — `PutCloudService` response embeds `buildFullSystem(ctx)`, i.e. `PutCloudServiceResponse.System` carries every cloud service's plaintext secret key back to the caller on every single cloud-service write. The same `buildFullSystem` call is used by the mutation-echo pattern described at `system.go:190` ("Used by GetConfig and by mutation handlers to return the updated config inline"), so a `PutVault`, `PutRoute`, `PutRetentionPolicy`, etc. response also carries the full cloud-service credential set, not just cloud-service edits.
- Auth gate: `GetSystem` requires `role=admin` (`backend/internal/auth/interceptor.go:117`), so this is not open to anonymous or low-privilege callers — but it is unconditional for every admin session, with no redaction, no `includeSecrets`-style opt-in, unlike the pattern used elsewhere in the same file.
- Contrast: TLS private keys are explicitly redacted with a one-line comment `KeyPem: "", // Never expose private keys via API` (`backend/internal/server/system_certs.go:93`), and the MaxMind license key is gated behind an explicit `includeSecrets` request flag (`system.go:262-265`). Cloud-service credentials get neither treatment.

**Attacker scenario:** Any admin-role session (including a session hijacked via XSS, a stolen admin JWT, or an over-privileged read-only tool that happens to hold an admin token) triggers this on the very first page load of the settings/inspector UI, and again on every subsequent write anywhere in config — vault edits, route edits, retention policy edits. Because GastroLog ingests its own logs (the self ingester), if any component along that path (reverse proxy access log with body capture, browser extension, HTTP debugging proxy, future accidental `slog` of a proto response) captures the response body even once, the S3/GCS access key, secret key, connection string, or service-account JSON becomes durably stored and full-text searchable in GastroLog itself — a "log a secret once, it's permanent" amplification the task brief calls out. No logging call currently does this (verified — see "checked and sound"), but the RPC layer hands the secret to any consumer capable of admin auth, with none of the deliberate restraint applied to TLS keys or MaxMind creds two structs over in the same file.

**Remediation:** Mirror the `MaxMindSettings` pattern: add a `Configured bool` (or masked `****` last-4) field to `apiv1.CloudService` for `AccessKey`/`SecretKey`/`ConnectionString`/`CredentialsJson`, strip the raw values from `loadConfigCloudServices`, and add an explicit `include_secrets` request flag (checked server-side, not just UI-side) for the one or two call sites that legitimately need the raw value (e.g. `config export` for restore). Since `PutCloudService`'s own response doesn't need to echo the secret back (the caller just sent it), the mutation-echo path should redact unconditionally.

---

## Finding 2 — `GetSettings(include_secrets=true)` returns the MaxMind license key to any authenticated user, not just admins

**Severity: High**

**Files:**
- `backend/internal/server/system.go:498-529` (`GetSettings`) — unauthenticated callers get only the password policy (line 507-522, by design, comment confirms intent). Any authenticated caller (`auth.ClaimsFromContext(ctx) != nil`, no role check) reaches `buildFullSettingsResponse(ctx, req.Msg.IncludeSecrets)` at line 524.
- `backend/internal/server/system.go:262-265` — when `includeSecrets` is true, `mm.AccountId`/`mm.LicenseKey` are populated with the raw MaxMind credential, unconditional on role.
- `backend/internal/auth/interceptor.go:80-87` — `SystemServiceGetSettingsProcedure` is in the `public` map (meaning "no forced auth, but attach claims if present"), and it is **not** in the `admin` map (compare to `SystemServiceGetSystemProcedure`, which is at line 117). So the only gate on `include_secrets=true` is "is this JWT valid," not "is this JWT an admin."
- User role model: `system/config.go:252` — `User.Role` is `"admin"` or `"user"`, confirming non-admin authenticated accounts exist in this product.

**Attacker scenario:** A low-privilege `"user"`-role account (e.g. an SRE given read/search-only access, per the project's stated mixed-audience design) calls `GetSettings` with `include_secrets: true` directly via the Connect API (bypassing whatever the UI chooses to send) and receives the operator's paid MaxMind GeoIP license key and account ID — credentials that should be admin-only, since `PutMaxMindSettings` and `RegenerateJwtSecret` are correctly admin-gated (`interceptor.go:129,131`) but reading them back is not.

**Remediation:** Add `gastrologv1connect.SystemServiceGetSettingsProcedure` handling so that `include_secrets=true` is only honored when `claims.Role == "admin"`; for non-admin authenticated callers, force `includeSecrets=false` server-side regardless of the request field (never trust the client-supplied flag as authorization).

---

## Finding 3 — Config-store secrets (JWT secret, TLS private keys, cloud credentials) are unencrypted at rest, and the file permissions the code's own comment recommends as mitigation are not actually applied

**Severity: High**

**Files:**
- Plaintext at rest is a documented, accepted tradeoff: `backend/internal/system/bootstrap.go:100-109` — "the JWT secret is stored as base64 in the config store (Raft snapshot or in-memory). It is NOT encrypted at rest... Mitigations: restrict filesystem permissions on the config vault (e.g. 0600 / owner-only)."
- The same plaintext-in-Raft-log pattern applies to `ClusterTLS.CAKeyPEM`/`ClusterKeyPEM` (`system/config.go:214,216`, carried through `system/command/command.go:811,813,824,826` as raw proto bytes) and to `CloudService.AccessKey/SecretKey/CredentialsJSON` (`system/storage.go:167-171`) — every Raft command is `proto.Marshal`-ed (`system/command/command.go:27-29`) with no encryption layer, confirmed by the research pass finding no `encrypt`/`aes`/`cipher`/`sealed`/`gcm` hits anywhere in `internal/system`, `internal/raftwal`, `internal/raftgroup`, `internal/multiraft`, `internal/vaultraft` (the one "sealed" hit is unrelated retention terminology).
- **The recommended mitigation is not implemented**: WAL segment files are opened with `0o644` — `backend/internal/raftwal/wal.go:809` (`os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)`) and again at `wal.go:865` for the spare segment. `0o644` is group- and world-readable-if-permitted, not the "0600 / owner-only" the code comment calls for.
- Raft snapshots (which embed a full `ServerSettings` JSON blob — including `JWTSecret`, `MaxMindConfig.LicenseKey` — inside a further proto envelope, per `system/command/command.go:364,861`) are written via the stock `hashicorp/raft` file snapshot store with **no permission override by GastroLog**: `internal/raftgroup/groupmanager.go:241` constructs `hraft.NewFileSnapshotStore(groupDir, 2, io.Discard)` with default library behavior — `os.MkdirAll(path, 0o755)` and `os.Create(...)` for `state.bin`/`meta.json` (`hashicorp/raft@v1.7.3/file_snapshot.go:102,137,173,207,506`). `os.Create` defaults to mode `0666` modified only by the process umask (typically `0644` on a default `022` umask) — combined with the `0755` snapshot directory, this makes `state.bin` **world-readable**, a strictly worse posture than the WAL segments.
- Parent directories are tighter: raft group dir `0o750` (`raftgroup/groupmanager.go:225`), WAL dir `0o750` (`raftwal/wal.go:360`) — but these only block "other," not "group," and don't apply to the snapshot subdirectory the library creates at `0o755`.

**Attacker scenario:** Any local account on the node's filesystem (a co-tenant process, a compromised sidecar/backup agent, a misconfigured shared-storage NFS/CIFS mount inheriting host-side ACLs loosely) can read `state.bin` in a group's snapshot directory and recover the JWT signing secret (forge admin auth tokens for the cluster), the cluster mTLS CA and node private keys (impersonate a cluster member or MITM inter-node Raft/RPC traffic), and every configured cloud-storage access key/secret/service-account JSON — without ever touching the GastroLog process or its API. This is a bigger blast radius than the WAL-segment 0644 issue alone, since a full FSM snapshot is a complete secret dump in one file, whereas WAL segments require reconstructing from the log.

**Remediation:** At minimum, change the WAL segment `os.OpenFile` mode from `0o644` to `0o600` at both `wal.go:809` and `wal.go:865` to match the documented mitigation. For snapshots, `hashicorp/raft`'s `FileSnapshotStore` doesn't expose a permission override — either (a) `os.Chmod` the snapshot directory/files to `0700`/`0600` immediately after each snapshot write (there's a completion hook point in the snapshot sink close path), or (b) set a restrictive process `umask(0077)` at startup so every file GastroLog or its dependencies create defaults to owner-only, which would also retroactively fix the WAL segment issue.

---

## Finding 4 — Secrets passed as literal CLI flags are visible in `ps` output

**Severity: Medium**

**Files:**
- `backend/cmd/gastrolog/main.go:187,192,194-195` — `--join-token`, `--bootstrap-token-file`, `--bootstrap-token-url`, `--bootstrap-token-secret` flags on `gastrolog server`. `--join-token` and `--bootstrap-token-secret` take the secret value directly on the command line.
- `backend/cmd/gastrolog/main.go:199-201` — `--initial-admin-file`, `--initial-admin-user`, `--initial-admin-password`; `--initial-admin-password` takes the admin password as a literal flag.
- `backend/cmd/gastrolog/cli/cluster.go:684-700` — the `join` CLI subcommand's `--join-token` flag is `MarkFlagRequired`, so an operator must supply the token as a literal argument (not file/env) for that specific subcommand.
- `scripts/cluster.sh:279-280` — the dev cluster script invokes `$GLOG register ... --password "$ADMIN_PASS"`, putting the admin password on the command line of a spawned `gastrolog` process during every `cluster.sh` init run.
- Comment in `backend/internal/app/bootstrap_token.go:33-34` acknowledges the literal flag is "the lowest-level escape hatch," implying file/URL delivery is the intended safer default — which is correctly what the production `deploy/compose.yml`, `deploy/stack.yml`, and `deploy/k8s.yml` manifests use (`GASTROLOG_INITIAL_ADMIN_FILE` env var pointing at a mounted secret, not a CLI flag or plaintext env value with the password inline).

**Attacker scenario:** Any local user able to run `ps aux` (or read `/proc/<pid>/cmdline` on Linux) while `gastrolog server --join-token=...` or `scripts/cluster.sh`'s `register --password ...` is executing captures the secret in full. This is a real but narrow window (process lifetime of the flag-parsing call, or the brief `register` subprocess in `cluster.sh`), and the file/URL/env alternatives already exist and are what production deploy manifests use — so this is a residual risk in the escape-hatch path and the local dev script, not the primary distribution mechanism.

**Remediation:** No code change strictly required given the safer alternatives already exist and are used in production manifests; consider documenting in `--help` text that the literal flags are dev/escape-hatch only, and switching `scripts/cluster.sh`'s `register` call to a mechanism that doesn't put the password on the command line (e.g. `--password-stdin`, if the `register` CLI command doesn't already support it — it does not appear to, based on scanning `cli/user.go`/`cli/cli.go`).

---

## Finding 5 — Default dev-cluster admin credential (`admin`/`admin123`) cannot reach a real deployment; production manifests use a "change-me" placeholder with no enforced rotation

**Severity: Low**

**Files:**
- `scripts/cluster.sh:15-16,40-41,443` — `ADMIN_USER="${GLOG_ADMIN_USER:-admin}"`, `ADMIN_PASS="${GLOG_ADMIN_PASS:-admin123}"`; line 443 echoes `"Admin: ${ADMIN_USER}/${ADMIN_PASS}"` to stdout after cluster init. This script is explicitly the local dev/soak-cluster launcher (per `CLAUDE.md`/`MEMORY.md` — "ONE cluster... /Volumes/GastroLog1/node{1..4}"), not a production deployment path.
- Verified this default does **not** appear in any deployment manifest: `grep` across `deploy/*.yml` and `deploy/helm/gastrolog/values.yaml` for `admin`/`password` finds only `deploy/admin-creds` (checked-in placeholder file, content `{"username": "admin", "password": "change-me"}`), referenced by `deploy/compose.yml:31-42`, `deploy/stack.yml:22-52`, `deploy/k8s.yml:102-159` via `GASTROLOG_INITIAL_ADMIN_FILE` / Docker secret / K8s Secret mount — all with explicit operator instructions ("Replace the secret file... with your own before running", `compose.yml:31`; `docker secret create` example, `stack.yml:22-23`).
- `backend/internal/app/initial_admin.go:99,187-191` (`validateInitialAdmin`) runs the same password-policy validation as regular user creation (length/complexity), but there is no check that rejects the literal string `"change-me"` or `"admin123"` specifically — an operator who ignores the manifest comments and deploys with the placeholder file unmodified gets a working, policy-compliant admin account with a publicly-known password.
- MinIO credential in the same manifests: `deploy/compose.yml:128` and `deploy/stack.yml:114` set `MINIO_ROOT_PASSWORD: gastrolog` — a hardcoded weak credential for the bundled example object-storage backend (not GastroLog's own auth, but shipped in the same reference deployment).

**Attacker scenario:** Low — reaching this requires an operator to (a) ignore explicit "replace before running" comments in the manifest, and (b) expose the resulting instance to an untrusted network. Not exploitable from the codebase alone; documentation-and-defaults hygiene issue rather than a code vulnerability.

**Remediation:** Consider having `validateInitialAdmin` (or a first-login check) refuse/warn on a small deny-list of known example passwords (`change-me`, `admin123`, `gastrolog`) so a forgotten placeholder fails loudly instead of silently working. Optional; low priority given the existing guardrails (explicit comments, secret-mount pattern, policy validation already in place).

---

## Checked and sound

- **No logging of secret material found.** Grepped every `SecretKey`/`AccessKey`/`CredentialsJSON`/`JWTSecret`/`LicenseKey`/`CAKeyPEM`/`ClusterKeyPEM`/`PasswordHash` reference across `backend/internal` for adjacency to `slog.*`, `log.*`, `fmt.Print*`, `fmt.Errorf`, `fmt.Sprintf` — zero hits outside test fixtures. Blobstore error paths (`backend/internal/blobstore/factory.go:55-118`) wrap generic operation names ("ensure bucket", "upload probe") without interpolating credential values.
- **TLS private keys are explicitly never returned via the certificate API** — `backend/internal/server/system_certs.go:93`, `KeyPem: "", // Never expose private keys via API`, with an explanatory comment at the call site.
- **`config export` explicitly excludes the JWT signing secret and certificate private keys** — `backend/cmd/gastrolog/cli/export.go:153-166` (`exportExclusions`), and computes/labels which secrets a given export *does* carry via `exportSecrets` (`export.go:182-199`) rather than asserting a blanket claim — the export is not a silent leak, it's a documented, opt-in secret bundle for restore purposes (though see Finding 1/2 for what feeds it).
- **Bootstrap join-token delivery (`backend/internal/app/bootstrap_token.go`) is well-built**: atomic write-then-rename (`writeBootstrapTokenAtomic`, line 74-107), explicit `0600` file mode (`bootstrapTokenFileMode`, line 39, applied via `tmp.Chmod` before rename at line 86), bounded reads (`maxBootstrapTokenBytes`, line 64) against both file and HTTP sources preventing memory-exhaustion from a hostile/misconfigured source, and the HTTP delivery path (`fetchBootstrapTokenWithRetry`) requires a non-empty shared secret (line 175-177) and distinguishes auth failure (fatal, stop retrying) from transient errors (line 225-236).
- **The `/cluster/bootstrap-token` HTTP endpoint uses constant-time secret comparison** — `backend/internal/server/server.go:361`, `subtle.ConstantTimeCompare`, and logs only `remote` address on rejection, never the submitted or expected secret (`server.go:362`).
- **`.env` is gitignored** (`.gitignore:31`) and not committed; local file mode is `0644` (readable by other local accounts on a shared host, but not a code-level finding — standard OS/environment hygiene, flagged here only for completeness, not as a separate numbered finding).
- **Admin-only mutation endpoints are correctly role-gated**: `PutMaxMindSettings`, `RegenerateJwtSecret`, `PutCloudService`-adjacent mutations, certificate CRUD, and user management are all in the `admin` map (`backend/internal/auth/interceptor.go:88-149`) — the gap identified in Finding 2 is specifically the *read* path for settings, not the write path.
