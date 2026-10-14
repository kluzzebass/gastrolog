# Issue map

Proposed remediation issues, all children of **gastrolog-5yuag7**. Grouped so each issue is one coherent change with one review, rather than one issue per finding — several findings share a fix.

Status column is filled in as issues are created.

## P0

| # | Proposed issue | Findings covered | Status |
|---|----------------|------------------|--------|
| 1 | Raft lane gRPC stacks require no client certificate — enable the mTLS interceptor and require verified client certs on the cluster listener | 01/C-1, 02/C-1 | gastrolog-3wz35r |
| 2 | Chart tooltips render log-derived values as unescaped HTML — stored XSS that steals operator tokens | 08/1 (with 08/2, 08/3 as the amplifiers) | gastrolog-5y2xqz |
| 3 | Ingester inputs are unbounded and no listener goroutine recovers — one crafted frame kills the node | 03/1, 03/2, 03/3, 03/4, 03/5, 03/7; 07/3 (query goroutines) | gastrolog-4965zz |
| 4 | Cloud storage credentials are returned in plaintext on every config read and write | 05/1 (with 05/2, 01/H-4) | gastrolog-2ylw7n |

## P1

| # | Proposed issue | Findings covered | Status |
|---|----------------|------------------|--------|
| 5 | Authorization is a single admin allowlist with ~20 gaps — invert to deny-by-default with declared per-handler requirements | 01/H-1, 01/H-3, 01/M-3, 04/1 | gastrolog-4d7frx |
| 6 | No per-vault read authorization — any authenticated user reads every vault | 07/1 | gastrolog-4mph42 |
| 7 | Cluster identity: per-node certificates, rotating join token, CA key handling, enrollment leaf verification | 02/H-1, 02/H-2, 02/H-3, 02/H-4, 02/M-1, 02/M-3 | gastrolog-566rxk |
| 8 | Authenticated users can exhaust node memory through aggregating pipelines | 07/2 | gastrolog-6c0cbt |
| 9 | SSRF: `TestHTTPLookup` and HTTP lookup tables fetch operator/user-supplied URLs without destination validation | 01/M-2, 04/2, 07/4 | gastrolog-593xl2 |
| 10 | No timeouts on any `http.Server` — slowloris against the API listeners | 04/3 | gastrolog-5ngvsn |
| 11 | Dependency vulnerabilities: 10 reachable Go CVEs (incl. gRPC authorization bypass) plus frontend advisories | govulncheck.txt, 08/5 | gastrolog-2cwyrr |
| 12 | Session lifecycle: logout does not revoke the access token; refresh rotation is not atomic; revocation is second-granular | 01/M-1, 01/M-4, 01/M-5 | gastrolog-696r6m |

## P2

| # | Proposed issue | Findings covered | Status |
|---|----------------|------------------|--------|
| 13 | Peer GLCB pulls skip the SHA-256 the manifest already carries; cloud-download verification silently skips | 06/1, 06/3 | gastrolog-txmm4q |
| 14 | Secrets at rest: config-store encryption, file modes, secrets off the command line | 05/3, 05/4, 02/L-2, 01/L-4 | gastrolog-30futr |
| 15 | Response hardening: CSP, HSTS, error-text leakage, open redirect, login timing side-channel | 04/4, 04/5, 04/6, 04/7, 08/2 | gastrolog-5mi6pl |
| 16 | `--no-auth` has no guard against non-dev use; auth sites fail open when the token store is nil | 01/H-2, 01/L-2 | gastrolog-50bm41 |
| 17 | Ingester TLS coverage: several ingesters offer none; Docker allows disabling verification; Kafka/MQTT support neither custom CA nor client certs | 02/M-4, 02/M-5, 02/L-4, 03/8 | gastrolog-54gnay |
| 18 | CI workflows have no explicit `permissions:` block | 08/6 | gastrolog-68noau |
| 19 | Data-file modes rely on directory permissions rather than their own; abandoned raft backups are never cleaned | 06/4, 02/L-3 | gastrolog-3g1q3x |

## Deliberately not filed

- **Tamper-evidence beyond corruption detection** (06/2). Whether GastroLog should defend against an adversary with disk or blob-store write access is a product decision, not a defect. Filing it as a bug would presume an answer. Raise it as a design question if the threat model calls for it.
- **Ingesters require no authentication** (03/8, partly). This is the documented design for log collection endpoints; network placement is the control. Recorded so the assumption is explicit rather than accidental.
- **Regex DoS** (07, informational). Checked and bounded; no action.
