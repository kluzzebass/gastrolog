# Security audit — 2026-08

**Epic:** **gastrolog-5yuag7** — *Security audit 2026-08: remediation*
**Branch:** `chore/gastrolog-5yuag7-security-audit`
**Status:** Findings documented. Remediation issues filed as children of the epic (see [issue-map.md](./issue-map.md)).

---

## What this is

A **read-only, whole-system security review** of GastroLog: the Go backend under [`backend/`](../../backend/), the React frontend under [`frontend/`](../../frontend/), and the build/CI configuration. Eight lenses ran independently, each producing evidence-cited findings; a dependency vulnerability scan ([govulncheck.txt](./govulncheck.txt)) ran alongside them.

**This pass documents problems. It changes no production code.** Fixes are tracked as children of the epic.

## Method

Each lens read the code and cited `file:line` for every claim, rated severity **with exploitability reasoning** (what position and credentials an attacker needs), and separated *verified by reading the code* from *suspected, needs a test*. Each lens also recorded a "checked and sound" list, so the negative space is part of the record — a later reader can tell what was examined and found fine, not just what failed.

Findings rated **Critical** were independently re-verified against the source before publication. One Critical (the Raft transport lane) was found independently by two lenses that did not share context.

## Scope and lenses

| Lens | Area | Findings |
|------|------|----------|
| [01](./findings/01-authn-authz.md) | Authentication, authorization, session lifecycle | 1C / 4H / 5M / 5L |
| [02](./findings/02-tls-transport.md) | TLS, cluster mTLS, enrollment, inter-node trust | 1C / 4H / 5M / 4L |
| [03](./findings/03-ingester-input.md) | Untrusted input across all network ingesters | 2C / 2H / 2M / 3L |
| [04](./findings/04-http-api.md) | HTTP/API surface, middleware, uploads | 1H / 3M / 3L |
| [05](./findings/05-secrets-config.md) | Secrets, credentials, configuration safety | 1C / 2H / 1M / 1L |
| [06](./findings/06-data-at-rest.md) | On-disk formats, integrity, cloud/archive path | 1H / 1M / 3L / 2I |
| [07](./findings/07-query-dos.md) | Query engine, expression evaluation, resource DoS | 2C / 1H / 1M / 1I |
| [08](./findings/08-frontend-supplychain.md) | Frontend XSS, token handling, supply chain, CI | 1C / 1H / 2M / 2L |

**68 findings total: 8 Critical (7 distinct), 16 High, 20 Medium, 21 Low, 3 informational.**

See [synthesis.md](./synthesis.md) for the cross-cutting themes and the prioritized remediation order.

## The one-sentence summary

GastroLog authenticates well and authorizes almost not at all: a single admin allowlist in one interceptor is the entire authorization model, there is no data-level (per-vault) authorization, and the highest-privilege transport in the system — the per-group Raft lanes that can write the replicated FSM — requires no client certificate at all.

## Threat models used

Findings state which threat model they assume, because severity depends on it:

- **Network-adjacent, unauthenticated** — can reach a listening port, has no credentials. The most important class: cluster ports and ingester ports accept traffic from anyone who can route to them.
- **Authenticated low-privilege user** — holds a valid `role: "user"` token.
- **Log writer** — can get a record ingested (often the same as network-adjacent, since no ingester requires authentication).
- **Compromised peer node** — holds a valid cluster certificate.
- **Local disk or blob-store write access** — relevant only to integrity findings.

A finding that requires already-root-on-the-host is rated Low and says so.
