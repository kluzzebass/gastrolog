# Lens 07: Query Engine, Expression Evaluation, and Denial of Service

Scope: `backend/internal/query/**`, `backend/internal/querylang/**`, lookup tables
(`backend/internal/lookup/**`), routing/digester expression evaluation, CLI query
path, and regex/pattern compilation anywhere in the backend. Read-only review
(grep + read); one finding (regex compile-time behavior) was verified with a
standalone Go program run outside the repo, not by touching or running any
gastrolog code or cluster.

All line numbers verified by reading the cited file at the time of writing.

---

## Finding 1 — No per-vault read authorization; any authenticated user can read any vault (Critical)

**Files:**
`backend/internal/auth/interceptor.go:75-151`, `backend/internal/auth/jwt.go:15-19`,
`backend/internal/query/vault_filter.go:23-51`, `backend/internal/server/query.go:71-122`

**What I verified:**
- `Claims` (`jwt.go:15-19`) carries only `Role` and `UserID` — there is no vault
  scope, tenant ID, or ACL field anywhere in the JWT.
- `NewAuthInterceptor`'s `admin` map (`interceptor.go:88-149`) lists the
  procedures that require `role == "admin"`. `QueryServiceSearchProcedure`,
  `QueryServiceExplainProcedure`, `QueryServiceFollowProcedure`, and
  `QueryServiceGetContextProcedure` are **not** in that map (only
  `QueryServiceExportToVaultProcedure` is admin-gated). Any authenticated user
  — any role — can call `Search`, `Follow`, `Explain`.
- `authenticate()` (`interceptor.go:182-215`) only checks token validity and,
  for admin-listed procedures, `claims.Role == "admin"`. It never looks at
  which vault the request targets.
- `ExtractVaultFilter` (`vault_filter.go:23-51`) parses a client-supplied
  `vault_id=<uuid>` predicate out of the query expression purely to decide
  *which vaults to scan* — it performs no comparison against the caller's
  identity or permissions. Omitting `vault_id=` entirely searches **every**
  vault the node knows about (`server/query.go` `selectedOrAllVaults`,
  `internal/server/query.go:827-841`, falls back to `cfgStore.ListVaults`).

**Attacker scenario:** Any authenticated, non-admin user (e.g. a viewer/read
role, if one exists, or any freshly-registered account before roles are
assigned) can run `vault_id=<any-uuid>` or a bare `*` search and read records
from every vault on the node/cluster, including vaults belonging to other
tenants or teams that the UI never shows them. This is not a bug in one
endpoint — it is a structural absence of a vault ACL layer between
authentication and the query engine.

**Exploitability:** Authenticated, any role, single RPC call, works cluster-wide
(every node forwards/collects across vaults the same way — see
`collectRemote`/`buildSearchPartitionTargets` in `query.go`).

**Remediation:** Introduce a per-user/per-role vault scope (allow-list or
policy) in `Claims`, and enforce it in `QueryServer.Search`/`Explain`/`Follow`
by intersecting the requested/derived vault set with the caller's allowed
vaults before calling `eng.Search`/`collectRemote`, rather than trusting
whatever `vault_id=` the client supplies.

---

## Finding 2 — Aggregating/materializing pipelines ignore result limits: unbounded memory via a single legitimate query (Critical)

**Files:**
`backend/internal/query/pipeline.go:166-178`, `:96-113`
`backend/internal/query/aggregate.go:18-19` (`MaxGroupCardinality`), `:592-609`
(`dcountAcc`), `:611-632` (`medianAcc`), `:674-698` (`valuesAcc`)
`backend/internal/server/query_pipeline.go:32-38`, `:85-90`
`backend/internal/server/query.go:56-58`, `:136-138`
`backend/internal/system/bootstrap.go:119-125`

**What I verified:**
- `RunPipeline` explicitly discards the caller's/server's `Limit` for any
  pipeline that isn't a pure head-only filter chain:
  ```go
  // pipeline.go:168-170
  origLimit := q.Limit
  q.Limit = 0
  ```
  The comment at `pipeline.go:166-167` states the intent directly: *"Pipeline
  operators control their own result limits... clear it so Search returns all
  matching records."* For `stats`/`timechart`/`sort`/`tail`/`slice` there is no
  such operator-level cap unless the user happens to add `head N` themselves.
- `query_pipeline.go:32-38` and `:85-90` clamp `q.Limit` to
  `s.maxResultCount` *before* calling `RunPipeline` — but `RunPipeline`
  immediately zeroes it again (`pipeline.go:170`), so this clamp is a no-op for
  every aggregating or fully-materializing pipeline (`stats`, `timechart`,
  `sort`, `tail`, `slice`, `raw`). It only has effect for the non-materializing
  record-list return path further down.
- `s.maxResultCount` (`query.go:58`) defaults to `0` = unlimited, and is only
  set from `ServerSettings.Query.MaxResultCount`
  (`server.go:714-736`/`system/config.go:105-107`). Both bootstrap paths
  (`bootstrap.go:119-125`, `:143-147`) set `Query.Timeout = "30s"` but **never
  set `MaxResultCount`**, so a fresh install ships with unlimited result count
  by default; only the 30s wall-clock timeout bounds a search, and that timeout
  is itself an admin-editable string that can be set to `""` (`loadQueryConfig`
  then leaves `queryTimeout == 0`, i.e. no timeout at all).
- `MaxGroupCardinality = 10_000` (`aggregate.go:19`) caps the number of
  *distinct group keys* in a `stats ... by <field>` — it does **not** bound the
  state held **inside** a single group's accumulators. `dcountAcc.seen`
  (`:594`), `medianAcc.vals` (`:613`), and `valuesAcc.seen`/`.order`
  (`:676-677`) each grow one entry per matching record with a non-missing
  value, with **no cap at all**, even with zero or one group.
- `sort`/`tail`/`slice` are documented as "coordinator-only... buffers all
  records in memory on the coordinating node"
  (`server/query_pipeline.go:206`, `pipeOpNote` at `:251-257`), and indeed
  `RunPipeline` calls `applyRecordOpsLimit` with `implicitLimit = 0` whenever
  `origLimit == 0` (`pipeline.go:191-196`), i.e. whenever no explicit
  `limit=`/`s.maxResultCount` was set — meaning the entire matched record set
  for the query's time range is pulled into memory before sorting.

**Attacker scenario:** Any authenticated user (see Finding 1 — no vault
restriction either) issues, e.g.:
```
* last=all | stats median(response_time_ms), dcount(request_id), values(user_agent)
```
or
```
* last=all | sort by ingest_ts
```
With a default/fresh configuration (`MaxResultCount` unset), this is not
capped by record count at all. `median`/`dcount`/`values` each build an
in-memory collection sized to the number of matching records (or distinct
values) — for a busy vault with millions/billions of historical records this
is an easy way to allocate gigabytes on the node in well under the 30s
timeout, since the scan is a fast local mmap/sequential read, not the
bottleneck. `sort` without an explicit `head`/`tail`/`slice` cap does the same
via full materialization. This single node also typically leads many vaults
(`histogramFullyLocal`, `LocalLeaderVaultIDs`), so the blast radius from one
crashed/thrashing node is every vault it leads, cluster-wide.

**Exploitability:** Authenticated, any role, single query, no special
timing or repetition needed — one request is enough given an unbounded/large
time range. Severity is highest when `MaxResultCount` is left at its
(unset) default, which is the out-of-the-box state.

**Remediation:**
1. Make `MaxGroupCardinality`-style bounds apply to per-accumulator state too
   (cap `dcountAcc`/`valuesAcc` set sizes, cap or reservoir-sample
   `medianAcc.vals`), and mark the result as `Truncated` the way group
   overflow already does.
2. Do not silently clear `q.Limit` for aggregating pipelines — instead bound
   the number of *input* records fed into the aggregator/sort by
   `s.maxResultCount` (with `Truncated: true` surfaced to the client), or
   require an explicit acknowledgement (`| stats ... limit=`) for unbounded
   scans.
3. Set a sane non-zero default for `Query.MaxResultCount` in both bootstrap
   paths so a fresh install isn't unlimited by default.

---

## Finding 3 — Unrecovered panics in detached query goroutines can crash the whole node (High)

**Files:**
`backend/internal/server/query.go:220-224`, `:503-509`
`backend/internal/server/query_follow.go:107-142`, `:161-171`

**What I verified:**
- `grep -rn "recover()"` across `internal/query`, `internal/querylang`,
  `internal/lookup`, and `internal/server` (excluding tests) returns **zero**
  matches. The only `recover()` calls in the whole backend query-adjacent area
  are in `internal/pipeline/ingestion/manager.go:414,510` (ingest path, not
  query).
- Go's `net/http` server recovers panics **per connection/request goroutine**
  automatically (this is standard library behavior, not something gastrolog
  implements) — so a panic inside the RPC handler's own goroutine fails only
  that request. But every `go func() { ... }()` launched *from inside* a
  handler runs in its own goroutine outside that protection: an unrecovered
  panic there is an unhandled goroutine panic, which terminates the entire
  process regardless of what triggered it.
- Concretely, `searchDirect` spawns a bare goroutine to compute the page-1
  histogram:
  ```go
  // query.go:220-224
  histCh = make(chan []*apiv1.HistogramBucket, 1)
  go func() {
      histCh <- s.computePageHistogram(ctx, eng, histogramQ, remoteHist, distributed, selectedVaults)
  }()
  ```
  and a second detached goroutine to drain that channel if the context is
  cancelled first (`query.go:508`). `computePageHistogram` calls into
  `eng.ComputeSearchPageHistogram`/`eng.ComputeHistogram`
  (`internal/query/histogram.go`, 1377 lines of bucket/index arithmetic) — any
  panic there (nil map, index arithmetic, malformed chunk data) is unrecovered
  and fatal to the process.
- `followRemoteRecords`/`mergeFollowStreams` (`query_follow.go:107-142`,
  `:161-171`) run the local search iterator and the cross-node merge inside
  detached goroutines the same way, with no recover.

**Attacker scenario:** Any input that reaches an edge case that panics inside
histogram bucketing, the search iterator, or follow merging — reachable by any
authenticated user via ordinary `Search`/`Follow` calls (see Finding 1: any
vault, any query shape) — kills the entire gastrolog process, not just the
one request. This is a full node outage (and, transitively, an outage for
every vault that node was leading) triggered by a query-shaped bug rather
than a crash confined to the RPC that hit it.

**Exploitability:** Authenticated, any role. Requires finding/triggering a
panicking edge case in the histogram/search/follow code paths (not
demonstrated here — this review did not find a concrete crashing input,
only the absence of the safety net). Rated High rather than Critical because
it is conditional on such a bug existing somewhere in a large, actively
developed 1300+ line file, not proven to be trivially triggerable.

**Remediation:** Wrap every detached goroutine reachable from a query RPC
(`query.go:221`, `query_follow.go:108`, `:139`, `:163`, and any future
`go func()` added to this package) with a `defer recover()` that logs and
degrades gracefully (e.g. empty histogram, closed channel) instead of letting
the panic escape unrecovered.

---

## Finding 4 — HTTP lookup table: serial per-record outbound requests, no concurrency/volume cap (Medium)

**Files:**
`backend/internal/lookup/http.go:214-264`
`backend/internal/query/pipeline_ops.go:956-995`

**What I verified:**
- `applyRecordLookupSingle`/`applyRecordLookupParameterized`
  (`pipeline_ops.go:957-995`) call `table.LookupValues(ctx, ...)`
  **synchronously, once per record (per field)**, with no batching or
  concurrency.
- For an `HTTP`-backed lookup table (`lookup/http.go`), `LookupValues`
  (`:216-264`) does an actual `http.Client.Do` GET (`doFetch`, `:277-333`)
  with a 5s default timeout (`defaultHTTPTimeout`, `:20`) on any cache miss.
  The cache is keyed by parameter value (`:221-229`) and capped at 10,000
  entries with an LRU-less full-clear-on-overflow policy (`:249-252`) — so
  attacker-influenced field values with high cardinality (request IDs, user
  agents, arbitrary attribute values from ingested logs) each cost one
  outbound request.
- The URL path is built by substituting `{param}` placeholders with
  `url.PathEscape(value)` (`:242-245`, `:269-272`) — the *host* is fixed by
  the admin-configured `URLTemplate` (configuring lookup tables is admin-only:
  `SystemServicePutLookupSettingsProcedure` is in the `admin` map,
  `interceptor.go:128`), but the *path/query segment* is attacker-influenced
  per record.

**Attacker scenario:** A non-admin, authenticated user runs
`* | lookup mytable(some_high_cardinality_field)` over a large time range.
Each distinct value not already cached triggers a blocking GET (up to 5s
each) to whatever internal endpoint the admin configured, run serially in the
RPC goroutine. This both (a) ties up the query for a long time (amplifying
the resource-exhaustion risk from Finding 2, since this is one of the
"streaming" pre-ops that still runs per-record without a distinct-request
cap) and (b) lets a low-privilege user drive a large number of
attacker-influenced requests at an internal system the admin trusted enough
to wire into lookups, which is a mild request-forgery-via-proxy concern (host
is fixed, path/query segment is not).

**Exploitability:** Authenticated, non-admin sufficient; requires an admin to
have configured at least one HTTP lookup table (this is opt-in
infrastructure, not present by default).

**Remediation:** Cap the number of *distinct* outbound lookups per query
(e.g. via `MaxGroupCardinality`-style truncation), and/or move to a
bounded-concurrency batch fetch with a shared per-query deadline separate
from the per-call HTTP timeout, so one query can't serially chain N×5s of
outbound calls.

---

## Regex DoS: checked, verified bounded (Informational)

**Files:** `backend/internal/querylang/parser.go:312-324`,
`backend/internal/querylang/glob.go:11-71`,
`backend/internal/server/query_convert.go:76,90-92`

- Every user regex predicate (`/pattern/`) is compiled once at parse time via
  `regexp.Compile("(?i)" + pattern)` (`parser.go:316`) and matched with Go's
  RE2-based `regexp` package (`Pattern.Match` in `scanner.go:531`) — no
  catastrophic backtracking is possible by construction.
- The whole query expression (which bounds total regex/glob pattern text) is
  capped at `maxExpressionLength = 4096` bytes (`query_convert.go:76,90-92`),
  enforced before any parsing/compilation happens.
- I verified empirically (standalone `go run`, not part of the repo or
  cluster) that Go's `regexp` package rejects the classic
  nested-repeat-count DoS pattern (`(?:(?:a{1000}){1000})`-style) at parse
  time with `invalid repeat count`, in microseconds, rather than building an
  exponential/huge automaton. A worst-case ~4000-byte pattern built from 500
  alternated `a{1000}` clauses compiled in ~0.2ms and matched a 100KB input in
  ~2ms. There is no per-query cap on the *number* of regex/glob predicates,
  but at 4096 bytes total and with per-pattern compile/match cost this cheap,
  that is not an exploitable amplification vector.
- **Conclusion:** regex compile-time and matching-time cost are not a
  meaningful DoS vector here, independent of and in addition to the
  memory-exhaustion issues in Finding 2 (which are not regex-related).

---

## Checked and sound

- **Scalar functions** (`querylang/scalar.go`): all pure, no file or network
  I/O, no `os`/`net` imports. Bounds-checked (`substr` clamps indices,
  `bitshl`/`bitshr` guard shift amounts `< 0 || >= 64`, `:92-103`).
  Type-mismatched/missing input returns `MissingValue()` rather than
  panicking or erroring (e.g. `mathFunc1`/`mathFunc2`, `:139-171`).
- **Arithmetic division/modulo** (`querylang/eval.go:160-169`): explicitly
  checks `rf == 0` and returns `NaN` instead of panicking — no divide-by-zero
  crash.
- **`slice` operator bounds**: the parser rejects `start <= 0` and
  `end <= 0`/`end < start` at parse time (`pipeline_parser.go:804-810,
  834-843`), so the negative-index slice panic that would otherwise be
  reachable in `applyRecordSlice` (`pipeline_ops.go:703-713`) is not
  reachable through normal query parsing.
- **No shell/exec anywhere in the query or CLI query path**: grepped
  `os/exec`/`exec.Command` across `cmd/gastrolog/cli`, `internal/query`,
  `internal/querylang` — zero matches. No command-injection vector found.
- **CLI export/import**: no `os.Create`/`os.WriteFile`/`filepath.Join` calls
  driven by query text found in `cmd/gastrolog/cli/export.go` or
  `import.go` — output paths are operator-supplied flags, not derived from
  query content.
- **Routing/digester expression evaluation** (`internal/pipeline/routing/table.go`):
  route match expressions use the same `querylang` parser/regex engine, but
  route configuration is admin-only (`SystemServicePutRouteProcedure` is in
  the `admin` map). The attacker-controlled side of this evaluation is
  ingested record *content* being matched against an admin-authored pattern —
  the intended, bounded per-record RE2 cost, not an attacker-supplied
  pattern. Out of scope for this lens's "attacker-supplied regex" concern.
- **Lookup table file sources**: JSON/YAML sources are capped at 10MB
  (`maxStructuredFileSize`, `filelookup.go:19`) and 100,000 transformed rows
  (`maxTransformRows`, `:303`), both enforced before the binary index is
  built; CSV is mmap'd directly (no in-process JSON-tree blowup). Configuring
  any lookup table (file, HTTP, MaxMind) is admin-only.
- **Query-timeout context propagation**: `ctx.Err()` is checked periodically
  inside the scan loops (`scanner.go:251,297,438`, `search.go:497-498,
  553-554,607,698,788,1203,1235`, `ts_index_scanner.go:263`), so a configured
  `queryTimeout` does eventually abort a long-running per-record scan — it
  just doesn't reclaim memory already allocated by an aggregator/sort buffer
  before the deadline hits (see Finding 2).
