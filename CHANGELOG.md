# Changelog

All notable changes to GastroLog are documented here.

## v0.6.0 — 2026-02-26

### Features
- **Pipeline lookup operator** — `lookup` operator for field enrichment with reverse DNS, GeoIP (MaxMind MMDB with auto-download), and ASN lookup tables
- **World map visualizations** — choropleth and scatter map visualizations with zoom/pan
- **Visualization operators** — explicit `| timechart`, `| map choropleth`, `| map scatter`, etc. replace heuristic chart selection
- **Server-side query highlighting and validation** — syntax highlighting and field discovery moved to backend
- **Line comments** — `#` comments in queries
- **Pipeline records in log viewer** — pipeline results stream back to the log viewer when no aggregation is applied
- **Per-store metrics** — internal metrics ingester emits per-store record counts, chunk counts, and data bytes on a configurable interval

### Performance
- **Apache ECharts** — all visualizations migrated from visx to ECharts
- **zstd ingestion** — HTTP and OTLP ingesters accept zstd-compressed payloads

### Fixes
- Config poll no longer causes interactive chart state (zoom, pan) to reset every 10 seconds
- Negative numbers now parse correctly in `head`/`tail`/`slice`/`timechart`
- Time range buttons no longer strip newlines from queries

## v0.5.0 — 2026-02-24

### Features
- **Pipeline autocomplete** — context-aware autocomplete for pipe operators with a declarative grammar table. After `|`, suggests keywords; inside operator bodies, suggests only what the grammar allows (aggregation functions, fields, `by`/`as` clauses, etc.)
- **Unified timechart histogram** — the severity histogram is now powered by the `timechart` pipeline operator, producing stacked bar charts with animated transitions via visx
- **Auto-refresh for filter queries** — filter queries (not just pipelines) now support configurable auto-refresh polling
- **Configurable follow mode buffer** — sliding window size for follow mode is now user-configurable
- **Tail and slice operators** — new `tail N` and `slice START END` pipeline operators for result subsetting
- **Ingester default params** — backend serves ingester type defaults via `GetIngesterDefaults` RPC, so the frontend no longer hardcodes them

### Fixes
- Eval pipe function calls (`upper`, `lower`, `len`, etc.) now highlight correctly — `GetSyntax` RPC returns all scalar functions, and the tokenizer correctly classifies function calls in eval expressions
- Histogram tooltip order matches the visual bar stacking, with the hovered level highlighted
- Previous results stay visible while a new search is loading (no flash of empty state)
- Empty ingester params treated as unset instead of rejected
- Expired tokens silently refresh instead of showing error toasts

## v0.4.1 — 2026-02-23

### Features
- **Cancel button for queries** — the search button transforms into a cancel button while a query is running, allowing you to abort long-running pipeline queries

### Fixes
- Pipeline record operators (`where`, `eval`, `sort`, `rename`, `fields`, `raw`) now see extracted fields (KV pairs, JSON, logfmt) from the log message, not just explicit attributes
- Pipeline queries no longer require a filter expression before the pipe — `last=5m | raw` and `| stats count` now work as implicit match-all

## v0.4.0 — 2026-02-23

### Features
- **Pipeline query language** — pipe-based transformation pipeline extending the query language with post-filter operators separated by `|`. Operators: `stats` (aggregation with 10 functions: count, sum, avg, min, max, dcount, median, first, last, values), `where`, `eval`, `sort`, `head`, `rename`, `fields`, `raw`. Time bucketing with `bin()`, grouping with `by`, aliases with `as`.
- **Expressions** — full arithmetic (`+`, `-`, `*`, `/`, `%`), comparison operators, and 20+ scalar functions (`len`, `lower`, `upper`, `trim`, `substr`, `replace`, `coalesce`, `if`, `toNumber`, `round`, `ceil`, `floor`, `abs`, `pow`, `log`, `now`, etc.) usable in `stats`, `eval`, `where`, and filter expressions.
- **Comparison operators in filter expressions** — `!=`, `>`, `>=`, `<`, `<=` (e.g. `status>=500`)
- **Pipeline visualizations** — time series charts for `bin()` queries with hover crosshair and tooltips, single-value display for scalar aggregations, sortable tables for grouped results, chart/table toggle, and auto-refresh polling at configurable intervals.
- **Self-monitoring metrics ingester** — built-in ingester type that ships GastroLog's own runtime metrics as log records
- **System metrics in header** — replaced record/store/ingester counts with live CPU, memory, and storage utilization; detailed memory breakdown in inspector metrics panel

### Performance
- Reduced memory allocation churn during pipeline auto-refresh polling
- Channel-based pool for brotli compression writers (prevents GC eviction)
- Bounded zstd encoder concurrency to prevent unbounded memory growth

### Fixes
- Time series chart correctly renders negative Y values
- Missing values in time series chart are skipped instead of plotted as zero
- Follow mode surfaces non-retriable errors instead of silently reconnecting
- Proactive JWT refresh before token expiry

## v0.3.2 — 2026-02-22

### Performance
- **Release workflow 5x faster** — Docker job reuses pre-built linux binaries via artifacts instead of rebuilding from source (25 min → 5 min)

## v0.3.1 — 2026-02-22

### Features
- **Multiple retention rules per store** — stores now support an array of retention rules, each pairing a policy with an action (expire or migrate). Enables tiered storage where chunks age-out from hot to cold stores automatically.
- **Prometheus `/metrics` endpoint** — exposes Go runtime, ingestion, and storage metrics in Prometheus format

### Fixes
- Stores created through the UI now correctly apply rotation policies (age, size, record count thresholds were silently ignored)
- Age-based rotation now triggers via background sweep (every 15s) even when no records are flowing to a store
- Chunks sealed by cron rotation or background sweep now get compressed and indexed (previously skipped the post-seal pipeline)
- Stable sort order for stores list in inspector panel

### Breaking Changes
- JSON config store (`--config-type json`) removed — use SQLite (default) or memory
- SQLite migration 004 converts the single `retention` column to a `store_retention_rules` junction table. Existing single-policy references are preserved as expire rules.

## v0.3.0 — 2026-02-22

### Features
- **GIN-style structural JSON index** — deep path indexing with null-byte separators, array traversal, path existence and path-value equality queries. Two-pass mmap build with shared string dictionary. Complements existing KV indexer for non-JSON formats.
- OTLP, Fluent Forward, and Kafka ingesters with backpressure signaling
- Metrics tab in Inspector with system, queue, storage, and ingestion widgets
- Chatterbox generates deeply nested JSON (8 variants: HTTP, error, Kubernetes, distributed trace, database, pipeline, etc.)

### Performance
- Code splitting: help markdown (40 files), react-markdown, and mermaid lazy-loaded — 45% main bundle reduction (1,650 KB to 901 KB)

### Fixes
- Standardized X-in-circle close button across all dialog sizes
- Expired token now silently redirects to login instead of showing error toast
- Replaced horizontal type buttons with dropdowns in settings
- Improved toast readability with solid backgrounds and severity accents

## v0.2.3 — 2026-02-21

### Features
- Clickable "used by" entity references in settings (navigate to referencing entity)
- Help topics reorganized by data flow with full-text search
- Renamed "routing" to "filtering" across help and backend for clarity

### Fixes
- Used placeholder as default entity name, disabled Create on name conflicts
- Aligned detail panel key-value columns with baseline and gap

## v0.2.2 — 2026-02-21

### Features
- Multiple color theme palettes (Observatory, Meridian, Patina, Aperture, Signal)
- Responsive tablet layout with touch-friendly controls

## v0.2.1 — 2026-02-20

### Improvements
- Improved wizard and settings form UX
- Setup wizard dismissal tracked server-side instead of localStorage

## v0.2.0 — 2026-02-20

### Features
- `--no-auth` mode to disable authentication entirely
- Preferences dialog in user menu (separate from admin settings)
- Syntax highlighting intensity toggle (full / muted / off)
- Entity renaming with surrogate keys (stores, ingesters, filters, policies)
- Server-side max result count for Search requests
- Refresh tokens with configurable duration and validation guards
- TLS hardening, rate limiting, and token revocation

### Fixes
- Rejected `max_concurrent_jobs < 1` in server config
- Sanitized JSON parse errors in HTTP ingester responses

### Security
- P2 security hardening pass (input validation, error sanitization, token lifecycle)
- JWT secret storage security documentation
- Security warning on `--pprof` flag

## v0.1.2 — 2026-02-19

### Improvements
- Consolidated `useState` to `useReducer` and extracted sub-components in SearchView
- Accessibility and event handler improvements (keyboard navigation, ARIA)
- React Compiler error resolution and key stability improvements

### Fixes
- Fixed follow mode routing bug (time directive injection broke follow)
- Preserved current route in `useNavigate` instead of hardcoding `/search`

## v0.1.1 — 2026-02-19

### Features
- Clickable syntax-highlighted spans to populate search bar
- Clickable example values in settings form inputs

### Fixes
- Fall back to substring search for non-indexable bare tokens
- Prevented runaway infinite scroll on empty search
- Used actual disk bytes instead of logical bytes for storage stats

## v0.1.0 — 2026-02-19

### Features
- React Compiler enabled with redundant memoization removed
- Inline icon support in help markdown
- Saved Queries help topic

### Fixes
- Stopped MermaidDiagram re-rendering every few seconds in help dialog
- Removed useless render-time clock from ResultsToolbar
- Resolved ESLint warnings and SonarQube hints (nested ternaries, accessibility, complexity)
- Help dialog UX improvements

## v0.0.6 — 2026-02-19

### Fixes
- Removed flaky empty-Raw assertion from chatterbox test

## v0.0.5 — 2026-02-19

### Fixes
- Populated severity level counts in filtered histogram path
- Made `GetServerConfig` public so password rules show on registration page
- Moved user preferences into JSON column on users table

## v0.0.4 — 2026-02-19

### Features
- Route settings and inspector dialogs via URL search params (browser back/forward support)
- Route help navigation via URL search params
- Recipes help section with ingester configuration guides (rsyslog, Promtail, journald, Docker mTLS)
- Password policy with complexity rules and inline visualizer
- Skip button on setup wizard
- About section in help system
- Configurable HTTPS port in service settings

### Fixes
- Collapse detail pane when unpinning with no record selected
- Unified dialog navigation and panel header patterns
- Aligned certificate card actions and auto-sized settings nav
- Improved inspector empty states and metric labels
- Clarified ingester "Errors" metric as "Dropped"
- Updated chatterbox help to match UI

### Housekeeping
- Squashed 12 SQLite migrations into single `001_init.sql`
- Docker filter form unified with querylang filter engine
- Added `compose.yml` with config/store volumes and ingester ports
- Filled in README.md

## v0.0.3 — 2026-02-18

### Changed
- Parallelized release workflow into 4 concurrent jobs for faster builds

## v0.0.2 — 2026-02-18

### Changed
- Multi-arch Docker builds (amd64 + arm64) in release workflow

## v0.0.1 — 2026-02-18

The first release of GastroLog — a chunk-based log management system with a React frontend.

### Core Engine
- Chunk-based storage with file and memory backends
- Two-pass token indexer with bounded memory usage
- Time, source, attribute, and key-value indexes for sealed chunks
- Boolean query language with DNF execution and index-accelerated scans
- Multi-store search with heap-based merge
- Follow mode (tail -f style) with live streaming
- Explain command for query execution plans
- Index analyzer for per-chunk diagnostics

### Storage & Indexing
- Dictionary encoding for attribute keys and values
- Crash recovery: truncate orphaned raw.log, rebuild missing indexes
- Directory lock to prevent multiple processes
- Rotation policies (cron, size, count-based)
- Retention policies (TTL, size-based, count-based) with background cleanup
- Optional zstd compression for sealed chunks (seekable random-access)

### Ingestion
- Syslog (RFC 3164/5424), HTTP (Loki Push API), RELP, File Tail, Docker
- Chatterbox test data generator with multiple log formats
- Digester pipeline with level extraction and timestamp parsing
- Attribute-based routing for log messages
- Format-specific KV extractors (logfmt, JSON, access log)

### Query Language
- Boolean expressions (AND, OR, NOT) with parentheses
- `key=value` attribute and message filters
- `store=`, `chunk=`, `pos=` filters
- Time range filters (`start=`, `end=`, `last=`) with human-friendly timestamps
- Regex search (`/pattern/`) and glob patterns (`pattern*`)
- Severity filtering

### Frontend
- Observatory dark-first design with Cormorant Garamond / Libre Franklin / IBM Plex Mono
- Light/dark/system theme toggle
- Syntax-highlighted log messages with severity badges
- Severity-stacked histogram with clickable segments
- Field explorer sidebar with auto-complete
- Resizable/collapsible detail panel with context records (show surrounding logs)
- Infinite scroll, query history, clipboard copy, result export
- Follow mode with auto-reconnect and "X new logs" badge
- URL-based search state with TanStack Router
- Settings dialog: stores, ingesters, filters, rotation/retention policies, certificates, users, server config
- Inspector: store chunk browser with timeline visualization, ingester metrics
- Jobs tab with streaming updates for async operations
- Help system with 40+ markdown topics, Mermaid diagrams, and contextual help buttons
- Saved queries
- First-time setup wizard

### Server & Operations
- Connect RPC server (QueryService, StoreService, ConfigService, LifecycleService, AuthService, JobService)
- JWT authentication with refresh tokens, rate limiting, TLS hardening
- User management (admin API, registration, password policy)
- Brotli/gzip response compression
- Embedded frontend assets in Go binary
- Cobra CLI (`gastrolog server [flags]`)
- Docker image (multi-arch: amd64 + arm64), systemd service, Helm chart
- GitHub Actions CI and release workflows
- Kubernetes liveness and readiness probes
- Graceful shutdown with drain
