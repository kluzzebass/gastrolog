# Vision Issue Breakdown

This document breaks the vision into epics and issues, with dependencies. Each issue traces back to a specific part of `vision.md`. Issues are numbered for cross-referencing; `→` indicates a blocking dependency.

---

## Epic 1: Tiered Storage

The foundation that most other epics depend on. Must be built first.

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 1.1 | Decouple vault from single storage backend (vault as logical container) | — | Tiered Storage: vault as logical container |
| 1.2 | Tier chain configuration model (ordered list of tier backends per vault) | 1.1 | Tiered Storage: vault as logical container |
| 1.3 | Tier interface: active chunk + sealed chunks + rotation + retention per tier | 1.1 | Tiered Storage: tier types |
| 1.4 | Memory tier backend (extract from current memory vault) | 1.3 | Tiered Storage: tier types |
| 1.5 | Local SSD tier backend (extract from current file vault) | 1.3 | Tiered Storage: tier types |
| 1.6 | S3 tier backend (active chunk on local disk, sealed chunks in S3) | 1.3 | Tiered Storage: tier types |
| 1.7 | GCS tier backend | 1.6 | Tiered Storage: tier types |
| 1.8 | R2 tier backend | 1.6 | Tiered Storage: tier types |
| 1.9 | Archival tier backend (Glacier/Archive storage class transitions) | 1.6 | Tiered Storage: tier types |
| 1.10 | Per-tier primary election | 1.3 | Tiered Storage: per-tier primary nodes |
| 1.11 | Memory tier replication (write-mirror to secondary) | 1.4, 1.10 | Tiered Storage: replication |
| 1.12 | Local SSD tier replication (sealed chunk copy to secondary) | 1.5, 1.10 | Tiered Storage: replication |
| 1.13 | Inter-tier record streaming (primary-to-primary) | 1.10 | Tiered Storage: inter-tier record streaming |
| 1.14 | Durability handoff protocol (durable ack before source tier drops chunk) | 1.13 | Tiered Storage: durability handoff |
| 1.15 | Time-based tier transition policy | 1.13 | Tiered Storage: tier transitions |
| 1.16 | Size-based tier transition policy | 1.13 | Tiered Storage: tier transitions |
| 1.17 | Budget-based tier transition policy | 1.13 | Tiered Storage: tier transitions |
| 1.18 | Access-based tier transition policy (query frequency tracking) | 1.13 | Tiered Storage: tier transitions |
| 1.19 | Cross-tier query fan-out (progressive results from each tier) | 1.3 | Tiered Storage: transparent query fan-out |
| 1.20 | On-demand promotion (cold → warm cache fetch) | 1.6 | Tiered Storage: on-demand promotion |
| 1.21 | Warm cache eviction (drop local copy when durable in colder tier) | 1.20 | Tiered Storage: on-demand promotion |
| 1.22 | Research: chunk metadata storage at scale (Raft vs gossip vs hybrid) | 1.10 | Tiered Storage: open design question |
| 1.23 | Migrate existing memory/file/cloud vaults to tier chain model | 1.4, 1.5, 1.6 | Tiered Storage: vault as logical container |

**Contradictions / risks:**

- **1.14 + replication timing**: The durability handoff requires waiting for replication ack before the source tier drops a chunk. If the destination tier's secondaries are slow or unreachable, this blocks the source tier's retention from running. Need a policy for what happens when the ack is delayed — does the source tier hold indefinitely, or is there a timeout with data loss acceptance?
- **1.10 + "no primary node" principle**: Per-tier primaries reintroduce the concept of a node "owning" a responsibility. The CLAUDE.md states "there is no primary node." The distinction is that tier primaries are per-tier-per-vault (fine-grained, dynamic, redistributable), not per-node. But the tension should be acknowledged — this is a deliberate, scoped exception to the general principle.
- **1.6 active chunk locality**: Cloud tier active chunks live on the tier primary's local disk. If the primary dies before sealing, the active chunk is lost. This is the same risk as the memory tier, but for cloud tiers it's less obvious since the expectation is "my data is in S3." The durability handoff (1.14) mitigates this for data from previous tiers, but records that arrived directly into the cloud tier's active chunk are at risk until sealed and uploaded.

---

## Epic 2: Programmable Ingestion

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 2.1 | Parse operator for route pipelines (syslog, JSON, logfmt, etc.) | — | Programmable Ingestion: transform on ingest |
| 2.2 | Enrich operator (geoip on field) | — | Programmable Ingestion: transform on ingest |
| 2.3 | Lookup operator for route pipelines (external data source join) | — | Programmable Ingestion: transform on ingest |
| 2.4 | Redact operator (field-level masking at ingestion) | — | Programmable Ingestion: transform on ingest |
| 2.5 | Sampling stage (per-severity percentage, runtime adjustable) | — | Programmable Ingestion: sampling |
| 2.6 | Sample rate annotation field on records | 2.5 | Programmable Ingestion: sampling |
| 2.7 | Sample-aware aggregation (extrapolate in stats queries) | 2.6 | Programmable Ingestion: sampling |
| 2.8 | Route forking (branch to parallel paths with independent transforms) | — | Programmable Ingestion: fork and fan-out |
| 2.9 | Route-by-field stage (dynamic destination based on field value) | — | Programmable Ingestion: transform on ingest |
| 2.10 | Visual route editor: stage card picker (categorized operator list) | 2.1, 2.2, 2.3, 2.4, 2.5, 2.8, 2.9 | Programmable Ingestion: visual route editor |
| 2.11 | Visual route editor: card configuration forms | 2.10 | Programmable Ingestion: visual route editor |
| 2.12 | Visual route editor: flow connections and fork visualization | 2.10 | Programmable Ingestion: visual route editor |
| 2.13 | Visual route editor: read-only pipeline syntax preview | 2.10 | Programmable Ingestion: visual route editor |
| 2.14 | Route pipeline execution engine (apply pipeline to records at ingestion time) | 2.1 | Programmable Ingestion: transform on ingest |

**Contradictions / risks:**

- **2.14 + performance**: Running a pipeline (parse, geoip lookup, external API call) on every ingested record adds latency to the ingestion path. If a lookup stage calls an external API that's slow or down, it could block ingestion entirely. Need to define behavior for slow/failed stages — skip, buffer, drop, dead-letter queue?
- **2.4 + compliance (Epic 7)**: The redact operator is listed here and field-level encryption is in Epic 7. These are different mechanisms (irreversible masking vs. reversible encryption). The vision references both without clarifying when to use which. Should the route editor offer both as distinct stages?

---

## Epic 3: Query Language Extensions

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 3.1 | Subquery support in pipeline parser | — | Query Language: subqueries |
| 3.2 | Subquery execution in query engine (nested pipeline evaluation) | 3.1 | Query Language: subqueries |
| 3.3 | Computed virtual columns: definition and persistence in cluster config | — | Query Language: computed virtual columns |
| 3.4 | Computed virtual columns: evaluation during query execution | 3.3 | Query Language: computed virtual columns |
| 3.5 | Timeline operator (group results by field, render as timeline) | — | Query Language: subqueries (example) |
| 3.6 | Saved query poll interval (auto-refresh) | — | Query Language: live dashboards |
| 3.7 | Dashboard view (collection of saved queries as panels) | 3.6 | Query Language: live dashboards |

---

## Epic 4: Traces and Logs Unification

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 4.1 | Span field detection (trace_id, span_id, parent_span_id, duration) | — | Traces: automatic shape detection |
| 4.2 | Waterfall diagram renderer | 4.1 | Traces: automatic shape detection |
| 4.3 | Automatic result shape switching (log list vs. waterfall) | 4.1, 4.2 | Traces: automatic shape detection |
| 4.4 | Implicit time-window correlation (group by shared field within time window) | — | Traces: correlation without instrumentation |
| 4.5 | Span parent-child index | — | Traces: span indexing |
| 4.6 | `children` query operator | 4.5 | Traces: span indexing |
| 4.7 | `critical_path` query operator | 4.5 | Traces: span indexing |

**Contradictions / risks:**

- **4.3 + user expectation**: Automatic shape switching could be surprising. If a user searches for `level=error` and some results happen to have span fields, switching to waterfall mid-result-set would be confusing. Need clear rules for when to switch (all results have span fields? majority? separate rendering per record?).

---

## Epic 5: UI as Instrument

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 5.1 | Comprehensive keyboard shortcut system (T, C, F, D mappings) | — | UI: keyboard-driven investigation |
| 5.2 | Record pinning (pin multiple records in detail panel) | — | UI: detail panel as workspace |
| 5.3 | Record diffing (side-by-side comparison of pinned records) | 5.2 | UI: detail panel as workspace |
| 5.4 | Record annotation (attach notes to records) | — | UI: detail panel as workspace |
| 5.5 | Workspace persistence across page reloads | 5.2, 5.4 | UI: detail panel as workspace |
| 5.6 | Investigation model (query + time range + selected records + annotations) | 5.4 | UI: saveable investigations |
| 5.7 | Investigation save/load | 5.6 | UI: saveable investigations |
| 5.8 | Investigation permalinks | 5.7 | UI: saveable investigations |
| 5.9 | Responsive density (adapt layout to screen size and pixel density) | — | UI: responsive density |
| 5.10 | Query bar stage-based inline builder (visual pipeline stages) | — | (from design discussion, not in vision text) |

---

## Epic 6: Collaboration

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 6.1 | Team namespace for saved queries | — | Collaboration: shared saved queries |
| 6.2 | Real-time presence infrastructure (WebSocket channels) | — | Collaboration: presence awareness |
| 6.3 | Presence indicators in timeline (avatar in gutter) | 6.2 | Collaboration: presence awareness |
| 6.4 | Investigation timeline (audit trail of queries and selections during incident) | 5.6 | Collaboration: investigation timeline |
| 6.5 | Investigation handoff (structured transfer with summary and next steps) | 5.7 | Collaboration: handoff protocol |

**Contradictions / risks:**

- **6.2 + cluster architecture**: Presence awareness requires real-time state about which users are looking at what. In a multi-node cluster, this state needs to be shared across nodes (user on node-1 should see presence of user on node-2). This is a new type of ephemeral, high-frequency state that doesn't fit Raft (too chatty) or the existing PeerState broadcast (designed for stats, not user sessions). Needs its own dissemination mechanism.

---

## Epic 7: Compliance

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 7.1 | Purge command: delete records matching expression across all vaults/tiers/nodes | 1.3 | Compliance: right to erasure |
| 7.2 | Purge audit trail and compliance certificate | 7.1 | Compliance: right to erasure |
| 7.3 | Field-level encryption at ingestion (route pipeline stage) | 2.14 | Compliance: field-level encryption |
| 7.4 | Role-based field decryption (PII role sees real values, others see masked) | 7.3 | Compliance: field-level encryption |
| 7.5 | Audit vault (log all queries, record accesses, exports) | — | Compliance: access auditing |
| 7.6 | Data residency constraints (pin vault tiers to specific nodes/regions) | 1.10 | Compliance: data residency |
| 7.7 | Cryptographic retention enforcement (verifiable deletion proof) | 1.3 | Compliance: retention enforcement |

**Contradictions / risks:**

- **7.6 + per-tier primaries (1.10)**: If a vault has a data residency constraint (EU only), all tier primaries for that vault must be on EU nodes. The tier primary election (1.10) must be residency-aware. If no EU nodes are available, the vault can't accept writes — need to define this failure mode.

---

## Epic 8: Anomaly Detection

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 8.1 | Research: behavioral baseline algorithm selection | — | Anomaly Detection: behavioral baselines |
| 8.2 | Per-source baseline model (field distributions, cadence, severity mix) | 8.1 | Anomaly Detection: behavioral baselines |
| 8.3 | Anomaly score computation and annotation on records/timeline | 8.2 | Anomaly Detection: quiet annotations |
| 8.4 | anomaly_score as queryable field | 8.3 | Anomaly Detection: queryable anomalies |
| 8.5 | Root cause field change detection | 8.2 | Anomaly Detection: root cause correlation |
| 8.6 | UI: subtle anomaly indicators in severity bar and timeline | 8.3 | Anomaly Detection: quiet annotations |

**Contradictions / risks:**

- **8.3 + storage**: Where do anomaly scores live? They're described as queryable fields, but they're computed, not stored. Computing them at query time for every record is expensive. Pre-computing and storing them means every record gets an extra field, increasing storage. Alternatively, anomaly scores could be per-chunk or per-time-bucket summaries rather than per-record, which changes the query semantics.

---

## Epic 9: Multi-Tenancy

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 9.1 | Tenant model in config (tenant entity, tenant-vault association) | — | Multi-Tenancy: tenant isolation |
| 9.2 | Tenant-scoped query isolation (enforce vault boundaries per tenant) | 9.1 | Multi-Tenancy: tenant isolation |
| 9.3 | Per-tenant encryption keys | 9.1 | Multi-Tenancy: per-tenant encryption |
| 9.4 | Bring-your-own-key (BYOK) support | 9.3 | Multi-Tenancy: per-tenant encryption |
| 9.5 | Tenant-aware routing (identify tenant at ingestion, route to tenant vault) | 9.1, 2.9 | Multi-Tenancy: tenant-aware routing |
| 9.6 | Per-tenant resource quotas (ingestion rate, storage, query concurrency) | 9.1 | Multi-Tenancy: resource quotas |
| 9.7 | Tenant lifecycle management API (onboard, offboard, migrate) | 9.1 | Multi-Tenancy: managed service model |
| 9.8 | Tenant-scoped billing metrics | 9.6 | Multi-Tenancy: managed service model |

---

## Epic 10: Self-Healing Cluster

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 10.1 | Automatic tier primary re-election on node failure | 1.10 | Self-Healing: graceful degradation |
| 10.2 | Automatic vault rebalancing on node join/leave | 1.10 | Self-Healing: automatic vault rebalancing |
| 10.3 | Storage pressure detection and automatic tier demotion | 1.15, 1.16 | Self-Healing: storage pressure management |
| 10.4 | Partial-data query response (answer with available data, indicate gaps) | 1.19 | Self-Healing: graceful degradation |
| 10.5 | Capacity planning signals in inspector | — | Self-Healing: capacity planning signals |

---

## Epic 11: CLI Enhancements

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 11.1 | Stdin piping mode (`--stdin` for chaining queries) | — | CLI: full query language |
| 11.2 | Terminal chart rendering (Unicode block characters) | — | CLI: full query language |
| 11.3 | Parquet export format | — | CLI: pipe-friendly output |
| 11.4 | Shared state sync (saved queries, history between CLI and UI) | — | CLI: shared state |
| 11.5 | Purge command in CLI | 7.1 | CLI: scriptable administration |

---

## Epic 12: Performance

| # | Issue | Depends on | Vision section |
|---|-------|-----------|----------------|
| 12.1 | Benchmark: index-driven query latency target (< 10ms for point lookups) | — | Speed: index-driven queries |
| 12.2 | Benchmark: full-text scan throughput target (TB/s across cluster) | — | Speed: full-text scan |
| 12.3 | Follow mode latency audit (measure ingestion-to-screen pipeline) | — | Speed: follow mode latency |
| 12.4 | Startup time benchmark (target < 3s to serving) | — | Speed: startup time |

---

## Cross-Epic Dependencies

Key dependencies that span epics:

| Downstream | Depends on | Reason |
|---|---|---|
| 2.10 (visual route editor) | 2.1–2.9 (all route operators) | Can't build the picker until the operators exist |
| 5.6 (investigation model) | — | **Keystone**: collaboration (6.4, 6.5), CLI shared state (11.4), and handoff all build on this |
| 7.1 (purge) | 1.3 (tier interface) | Must purge across all tiers |
| 7.3 (field encryption) | 2.14 (route pipeline engine) | Encryption is a pipeline stage |
| 7.6 (data residency) | 1.10 (per-tier primaries) | Primary election must be residency-aware |
| 9.5 (tenant routing) | 2.9 (route-by-field) | Tenant identification is a routing decision |
| 10.1–10.3 (self-healing) | 1.10 (per-tier primaries) | Healing requires re-election and rebalancing of tier primaries |

## Summary of Contradictions and Open Questions

1. **Durability handoff timeout** (1.14): If the destination tier's replication is slow or unreachable, the source tier can't drop its chunk. Unbounded hold is a resource leak; timeout means potential data loss. Need a policy.

2. **Route pipeline failure mode** (2.14): A slow or failed pipeline stage (external API lookup) could block ingestion. Need defined behavior: skip, buffer, dead-letter, timeout.

3. **Anomaly score storage** (8.3): Per-record scores are expensive to store; per-bucket scores change query semantics. Need to decide granularity.

4. **Automatic shape detection threshold** (4.3): When do results switch from log list to waterfall? All results must have span fields? Majority? Per-record rendering? Surprising behavior if mixed.

5. **Presence state dissemination** (6.2): User presence is ephemeral, high-frequency state that doesn't fit Raft or PeerState broadcasts. Needs its own mechanism (dedicated WebSocket hub, CRDT, or gossip protocol).

---

## Architectural Conflict: Encryption

The vision mentions per-tenant encryption, field-level encryption, and BYOK in several places, but these claims conflict with fundamental architectural decisions elsewhere in the vision. This section documents the conflicts and how comparable systems handle them.

### Conflict 1: Encryption at rest vs. mmap

The performance pillar depends on mmap for zero-copy reads of sealed chunks on local SSD. Encryption at rest is incompatible with mmap — you can't memory-map an encrypted file and read plaintext records from it. Decrypting requires reading into heap memory, which is the exact pattern GastroLog's architecture forbids (see CLAUDE.md: "NEVER use os.ReadFile to slurp entire files into heap memory").

**How others handle it:**

- **Elasticsearch**: Does not encrypt data at rest natively. Delegates to filesystem-level encryption (LUKS, dm-crypt, BitLocker) or cloud-managed encrypted volumes (AWS EBS encryption, GCP CMEK). The application never sees the encryption — the OS transparently decrypts on read. Lucene segments are mmap'd as usual.
- **Splunk**: Same approach — relies on OS/volume-level encryption. Splunk's indexer mmap's tsidx files directly. Encryption is below the application layer.
- **ClickHouse**: Supports application-level encryption of MergeTree parts via encrypted disks, but this uses a virtual filesystem layer that decrypts blocks on read into buffers — no mmap. Performance penalty acknowledged in docs.
- **Loki**: No encryption at rest. Delegates to the object store's encryption (S3 SSE, GCS CMEK).

**Implication for GastroLog:** Application-level encryption of the local SSD tier would break the mmap architecture. The practical path is filesystem/volume-level encryption (LUKS, encrypted EBS volumes), which is transparent to the application. The vision's claim of "per-tenant encryption keys" at the vault level is not achievable with mmap unless each tenant's data lives on a separate encrypted filesystem — possible but operationally complex. For cloud tiers, server-side encryption (S3 SSE-KMS) handles per-tenant keys naturally.

### Conflict 2: Field-level encryption vs. indexing

If a field is encrypted at ingestion, its value is opaque to the index. You cannot search for `credit_card=1234` if the credit card number is encrypted. You cannot aggregate, sort, or filter on encrypted fields. This conflicts with the "query anything" model.

**How others handle it:**

- **Elastic**: Field-level encryption is not supported. PII handling is done via ingest pipeline `remove` processors (irreversible deletion) or document-level security (access control, not encryption). The data is searchable because it's plaintext in the index.
- **Splunk**: Offers field-level hashing (one-way, not reversible) for compliance. Hashed fields can be compared for equality (same hash = same value) but not searched by plaintext value, not range-queried, not aggregated. This is a deliberate tradeoff — compliance over queryability.
- **Datadog**: Sensitive Data Scanner redacts or hashes fields at ingestion. Redacted fields show `[REDACTED]` in the UI. No reversible encryption. No search on redacted values.
- **CockroachDB**: Column-level encryption planned but not shipped as of 2025. Acknowledged as fundamentally incompatible with secondary indexes on encrypted columns.

**Implication for GastroLog:** True field-level encryption (reversible, with role-based decryption) means encrypted fields are unsearchable and unindexable. The vision's example ("analysts see `credit_card=****` unless they have the PII role") is achievable only if the field is stored in plaintext and masked at query time based on role — which is access control, not encryption. The data is still on disk in the clear. If actual encryption is required (data at rest must be ciphertext), the field becomes opaque to queries. The vision should clarify which model it means: role-based display masking (practical, queryable) vs. actual field-level encryption (compliant, not queryable).

### Conflict 3: Encryption vs. compression

Encrypted data does not compress. The correct order is compress-then-encrypt. But GastroLog's sealed chunk format has indexes that reference byte offsets into the compressed data. If encryption wraps the compressed data:

- **Indexes outside encryption boundary**: Index files contain field names, token lists, timestamp ranges — metadata that reveals the structure and content of the encrypted data. An attacker with disk access sees what fields exist, what values are indexed, and when records were written, even without the decryption key.
- **Indexes inside encryption boundary**: Every index lookup requires decryption first. For a query that checks 1,000 chunk indexes to find matching ones, this means 1,000 decryption operations before the query engine even knows which chunks are relevant. This destroys the "index-driven queries return in milliseconds" goal.

**How others handle it:**

- **Elasticsearch / Splunk / Loki**: Avoid the problem entirely by using volume-level encryption (transparent to the application). Indexes and data are both encrypted at the filesystem level, decrypted transparently on read by the OS.
- **ClickHouse**: Encrypted disk implementation encrypts whole files (data + indexes together) and decrypts blocks on read. No mmap. Accepts the performance cost.

**Implication for GastroLog:** Application-level encrypt-after-compress with separate index treatment is complex and leaks metadata. Volume-level encryption avoids all of these issues. For cloud tiers, S3 SSE-KMS provides per-key encryption transparently.

### Conflict 4: BYOK trust model

"Even the cluster operator cannot read their data without the tenant's cooperation" requires that decryption keys never touch the server in plaintext. This is only achievable with envelope encryption backed by an external KMS (AWS KMS, GCP Cloud KMS, HashiCorp Vault). The data encryption key (DEK) is stored alongside the data but encrypted with a key-encryption key (KEK) that lives in the external KMS. Decryption requires an API call to the KMS, which the tenant controls.

**How others handle it:**

- **Elasticsearch**: Supports KMS-backed encryption via the keystore and encrypted snapshot repositories. BYOK via AWS KMS or GCP CMEK. The server holds wrapped DEKs; the KEK never leaves the KMS.
- **Splunk**: BYOK via AWS KMS for Splunk Cloud. Self-managed Splunk has no BYOK — the operator controls everything.
- **Datadog / Loki**: No BYOK. SaaS trust model — you trust the provider.

**Implication for GastroLog:** BYOK is achievable for cloud tiers (S3 SSE-KMS with customer-managed keys is a standard AWS feature). For local tiers, it requires integration with an external KMS for envelope encryption, plus the performance cost of KMS API calls on every chunk seal/read. The vision should scope BYOK to cloud tiers initially and treat local-tier BYOK as a separate, later effort.

### Recommendation

Update the vision's encryption claims to reflect reality:

1. **At-rest encryption** for local tiers: delegate to volume/filesystem-level encryption (LUKS, encrypted EBS). Transparent to the application, preserves mmap.
2. **At-rest encryption** for cloud tiers: use provider-native encryption (S3 SSE-KMS, GCS CMEK). Per-tenant keys are natural here.
3. **Field-level "encryption"**: clarify as role-based display masking (data stored in plaintext, masked at query time by role). Actual field-level encryption makes fields unsearchable — document this tradeoff explicitly.
4. **BYOK**: scope to cloud tiers via KMS integration. Local-tier BYOK deferred.
5. Remove claims about application-level encryption of local vault data unless the mmap architecture is also reconsidered.
