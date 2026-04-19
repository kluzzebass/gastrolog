# Product comparison overview

This directory holds **peer product** research: what other systems do so we can judge what Gastrolog should implement for parity, not how to integrate with them.

**Edge → central ingest:** Gastrolog does **not** implement its own log forwarder or agent. Operators are expected to run established **edge agents**—**Vector** and **Fluent Bit** are common choices—to collect, buffer, and ship logs into a **central** Gastrolog cluster. That division is intentional: those tools own node-level concerns (inputs, resource use, disk buffering, retries); Gastrolog owns storage, query, and cluster behavior. See [Edge log forwarding (complementary)](#edge-log-forwarding-complementary) below.

Use this page to pick **which peers matter** for a given feature area (auth, query language, retention, clustering, anomaly detection, and so on). Deeper dives live in topic-specific notes under the same directory—for example [peer authentication, authorization & multi-tenancy](./peer-authentication-and-authorization.md) and [anomaly detection research](./anomaly-detection-research.md).

**Query languages:** Gastrolog’s own query surface is partly inspired by **Azure Monitor’s KQL** (Log Analytics). For parity research on *how* users expect to filter, project, join, and aggregate logs—not only on storage engines—include the hyperscaler row set below alongside Elastic/OpenSearch (DSL), Splunk (SPL), Grafana (LogQL), and SaaS log explorers.

## How this overview is organized (categories)

Peers are grouped by **what you are benchmarking**, not by how vendors market themselves. The same company can appear in more than one mental bucket (for example **Datadog** for logs RBAC *and* for Watchdog-style anomaly UX).

| Category | Use it when researching… | Where it lives in this doc |
|----------|----------------------------|----------------------------|
| **Log & search platforms** | Core log ingestion, query languages, RBAC, multi-tenancy, retention, cluster operations | [Log & search platforms](#category-log-search) |
| **Hyperscaler log & query** | KQL vs CloudWatch Logs Insights vs BigQuery-backed log workflows | [Hyperscaler table](#category-hyperscaler-log) |
| **Anomaly detection & AIOps** | Metric baselines, outliers, forecast breaches, alert noise, “problem inbox” / correlation UX, log-pattern clustering vs metrics-first ML | [Anomaly & AIOps peers](#category-anomaly-aiops); full survey in [anomaly detection research](./anomaly-detection-research.md) (also covers **Splunk**, **Elastic**, **Datadog**, **Grafana**, **Graylog**, **Sumo Logic** from that angle) |
| **Edge & complementary ingest** | Vector / Fluent Bit / OTLP collectors as the hop before Gastrolog | [Edge log forwarding](#edge-log-forwarding-complementary) |
| **Adjacent systems** | Prometheus, warehouses, CI log viewers—overlap without being log-product peers | [Adjacent table](#category-adjacent) |

<a id="category-log-search"></a>

## Log & search platforms (primary peers)

| Peer | One-line positioning | Distinguishing features (why we compare) |
|------|----------------------|------------------------------------------|
| **Splunk** | Enterprise log search and analytics (on‑prem **Enterprise** and **Splunk Cloud**). | **SPL** and rich app ecosystem; access control is **layered** (role capabilities, per-role index/search filters in `authorize.conf`, knowledge-object ACLs). Tenancy is often **by convention** (indexes, apps, roles) rather than a single “tenant” object. |
| **Graylog** | Open-core log platform with optional enterprise features. | **Streams** and **index sets** as the main data model; permissions are **roles plus explicit sharing** on entities (streams, dashboards, alerts). Strong **LDAP/AD** in open tier; **OIDC/SAML** in Enterprise. |
| **Elastic** | **Elasticsearch** (storage + search) plus **Kibana** (UI and operational tools). | **Index-level** privileges plus **document- and field-level security**; **Kibana Spaces** for workspace separation; many **auth providers** and subscription-gated SSO/token/PKI patterns. De facto standard for **Query DSL** and large-scale search. |
| **OpenSearch** | Elasticsearch lineage with AWS and community backing; **OpenSearch Dashboards** + Security plugin. | First-class **Dashboards multi-tenancy** (tenants, `securitytenant` header); **Security plugin** RBAC with cluster, index, DLS/FLS-style options. Relevant when benchmarking **self-managed** or **OpenSearch Service**–style deployments. |
| **Grafana + Loki** | **Loki** (log aggregation) is usually paired with **Grafana** for query and RBAC. | **Organizations** and **folder** permissions as the main isolation story for the UI; **Prometheus-style** metrics and **LogQL** for logs. Good reference for **“minimal log store + rich control plane UI”** expectations. **Grafana Cloud Machine Learning** (outlier/forecast on metrics) is a separate surface from Loki; see [anomaly detection research](./anomaly-detection-research.md). |
| **Datadog** | SaaS observability (logs, metrics, APM, security, etc.). | **Organization** model (including **multi-org** for MSP-style separation); **logs RBAC** with **restriction queries** and index-scoped permissions. Strong reference for **SaaS-only** UX and **billing/usage** per org. |
| **Sumo Logic** | Cloud log analytics and SIEM-style workflows. | **RBAC roles** combine **capabilities** with **role search filters** that narrow visible log data; multi-account and SAML behaviors have sharp edges (documented caveats around keys and multi-account). |

<a id="category-hyperscaler-log"></a>

## Hyperscaler log & query peers (Azure, AWS, Google Cloud)

These are not single “Splunk-shaped” products; each cloud splits **ingestion, storage, query UI, and billing** across services. They still matter for **log querying semantics**, retention, routing, and operator UX.

| Peer | Primary log / query surfaces | Why compare (especially for querying) |
|------|------------------------------|----------------------------------------|
| **Microsoft Azure** | **Azure Monitor**; logs in **Log Analytics workspaces** queried with **KQL** (same family as **Azure Data Explorer / ADX**). | **KQL** is tabular and pipeline-oriented (`table \| where \| summarize`); strong reference for **structured log analytics**, cross-table joins, and operator ergonomics. Gastrolog’s query language already draws from this family. |
| **Amazon Web Services** | **CloudWatch Logs** (log groups / streams); **CloudWatch Logs Insights** for interactive queries; **Amazon OpenSearch Service** for search-style APIs and Dashboards. | **Logs Insights** uses its own query language (not KQL); **OpenSearch Service** aligns with **Query DSL / Lucene**-style search. Good for contrasting **“metrics-ish log analytics” vs full-text search** in one ecosystem. |
| **Google Cloud** | **Cloud Logging** (sinks, buckets, retention); export to **BigQuery**; **Log Analytics** features tied to logging + SQL. | Mix of **logging filters** in the ops suite and **SQL** over log rows in BigQuery—useful when benchmarking **federated or warehouse-backed** log investigation vs in-stream query only. |

<a id="category-anomaly-aiops"></a>

## Anomaly detection & AIOps peers

These vendors are called out in [anomaly detection research](./anomaly-detection-research.md) **Part I** alongside the log platforms above. They skew **metrics-first, incident-correlation, and ML-on-signals** rather than “replace your log index.” Use them when benchmarking **how anomalies are detected, tuned, explained, and consumed in the UI**—especially when Gastrolog touches alerting, baselines, or post-hoc explanation over high-cardinality logs.

| Peer | One-line positioning | Distinguishing features (why we compare) |
|------|----------------------|------------------------------------------|
| **New Relic** | SaaS observability with **NRQL** and **Applied Intelligence** / AIOps. | Golden-signal and NRQL **baseline** alerts, “ensemble” anomaly scoring, **issue maps** for correlated anomalies—reference for **sensitivity-as-stddev** UX and incident inbox patterns. |
| **Dynatrace** | Deep **APM** and full-stack monitoring with **Davis AI**. | **Auto-adaptive** and **seasonal** baselines, multi-dimensional baselining, **Problems** as first-class objects with topology (**Smartscape**)—strong reference for **root-cause grouping** and streaming evaluation limits. |
| **Honeycomb** | Event- and trace-oriented observability on a **wide-cardinality** model. | **BubbleUp** for attribute-distribution explanations on a user-selected cohort; **Anomaly Detection** (product evolution—see survey dates in doc) for early-warning on golden signals—reference when Gastrolog wants **detect-then-explain** on raw events without pre-bucketing everything. |

**Overlap:** **Splunk** (ITSI / MLTK), **Elastic** (Elastic ML), **Datadog** (Watchdog, anomaly monitors), **Grafana Cloud ML**, **Graylog** (security / UEBA-style anomaly), and **Sumo Logic** (LogReduce, LogCompare, outlier operators) are analyzed in the **same** anomaly survey—this table only lists vendors **not** already named in the [log & search platforms](#category-log-search) table.

## Edge log forwarding (complementary)

Gastrolog is built to **receive** and **serve** logs from a central tier, not to compete with **Vector**, **Fluent Bit**, **Fluentd**, the **OpenTelemetry Collector**, or vendor-specific agents on each host. Deploy those at the **edge** (nodes, k8s DaemonSets, sidecars) to forward into Gastrolog—this is the main reason there is **no first-party Gastrolog forwarder**.

When researching “parity,” the relevant questions for these tools are usually **interoperability**, not feature replacement: wire protocols and auth, batching and backpressure, field naming and metadata (e.g. resource attributes), and operational runbooks—not whether Gastrolog re-implements their pipelines.

<a id="category-adjacent"></a>

## Adjacent systems (partial overlap, not primary “log platform” peers)

Many products overlap **one slice** of what a log platform does—query UX, time ranges, labels, alerting, pipelines—without being a head-to-head competitor to Gastrolog as a **log search product**. They are still fair references when researching a specific capability.

| System / category | Overlap with Gastrolog | Why it is *not* usually a direct peer |
|-------------------|-------------------------|--------------------------------------|
| **Prometheus** (+ **Alertmanager**, **recording rules**) | Time-series model, **labels**, **PromQL**, range selectors, alert routing, “high cardinality” discipline. | Optimized for **metrics**, not durable log text search; different retention and query semantics than log lines. **Grafana** already bridges metrics + logs in many stacks. |
| **Long-term Prometheus** (**Thanos**, **Mimir**, **VictoriaMetrics**, **Cortex**) | Federation, downsampling, multi-tenant blocks, cost-aware retention. | Storage and operational concerns for **metrics blocks**, not log indexing. |
| **Distributed tracing** (**Jaeger**, **Tempo**, **Zipkin**, **X-Ray**, **Cloud Trace**) | Investigating incidents across services; trace IDs correlating to logs. | Data model is **spans and traces**, not log events; overlap is **workflow** (jump from trace to log), not log storage. |
| **Stream / queue platforms** (**Apache Kafka**, **Pulsar**, **NATS**, cloud queue services) | Ingest volume, partitioning, replay, ordering—how logs enter the system. | **Transport**, not query UI or retention policy for human search. |
| **Agents / collectors** (**Vector**, **Fluent Bit**, **Fluentd**, **OpenTelemetry Collector**, vendor agents) | Edge parsing, sampling, routing, and delivery into Gastrolog. | **Complementary by design**—Gastrolog does not ship a forwarder; use agents at the edge. See [Edge log forwarding (complementary)](#edge-log-forwarding-complementary). Compare for **ingest contracts and ops**, not as a product Gastrolog must duplicate. |
| **Data warehouses & lakes** (**Snowflake**, **BigQuery**, **Databricks**, **Iceberg** / **Delta** on object storage) | SQL over huge history, cost controls, partition pruning, “investigate in the warehouse.” | **Batch / OLAP** interaction model and governance tools—not live tail and ad-hoc log drill unless you deliberately build that. |
| **Columnar OLAP** (**ClickHouse**) | Fast aggregations and log-like tables at scale; often used as a log backend. | Frequently embedded as **storage engine** behind another product rather than the full “operator log product” surface. |
| **Notebook / data science** (**Jupyter**, **Hex**, etc.) | Exploratory queries, saved analysis, sharing results. | General analytics shell, not log RBAC / multi-tenant log product semantics. |
| **Host / endpoint state** (**osquery**) | Structured “facts about machines” queried like data. | **State snapshots**, not continuous log streams (though both appear in investigations). |
| **CI / build systems** (**GitHub Actions**, **GitLab CI**, **Buildkite**, etc.) | Log viewer UX for **job output**, step grouping, artifacts. | Scoped to **pipeline runs**, not organization-wide log retention and search. |

## How to use this for research

1. **Choose peers** from the categories above based on the feature (e.g. query language shape → **Azure (KQL)** plus one search-native stack **Elastic / OpenSearch**; multi-tenant SaaS → Datadog / Sumo; stream-centric logging → Graylog; AWS-native ops → CloudWatch Logs Insights **and** OpenSearch Service where relevant). For **labels, PromQL-style range queries, or alert rule ergonomics**, add **Prometheus + Grafana** even when Gastrolog is not “a metrics server.” For **anomaly detection, baselines, and explain-after-detect UX**, pull from **[Anomaly & AIOps peers](#category-anomaly-aiops)** and the full [anomaly detection research](./anomaly-detection-research.md) survey (including Elastic, Datadog, Splunk, Grafana ML, Graylog, Sumo).
2. **Add or refresh** a focused note under `docs/product-comparison/` (one topic per file is easier to maintain than one giant doc).
3. **Link new notes** from this overview when a peer set becomes a recurring reference.

## Contents of this directory

| Document | Purpose |
|----------|---------|
| `overview.md` | This page: which peers exist and when to use them. |
| [feature-matrix.md](./feature-matrix.md) | Running **Y / P / N / V** matrix: primary log & search peers vs Gastrolog on core platform capabilities (kept honest; extend rows over time). |
| [peer-authentication-and-authorization.md](./peer-authentication-and-authorization.md) | AuthN, AuthZ granularity, and multi-tenancy patterns across the peer set. |
| [anomaly-detection-research.md](./anomaly-detection-research.md) | **Part I:** Splunk, Elastic, Datadog, Grafana Cloud ML, New Relic, Sumo Logic, Graylog, Dynatrace, Honeycomb. **Part II:** algorithms and evaluation. Gastrolog anomaly / alerting design. |
