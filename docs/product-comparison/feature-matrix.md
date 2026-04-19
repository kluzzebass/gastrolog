# Feature matrix (primary log & search peers)

Log-platform capabilities beside Splunk, Graylog, Elastic, OpenSearch, Grafana + Loki, Datadog, and Sumo Logic. **Y** / **P** / **N** / **V** are shorthand, not scores—each **Feature** links to its explanation under [Notes](#notes).

## How to read cells

| Symbol | Meaning |
|--------|---------|
| **Y** | Commonly available in the core product experience for that column (still may need a specific edition or deployment—follow the feature link to the note). |
| **P** | Partial, add-on, cloud-vs-self-managed split, or “possible but not the default story.” |
| **N** | Absent, out of scope for the product, or not meaningful in the same way. |
| **V** | **Varies** a lot by edition, license tier, or operator setup—treat as “dig in peer docs before assuming.” |

**Gastrolog:** what ships **today** in this repo. **Peers:** deliberately high level—edition and topology change the story; the linked notes carry the caveats.

---

## Matrix

| Feature | Gastrolog | Splunk | Graylog | Elastic | OpenSearch | Grafana + Loki | Datadog | Sumo Logic |
|--------|:---------:|:------:|:-------:|:-------:|:----------:|:--------------:|:-------:|:----------:|
| [**Vendor-hosted SaaS**](#note-saas) | N | Y | P | Y | P | P | Y | Y |
| [**Self-managed / customer-operated**](#note-self-managed) | Y | Y | Y | Y | Y | Y | N | N |
| [**First-party edge agent / forwarder**](#note-edge-agent) | N | Y | P | Y | P | P | Y | Y |
| [**Central ingest without a vendor agent**](#note-central-ingest) | Y | Y | Y | Y | Y | Y | Y | Y |
| [**Ingest-time pipelines**](#note-ingest) | P | Y | Y | Y | Y | P | Y | Y |
| [**Ingester HA**](#note-ingester-ha) | Y | P | P | P | P | P | P | P |
| [**Full-text search**](#note-fulltext) | Y | Y | Y | Y | Y | P | Y | Y |
| [**Live tail / follow**](#note-livetail) | Y | Y | Y | Y | Y | Y | Y | Y |
| [**Structured / field-first filtering and analytics**](#note-structured) | Y | Y | Y | Y | Y | Y | Y | Y |
| [**Distinct query language**](#note-query-lang) | Y | Y | P | Y | Y | Y | P | Y |
| [**Cross-store or cross-index joins**](#note-joins) | Y | P | P | P | P | P | P | P |
| [**Built-in charts / dashboards from search**](#note-charts) | P | Y | Y | Y | Y | Y | Y | Y |
| [**Saved searches, views, or dashboard panels**](#note-saved) | P | Y | Y | Y | Y | Y | Y | Y |
| [**Logs ↔ traces / spans**](#note-logtrace) | P | Y | P | Y | P | Y | Y | P |
| [**Notebook, case, or investigation object**](#note-investigation) | N | Y | Y | P | P | P | Y | P |
| [**Alerting from log conditions**](#note-alerting) | N | Y | Y | Y | Y | Y | Y | Y |
| [**ML / anomaly / pattern on logs**](#note-ml) | N | Y | P | Y | P | P | Y | Y |
| [**Fine-grained data-plane RBAC**](#note-rbac) | N | Y | Y | Y | Y | P | Y | Y |
| [**Many custom roles**](#note-roles) | N | Y | Y | Y | Y | Y | Y | Y |
| [**SSO**](#note-sso) | N | Y | V | V | Y | Y | Y | Y |
| [**Multi-tenant customer isolation**](#note-multitenant) | N | P | P | P | Y | Y | Y | P |
| [**Multi-node / distributed storage**](#note-cluster) | Y | Y | Y | Y | Y | Y | Y | Y |
| [**Central tier HA**](#note-central-ha) | Y | Y | Y | Y | Y | P | Y | Y |
| [**Tiered or archive storage**](#note-tiered) | Y | Y | Y | Y | Y | P | Y | Y |
| [**Audit trail**](#note-audit) | N | Y | Y | Y | P | P | Y | Y |
| [**PII masking or ingest redaction**](#note-masking) | N | Y | P | Y | Y | P | Y | Y |
| [**Multi-phase / subquery-style composition**](#note-subquery) | N | Y | P | P | P | P | P | Y |

---

## Maintenance

1. Add a **row** when a new capability becomes a recurring parity topic; add a matching **Notes** anchor and paragraph, and make the **Feature** cell a markdown link `[**…**](#note-…)` to that anchor.
2. Update **Gastrolog** cells when shipped behavior changes; keep prose here aligned with the product, not with aspirational write-ups elsewhere.
3. When a peer column is wrong for a specific edition, fix the cell and extend that row’s note—do not turn this matrix into vendor marketing.

Last reviewed: **2026-04-19**.

## Notes

<a id="notes"></a>

<a id="note-saas"></a>
**Vendor-hosted SaaS.** Vendor-hosted **SaaS log service** (logs as a managed offering). **Elastic** and **Splunk** each have a major vendor-hosted line *and* self-managed options—the cell is **Y** for “a SaaS log offering exists,” not “SaaS-only.” **Graylog**, **OpenSearch**, and **Grafana + Loki** are often self-run; **P** marks a real managed/SaaS path (for example Graylog Cloud, OpenSearch Service, Grafana Cloud) without claiming parity across every deployment. **Gastrolog:** **N**—no vendor-hosted service.

<a id="note-self-managed"></a>
**Self-managed / customer-operated.** **Datadog** and **Sumo Logic** are **N** here in the sense of “customer operates the log platform binary/cluster like Splunk Enterprise or self-managed Elastic”; they may offer installable agents or bridges, but the log backend is their SaaS. **Gastrolog:** **Y**—you run it.

<a id="note-edge-agent"></a>
**First-party edge agent / forwarder.** **Install on nodes to ship logs**—vendor-supplied forwarder/collector family. **Gastrolog:** **N** by product intent—no first-party host forwarder; operators use Vector, Fluent Bit, OTLP Collector, and similar at the edge. The product is built to **receive** centrally, not to replace those agents. **Graylog**, **OpenSearch**, and **Grafana + Loki** are **P** because sidecars, Data Prepper, Alloy/Promtail, or third-party agents are common and not always one mandatory “vendor agent” story. **Splunk**, **Elastic**, **Datadog**, and **Sumo** are **Y** where a documented first-party collector/forwarder family is a mainstream deployment path.

<a id="note-central-ingest"></a>
**Central ingest without a vendor agent.** **Central listeners**—syslog, HTTP/API, OTLP, and similar protocols accepted on the service without requiring that vendor’s edge agent to be the hop (agents may still exist elsewhere in the stack).

<a id="note-ingest"></a>
**Ingest-time pipelines.** **Parse, enrich, drop, sample, route before durable store**—ingest-time pipeline processors. Vendors routinely highlight **processors at ingest** (Datadog *pipelines & processors*, Graylog *pipelines* / data management, Elastic *ingest pipelines*, Splunk *props/transforms* and *Edge Processing*, OpenSearch ingest processors, Sumo *field extraction* / parsing). **Grafana + Loki:** processing is often an agent (for example Alloy) ahead of Loki—**P** for “in the same shipped log product box” depending on how you draw the boundary. **Gastrolog:** **P**—**transform stages on ingest** (parse, enrich, redact, sample in the route) are **not** shipped yet, but **filter-based routing** and **per-route distribution** (fan-out, round-robin, or failover between vault destinations) already cover part of the “shape where data lands before storage” story as a **stopgap** until full ingest pipelines exist.

<a id="note-ingester-ha"></a>
**Ingester HA.** **Ingestion path**—keeping ingestion available across the cluster (contrast [central tier HA](#note-central-ha) for the serving tier). **Gastrolog:** **Y**—the normal pattern for **passive** (listener) ingesters is **several nodes, same listener on each**, with **load-balanced** inbound traffic (VIP, DNS, Kubernetes Service, anycast, etc.) so senders use one logical endpoint while the cluster absorbs the load in parallel—syslog, HTTP push, and similar protocols. **Singleton** placement is the **other** mode: exactly one active instance of that ingester in the cluster, moved to another node if its node fails. **Active** (pull) ingesters: scale across nodes when the source supports parallel or partitioned readers; singleton when only one consumer should run. **Peers:** **P**—similar outcomes usually mix load balancers, per-host agents, Kubernetes, or vendor-specific topology rather than this exact packaged combination.

<a id="note-fulltext"></a>
**Full-text search.** **Raw log message body**—full-text search on line contents, not only on labels/parsed fields. **Grafana + Loki:** **P**—query model is label-scoped; line-level “grep the heap” exists but is not the same operational profile as inverted-index full-text search in Elasticsearch-class backends.

<a id="note-livetail"></a>
**Live tail / follow.** **Real-time stream of new events**—live tail / follow in UI or CLI. Marketed under names like *Live Tail* (Datadog, Sumo), live streaming in Elastic Logs, Splunk search tail patterns. **Gastrolog:** **Y**—`follow` and streaming query paths.

<a id="note-structured"></a>
**Structured / field-first filtering and analytics.** Filter and aggregate on structured fields and attributes as the default analysis path (versus grep-only workflows).

<a id="note-query-lang"></a>
**Distinct query language.** **Named or de-facto languages**—SPL, Lucene/DSL, LogQL, KQL-adjacent pipelines, vendor query bars, etc. **Graylog:** **P**—Lucene-style search plus Graylog features; not one big branded pipeline language like SPL. **Datadog:** **P**—rich log query UX and facets rather than a single named SPL/LogQL-class language as the mental model for most users.

<a id="note-joins"></a>
**Cross-store or cross-index joins.** **One interactive query** spanning multiple stores or indices—not necessarily SQL-style relational joins. **Gastrolog:** **Y** with an important caveat—this row is about **one query spanning multiple logical stores**. Gastrolog searches **across vaults by default** (fan-out, merged results), so you do not need a separate “join these indices” workflow the way index-siloed stacks often do. That is **not** the same as arbitrary relational **JOIN** semantics between unrelated subqueries inside the language; peers’ **P** is about that heavier pattern being awkward everywhere. **Peers:** **P**—cross-index / cross-store joins in one ad-hoc UI query are usually limited, vendor-specific, or “export and join elsewhere.” Treat as “verify for your edition and topology,” not “fully relational.”

<a id="note-charts"></a>
**Built-in charts / dashboards from search.** **From search**—charts driven from query results. **Gastrolog:** **P**—queries can drive **inline visualizations** (for example `| linechart`, `| barchart` in the pipeline), but there is **no dashboard product** in the usual sense: no multi-panel canvas, no first-class dashboard object composed of several saved queries, no drag-and-drop layout. **Peers:** **Y** where the platform sells dashboarding as a core surface (Kibana, Grafana, Splunk Dashboard Studio, etc.).

<a id="note-saved"></a>
**Saved searches, views, or dashboard panels.** **From query work**—saved searches, Explorer-style views, dashboard panels tied to queries. Peers emphasize saved searches, Explorer views, and dashboard panels tied to queries (Elastic Logs Explorer; Datadog saved views; Splunk dashboards; OpenSearch Dashboards; Grafana). **Gastrolog:** **P**—saved queries exist as name + expression only; richer saved-dashboard workflows are not shipped.

<a id="note-logtrace"></a>
**Logs ↔ traces / spans.** **Same investigation UX**—jump between logs and traces/spans in one workflow. Datadog and Elastic openly market **logs correlated with traces**; Grafana stacks **Loki + Tempo** toward the same. Splunk and others vary by SKU. **Gastrolog:** **P**—OTLP span fields and log correlation exist in places, but automatic waterfall views and span-centric query operators (for example `children`, `critical_path`) are **not** shipped.

<a id="note-investigation"></a>
**Notebook, case, or investigation object.** **Shared incident context**—notebook, case, or investigation object beyond a single saved query. Graylog lists **Investigations**; Splunk and Datadog push **case-like** investigation workflows in broader platform messaging. **Elastic / OpenSearch / Grafana:** **P**—depends on Observability/Security apps or external tools. **Gastrolog:** **N**—shared investigation / case objects and handoff workflows are **not** shipped.

<a id="note-alerting"></a>
**Alerting from log conditions.** This row is **user- or operator-defined monitoring on log data** (saved search / scheduled query / “when this log pattern crosses a threshold, notify”)—what Splunk alerts, Datadog log monitors, Graylog event definitions, OpenSearch Alerting, Grafana alert rules on Loki queries, etc., sell. **Gastrolog:** **N**—there is **no** productized “define a query + threshold + destination” log alerting. What exists today is **internal system alerting** (for example rotation/retention throughput, channel pressure, ingest drops) surfaced in the UI for cluster operators—not the same capability as this row. If log-condition alerting ships later, change this cell (and optionally split a separate row for **platform health alerts**).

<a id="note-ml"></a>
**ML / anomaly / pattern on logs.** Edition and packaging vary widely for peers (**P** where ML is add-on, security SKU, or metrics-first). **Gastrolog:** **N** relative to shipping Watchdog / MLTK / Elastic ML–style product surfaces on logs.

<a id="note-rbac"></a>
**Fine-grained data-plane RBAC.** **Index / stream / row / field / restriction-query style**—fine-grained data-plane RBAC. **Gastrolog:** **N**—JWT with coarse `admin` / `user`; no per-vault/stream restriction queries or field-level document security comparable to Splunk role filters, Datadog restriction queries, or Elasticsearch DLS/FLS. **Grafana + Loki:** **P**—orgs/folders/datasource permissions are strong for the *UI*; row-level log restriction is weaker than index/DLS patterns in Elastic.

<a id="note-roles"></a>
**Many custom roles.** **Beyond a fixed small role set**—many named/custom roles. **Gastrolog:** small built-in set (`admin` / `user`); not a large custom RBAC matrix.

<a id="note-sso"></a>
**SSO.** **SAML / OIDC as first-class product integration** for human login. **Graylog** and **Elastic** are **V** because SAML/OIDC is often subscription- or edition-gated. **Gastrolog:** **N**—local accounts or `--no-auth`; no SAML/OIDC IdP integration in product.

<a id="note-multitenant"></a>
**Multi-tenant customer isolation.** **One control plane, multiple customers**—tenant-style isolation. **P** for several peers reflects “tenancy by convention” (indexes, streams, roles, spaces) more than one first-class `tenant` object for every deployment. **OpenSearch** **Y** reflects Dashboards/security-tenant patterns where operators use them. **Gastrolog:** **N**—single deployment, no customer-style tenant boundary in the control plane; first-class multi-tenant isolation is **not** shipped.

<a id="note-cluster"></a>
**Multi-node / distributed storage.** **Under one product**—multi-node or distributed storage as part of the same platform story. **Y** for every column only means “a scaled or distributed deployment is a normal part of the story,” not a ranking. **Gastrolog** is included on the same basis as peers—not as a special case.

<a id="note-central-ha"></a>
**Central tier HA.** **API, query, and config resilience across nodes**—no user-facing requirement to attach to a single “query primary.” Pair with [ingester HA](#note-ingester-ha) for the ingestion side. **Gastrolog:** **Y**—design target is that routine **client** use does not depend on one special “query primary”; replicated cluster config, cross-node query fan-out, ingest forwarding to owning nodes, and tier leadership/replication cover the path. **Peers:** **Y** for mature enterprise or SaaS stacks where clustered search/indexers and HA control planes are the norm. **Grafana + Loki:** **P**—HA is standard in real deployments, but what counts as “the product” is often several services and charts you wire yourself.

<a id="note-tiered"></a>
**Tiered or archive storage.** **Hot vs warm vs cheap / archive paths**—tiered or archive storage. **Grafana + Loki:** **P**—object storage and retention exist, but the bundled “ILM vs frozen tier” story differs from Elasticsearch-class index lifecycle. Other columns: **Y** at a high level (hot/warm/cold or archive paths exist in mainstream docs); details are edition-specific. Graylog and Splunk also market hot/warm/cold or indexing-tier stories in their own terms.

<a id="note-audit"></a>
**Audit trail.** **Queries, exports, admin actions**—compliance-oriented audit trail. **Gastrolog:** **N**—not shipped. Peers commonly ship admin/query audit logs in enterprise offerings; **OpenSearch** / **Grafana** cells are **P** because audit features often depend on Security plugin / Grafana Enterprise / deployment choices.

<a id="note-masking"></a>
**PII masking or ingest redaction.** **Product surface**—query-time field masking and/or ingest-time redaction as supported workflows. **Gastrolog:** **N**—role-based query-time masking and ingest-time `redact` stages are **not** shipped. **Graylog** / **Grafana** **P**—capability exists in parts of the stack or editions; verify for your license.

<a id="note-subquery"></a>
**Multi-phase / subquery-style composition.** **Examples:** `subsearch`, `let`, lookup-from-subquery—multi-phase or subquery-style composition. **Splunk** *subsearch* and **Sumo** *subqueries* are long-standing advertised patterns. **Gastrolog:** **N**—`let` / named intermediate query results are **not** shipped. Other columns **P** where ES\|QL, PPL, LogQL, or nested queries cover *some* patterns but not Splunk-style subsearches uniformly.
