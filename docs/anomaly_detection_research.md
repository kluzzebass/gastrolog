# Anomaly Detection — Inspiration & Algorithm Survey

Internal reference for designing anomaly detection in GastroLog.
Draft / working document.

Compiled from two parallel sub-agent research passes:

1. **How comparable systems do it** — Splunk, Elastic, Datadog, Grafana, New Relic, Sumo Logic, Graylog, Dynatrace, Honeycomb. What we can learn from their approaches.
2. **Algorithm reference** — statistical, forecasting, unsupervised ML, log-specific, streaming, evaluation.

---

## Part I — How Comparable Systems Do Anomaly Detection

### Splunk (ITSI + MLTK + Anomaly Detection App)

**Model of "anomaly":** three overlapping concepts — KPI metric deviation (ITSI), cohesion/entity drift, and density-based outliers in raw events (MLTK). The ITSI-specific "anomaly detection" feature was deprecated in 4.20, with adaptive thresholding and a separate outlier scenario taking its place.

**Algorithms:**
- ITSI shipped Adaptive Thresholding (stddev/quantile/range bands recomputed nightly), Trending (nonparametric rolling-window scoring), Cohesive (group-divergence).
- MLTK flagship: `DensityFunction` — fits Normal/Exponential/`gaussian_kde`/Beta distributions per group, flags tail points (default threshold 0.01).
- Splunk App for Anomaly Detection (EOL June 2025) exposed one "sensitivity" slider mapping to a score threshold.

**Input shape:** pre-aggregated KPI series (1-min buckets); batch model fits, not streaming.

**UI:** severity-coded KPI tiles, `is_outlier` column on SPL results. No per-log-line badges.

**Tuning surface:** high — 5–7 severity levels, adaptive/static thresholds, seasonality (weekday/weekend/hour-of-day), per-entity overrides.

**Caveats:** the deprecation of the core anomaly feature itself is telling; a historical pain point was the heavy expert tuning required.

Sources: [ITSI migration](https://help.splunk.com/en/splunk-it-service-intelligence/splunk-it-service-intelligence/visualize-and-assess-service-health/4.21/advanced-thresholding/migrate-anomaly-detection-to-adaptive-thresholding-in-itsi), [MLTK DensityFunction](https://www.splunk.com/en_us/blog/platform/building-machine-learning-models-with-densityfunction.html), [Building the Anomaly Detection service](https://medium.com/splunk-engineering/building-an-anomaly-detection-service-for-splunk-cloud-platform-858f2437270b), [App EOL](https://www.splunk.com/en_us/blog/platform/developing-the-splunk-app-for-anomaly-detection.html).

### Elastic Stack (Elastic ML / anomaly-detection jobs)

**Model:** unusual metric values in a bucket (count/mean/sum), unusual category appearing, rare events, anomalous entities within a population.

**Algorithms:** docs say "clustering, time-series decomposition, Bayesian distribution modeling, correlation." The `ml-cpp` C++ engine is the actual implementation. Log categorization uses a Drain-style token-clustering pass followed by per-category rate anomaly detection. Population analysis profiles "typical" peer behavior and flags divergence.

**Input shape:** datafeed from Elasticsearch indices; bucketed (15m/1h/1d typical); near-real-time but always bucket-aligned.

**UI:** Anomaly Explorer + Single Metric Viewer in Kibana with severity-scored rows (0–100), per-entity swim lanes, chart annotations. Integrates with Kibana alerting.

**Tuning surface:** extensive — job type, detector function (count, rare, freq_rare, distinct_count, mean, min, max, info_content), bucket_span, by/over/partition, influencers, model memory limit, calendars.

**Caveats:** bucket-span changes require job recreation; rare detector needs ~20 bucket-span warm-up; high false-positive rates without tuning; categorization tokenizer struggles with rare event types — exactly where the highest signal lives.

Sources: [algorithms](https://www.elastic.co/docs/explore-analyze/machine-learning/anomaly-detection/ml-ad-algorithms), [job types](https://www.elastic.co/docs/explore-analyze/machine-learning/anomaly-detection/ml-anomaly-detection-job-types), [limitations](https://www.elastic.co/guide/en/machine-learning/current/ml-limitations.html), [bucket span](https://www.elastic.co/blog/explaining-the-bucket-span-in-machine-learning-for-elasticsearch), [false-positive discussion](https://discuss.elastic.co/t/anomaly-jobs-general-strategies-to-reduce-false-positives/348094).

### Datadog (Watchdog, Anomaly / Outlier / Forecast Monitors)

**Model:** four surfaces — single-series anomaly, peer outlier, forecast breach (disk-full prediction), Watchdog zero-config auto-detection across APM/infra.

**Algorithms:** three exposed anomaly algorithms — `basic` (lagging rolling quantile, no seasonality), `agile` (seasonal decomposition, adapts fast to level shifts), `robust` (seasonal decomposition, stable predictions robust to long outages). Outliers use DBSCAN or MAD with Scaled variants normalizing by magnitude.

**Input shape:** pre-aggregated metrics, streaming evaluation. Seasonality: hourly, daily, weekly.

**UI:** gray "normal band" overlaid on metric chart; Watchdog has a dedicated inbox of auto-generated stories. No per-log-line badges; metric layer.

**Tuning surface:** minimal — `bounds` (stddev multiplier, recommend 2–3), directionality, seasonality, alert/recovery windows. Watchdog has essentially no config.

**Caveats:** `basic` has no seasonality; `agile` overreacts to level shifts; `robust` is slow to adopt genuine new baselines. Deployments routinely produce 20+ false positives for ~2h. Weekly models need 2–3 weeks of training.

Sources: [anomaly monitor](https://docs.datadoghq.com/monitors/types/anomaly/), [outlier monitor](https://docs.datadoghq.com/monitors/types/outlier/), [algorithms](https://docs.datadoghq.com/dashboards/functions/algorithms/), [outlier algorithms blog](https://www.datadoghq.com/blog/outlier-detection-algorithms-at-datadog/), [Watchdog](https://www.datadoghq.com/blog/watchdog/).

### Grafana Cloud ML (Outlier Detection + Forecasting)

**Model:** (a) outlier — peer-group divergence (DBSCAN) or self-band divergence (MAD); (b) forecast breach — prediction interval violated.

**Algorithms:** DBSCAN (default, for series that move closely together) and MAD (for stable bands) for outlier detection. Forecasting algorithm is not named in official docs; Grafana blog posts indicate a Prophet-like decomposable additive model with daily/weekly/yearly seasonality and iCal holidays, but this is inferred rather than officially documented.

**Input shape:** Prometheus-style metric time series; pre-aggregated. Outlier emits `<metric>:outliers` as 0/1 per timestamp per series. Strictly metrics — Loki/logs are not a first-class input.

**UI:** queryable metric consumed via Grafana alerting or dashboard overlay. No dedicated "anomaly explorer."

**Tuning surface:** algorithm choice, sensitivity percentile, interval, training window. Forecasting exposes seasonality switches, holidays, logistic-growth caps, prediction interval width — consistent with a Prophet-like backend.

**Caveats:** metrics-only; DBSCAN needs peer groups ≥3 members; MAD misbehaves in very stable bands. Forecast models need multi-week training windows; additive-decomposition forecasters generally struggle with non-stationary data.

Sources: [outlier](https://grafana.com/docs/grafana-cloud/machine-learning/dynamic-alerting/outlier-detection/), [forecasting](https://grafana.com/docs/grafana-cloud/machine-learning/dynamic-alerting/forecasting/), [Prometheus anomaly at scale](https://grafana.com/blog/2024/10/03/how-to-use-prometheus-to-efficiently-detect-anomalies-at-scale/).

### New Relic (Applied Intelligence / AIOps; NRQL Baseline Alerts)

**Model:** golden-signal deviations per entity (throughput/response time/error rate), NRQL baseline breaches (≥N stddevs from prediction), ingest-volume anomalies.

**Algorithms:** "ensemble of algorithms" automatically selected. Prediction uses 1–4 weeks of data with recency weighting; stddev gap between prediction and observation tracked over trailing 7 days. Seasonality: hourly, daily, weekly, auto.

**Input shape:** metric-oriented (NRQL result streams at minute granularity); streaming evaluation.

**UI:** AIOps incident inbox, issue-map visualization linking correlated anomalies, dynamic baseline ribbon on NRQL chart, red/yellow annotations on entity pages.

**Tuning surface:** sensitivity (stddev count 1–1000), direction, seasonality, aggregation window, sliding-window function, violation time threshold.

**Caveats:** no monthly/yearly seasonality; Monday-morning false-positive bursts typical; expanded anomaly detection in preview doesn't emit notifications directly — users must wire a NRQL condition on `NrAiAnomaly`.

Sources: [anomaly detection](https://docs.newrelic.com/docs/alerts/create-alert/set-thresholds/anomaly-detection/), [NRQL baseline GA](https://newrelic.com/blog/news/nrql-baseline-alerts-ga), [Applied Intelligence](https://newrelic.com/platform/applied-intelligence).

### Sumo Logic (LogReduce, LogCompare, Outlier Operator, Metric Outliers)

**Model:** (1) unusual new log pattern or volume-shifted pattern (LogReduce + LogCompare); (2) numeric outlier in a streaming query (`outlier`); (3) metric deviation (Metric Outliers).

**Algorithms:** LogReduce — fuzzy-logic string clustering into "signatures" by structural/repeated-token similarity; relevance ranking. LogCompare — LogReduce baseline vs target window, then symmetric KL-divergence to rank most-shifted signatures. Outlier operator — rolling expected value with stddev threshold. Metric anomalies use up to 30 days of history.

**Input shape:** LogReduce/LogCompare: raw events in an interactive search window (batch). Outlier operator: streams over timechart results. Metric outliers: pre-aggregated metric streams.

**UI:** dedicated LogReduce tab with expandable signatures and counts; LogCompare table sorted by change score with per-signature baseline vs target counts; metric charts with bands and shaded outlier regions.

**Tuning surface:** LogReduce — manual signature edits (merge/split/promote/demote), relevance thresholds, keyword filters. Outlier operator — window, threshold (stddev), direction.

**Caveats:** LogReduce relevance is a heuristic, not a statistical model; signatures still need human triage. LogCompare needs well-chosen baseline/target windows; misleading if baseline contained incidents. Underlying clustering isn't documented in detail.

Sources: [LogReduce](https://www.sumologic.com/help/docs/search/behavior-insights/logreduce/), [LogCompare](https://www.sumologic.com/help/docs/search/behavior-insights/logcompare/), [LogCompare KL-divergence](https://support.sumologic.com/hc/en-us/articles/115012482888-LogCompare-Detecting-Patterns-and-Changes-Across-Environments-and-Time).

### Graylog

**Model:** security/UEBA-flavored — point anomalies (one unusual login), contextual (admin login at 3am from unknown IP), collective (multi-stage attack patterns).

**Algorithms:** "machine learning" with 7 days of historical self-training. Specific family not publicly documented — marketing-level material only.

**Input shape:** Illuminate pipeline normalizes and enriches logs; time-sliced and historical-range compared.

**UI:** dedicated detectors and anomaly dashboards; alerts integrated with Graylog's alerting; SIEM/UEBA workflows rather than generic SRE.

**Tuning surface:** detector type selection, enablement per category; described as "no prior ML experience required."

**Caveats:** limited public technical detail; security-focused, not generic observability.

Sources: [anomaly detection feature](https://graylog.org/feature/anomaly-detection/), [docs](https://go2docs.graylog.org/current/what_more_can_graylog_do_for_me/anomaly_detection.html).

### Dynatrace (Davis AI)

**Model:** service response-time/error-rate/traffic deviations, infrastructure saturation, custom-metric bands, log-derived metric anomalies.

**Algorithms:** three baseline strategies — Auto-adaptive threshold (7-day rolling ML band), Seasonal baseline (learns daily/weekly over a full week before alerting), Static threshold. Multi-dimensional baselining splits by geography/browser/OS/connection/user-action so each combination has its own band. Davis correlates via Smartscape topology graph to collapse related anomalies into a single "problem."

**Input shape:** metrics + derived signals from OneAgent + Grail; streaming real-time evaluation.

**UI:** "Problems" — first-class incident objects with root-cause ranking, affected entities, timeline, log excerpts.

**Tuning surface:** per-service sensitivity (low/med/high); missing-data alerts togglable per stream; static-threshold overrides.

**Caveats:** missing-data alerts must be disabled for sparse streams or they alert-storm. Platform limits: 100k Davis events/hour, 60-day event lifetime, 90-minute problem-merge window. 7-day minimum learning period means week-one alerts are unreliable.

Sources: [anomaly detection](https://docs.dynatrace.com/docs/discover-dynatrace/platform/davis-ai/anomaly-detection), [multi-dimensional baselining](https://docs.dynatrace.com/docs/platform/davis-ai/anomaly-detection/concepts/automated-multidimensional-baselining), [limits](https://docs.dynatrace.com/docs/dynatrace-intelligence/reference/davis-ai-limits).

### Honeycomb (BubbleUp + Anomaly Detection alpha, 2025)

**Model:** two distinct things. BubbleUp is explanatory — given a user-selected heatmap region, find the dimensions whose distribution differs most between selected and baseline. It characterizes, doesn't detect. Anomaly Detection (Sept 2025 alpha) is a separate early-warning detector for error-rate/latency/requests.

**Algorithms:** BubbleUp — for each dimension, compute per-value distribution inside-box vs outside-box, rank by percent-difference, render side-by-side histograms. Historically described as descriptive statistics over the columnar Retriever store; Honeycomb's current platform page characterizes it as applying machine learning, so the internal mechanics may have evolved. Anomaly Detection algorithm details not yet published.

**Input shape:** raw events/spans with arbitrary high-cardinality fields; BubbleUp runs over the selected time window at query time. No pre-aggregation required.

**UI:** BubbleUp panel alongside heatmaps/bar charts showing ranked attribute differences. Anomaly Detection surfaces in a service-health view with "early warning" cards.

**Tuning surface:** BubbleUp is zero-config — drag a box. Anomaly Detection (alpha) documented as "learns normal behavior" with minimal knobs.

**Caveats:** BubbleUp is diagnostic, not detective — human has to notice something odd first. Anomaly Detection is alpha and covers only error rate / latency / requests at launch.

Sources: [BubbleUp docs](https://docs.honeycomb.io/investigate/analyze/identify-outliers/), [platform page](https://www.honeycomb.io/platform/bubbleup), [Anomaly Detection alpha](https://www.honeycomb.io/blog/introducing-anomaly-detection-early-warning-system-service-health).

### Patterns Across Systems

1. **Metrics-first, logs-second.** Every mature offering does anomaly detection on pre-aggregated time-series metrics. Log-pattern anomalies are a second-class subsystem (Elastic categorization, Sumo LogReduce, Honeycomb BubbleUp). Even when the input is logs, it gets counted into buckets first. **Implication for GastroLog:** a log platform that treats anomaly detection as first-class on *log patterns* (not derived metrics) is genuinely differentiated.

2. **Seasonal decomposition dominates.** STL/Prophet/Holt-Winters variants (trend + daily + weekly + residual, then band the residual) are the shared default. Datadog agile/robust, New Relic baseline, Grafana Prophet, Dynatrace seasonal — all fit this mold. 7-day minimum training is industry-standard.

3. **User-facing tuning collapses to one knob.** Sensitivity slider — Splunk (0–1), Dynatrace (low/med/high), Datadog bounds, New Relic sensitivity. Under the hood everything translates to a stddev multiplier on a decomposed-residual band. Users rarely pick algorithm family (exceptions: Datadog basic/agile/robust, Grafana DBSCAN/MAD).

4. **Detection is decoupled from explanation.** Every system has two layers — "here is an anomaly" and separately "here is why / what else correlates." Dynatrace Smartscape, New Relic Issue Maps, Datadog Watchdog Stories, Honeycomb BubbleUp, Elastic influencers. Nobody has solved one-shot detect-and-explain. **A GastroLog design that tightly integrates pattern-emergence detection with BubbleUp-style attribute-difference explanation over the same columnar store is a live opportunity — Honeycomb is the only one doing it and their detector is alpha.**

5. **Consolidation is the 2024–2026 trend.** Splunk's ITSI anomaly detection deprecated in favor of adaptive thresholding; App for Anomaly Detection EOL June 2025. Elastic and New Relic fold anomaly jobs into broader "AIOps" suites. Dynatrace/Datadog push zero-config umbrellas over per-metric jobs. Direction of travel: away from "job-per-detector with proliferating UI" toward "automatic coverage with a sensitivity slider and a problem inbox." **GastroLog should skip the first-generation UX entirely.**

**Three shared challenges** (pattern across all vendors):
- Deployments invalidate the model — every platform struggles ~2h post-deploy.
- Weekly seasonality needs 2–3 weeks of history; Monday-morning false positives are universal.
- Rare events — the highest-signal category — are where the tokenizer/clusterer is weakest. Elastic documents this directly.

These are the three failure modes worth designing around from day one.

---

## Part II — Algorithm Reference

### 1. Statistical Methods

Cheap, transparent, and the baseline every other method must beat.

| Method | Cost/event | Online? | Distribution | When to pick |
|---|---|---|---|---|
| Z-score | O(1) | yes | Gaussian | Gaussian-ish, stationary metrics — 80% case |
| MAD | O(log N) | yes (sketch) | none | Heavy-tailed metrics (latency, error counts) — default over z-score for logs |
| Grubbs | O(N) | no | Gaussian | Post-hoc single-outlier test, not continuous |
| Window threshold | O(1)–O(log N) | yes | none | SLOs and user-visible thresholds (what Prometheus alerting does) |
| EWMA/EWMS | O(1) | yes | Gaussian-ish | Per-series numeric at scale; O(1) streaming gold standard |
| CUSUM | O(1) | yes | shift detection | Small persistent mean shifts, baseline drift |

**Key refs:** Welford's online variance (Knuth AoCP Vol 2); Leys et al. 2013 on MAD; Roberts 1959 for EWMS; Page 1954 for CUSUM. Quantile sketches for streaming: [t-digest](https://github.com/tdunning/t-digest), [DDSketch](https://github.com/DataDog/sketches-go).

### 2. Time-Series Forecasting

Forecast the expected value; flag large residuals.

- **Holt-Winters (triple exponential):** O(1) per point, streaming. Stable daily/weekly seasonality. What Etsy's Skyline/Thyme and many internal tools start with.
- **STL decomposition + residual analysis:** batch, O(N) per pass. Separates trend/seasonality/residual cleanly; explainable. [Twitter's AnomalyDetection](https://github.com/twitter/AnomalyDetection) uses STL + Generalized ESD (repo archived November 2021 — read-only, unmaintained; reference only).
- **Prophet (Taylor & Letham 2017):** batch, seconds-to-minutes per series. Good for human-scale business metrics with calendar effects. Overkill for machine-generated log metrics. [Repo](https://github.com/facebook/prophet).
- **SARIMA:** rarely worth the model-selection overhead at log-platform scale.

**Reality check:** per-series forecasting falls apart beyond a few thousand series. For 50k services × 20 metrics you cannot afford per-series SARIMA. Options: (a) forecast top-N by traffic only, (b) cluster series and fit per cluster (`k-Shape`, Paparrizos & Gravano 2015), (c) skip forecasting for long tail and use EWMA + cross-series distributional detectors.

### 3. Distributional / Unsupervised ML

Treat events as points in feature space; find non-fits.

- **Isolation Forest (Liu/Ting/Zhou 2008):** linear O(N) train via sub-sampling (the paper's headline result), O(log N) score. Batch. First thing to try for multivariate metric anomalies. `sklearn.ensemble.IsolationForest`, [PyOD](https://github.com/yzhao062/pyod).
- **One-Class SVM (Schölkopf et al. 2001):** O(N²) to O(N³) training depending on kernel and solver; dominated by iForest and autoencoders in practice. Keep on the shelf.
- **LOF (Breunig 2000):** O(N²) naive. Useful when data has clusters of varying density. Rare for log metrics; more useful for host/user-behavior fingerprinting.
- **DBSCAN-based:** clusters first, anomalies are unclustered points. Streaming variant [DenStream](https://citeseerx.ist.psu.edu/) exists.
- **Autoencoder reconstruction error:** GPU training; CPU scoring ms-per-event. High-dimensional features. Skip for v1.

**Training contamination:** iForest tolerates ~5–10% contamination; OC-SVM and LOF less so. **Drift:** rolling re-training is the universal answer.

### 4. Log-Specific Methods

Logs are sequences of structured events; most log anomalies are sequence anomalies. Template extraction first, then sequence/rarity modeling.

**Template extraction (prerequisite for everything else in this section):**
- **Drain (He et al. 2017):** fixed-depth parse tree, online. O(depth × maxChild) per event with fixed hyperparameters — effectively constant-time post-warmup. Dominant choice. [Drain3](https://github.com/logpai/Drain3) pip-installable, production-ready.
- **Spell (Du & Li 2016):** LCS-based, online, slightly better accuracy, slower.
- **LogMine (Hamooni 2016):** hierarchical clustering, batch.
- [logparser benchmark](https://github.com/logpai/logparser) compares ~15 parsers.

**Even without ML, templatize:** cardinality reduction by 3–4 orders of magnitude, per-template rate anomalies are trivially computable, prerequisite for everything that follows.

**Pattern-rarity scoring (always build first):**
Count-min sketch or exact counter per template; flag (a) new templates, (b) frequency deviations. O(1) per event. **80% of what ML log anomaly detection delivers at 1% of the complexity.**

**Sequence modelers (build only if rarity isn't enough):**
- **DeepLog (Du 2017):** LSTM predicts next template ID from last W templates. GPU train, ms CPU inference. Best for stable structured workflows (HDFS, BGL); signal drops on interleaved microservice logs. [Reference impl](https://github.com/wuyifan18/DeepLog).
- **LogBERT (Guo 2021):** masked-template prediction, deep SVDD objective. Better on interleaved logs. Transformer training cost. [Repo](https://github.com/HelenGuohx/logbert).
- **LogAnomaly (Meng 2019):** semantic template embeddings so "new but similar" templates don't get flagged. [logdeep](https://github.com/donglee-afar/logdeep) implements DeepLog/LogAnomaly/RobustLog.

**Reality check:** academic literature heavily overfits to HDFS and BGL. [Le & Zhang ICSE 2022](https://arxiv.org/abs/2202.04301) shows naive baselines often match deep models on industrial data. Run the pattern-rarity baseline on real logs first, see what's left before investing in deep models.

### 5. Streaming Algorithms

Training-batch models are a non-starter on the ingest hot path.

- **RRCF — Robust Random Cut Forest (Guha et al. ICML 2016):** streaming iForest successor. O(log N) insert/score. Fixed memory. What AWS Kinesis Analytics uses. [Python](https://github.com/kLabUM/rrcf), [Java](https://github.com/aws/random-cut-forest-by-aws).
- **HTM (Numenta):** biologically-inspired. RRCF matches or beats it on most benchmarks. [htm.core](https://github.com/htm-community/htm.core).
- **Streaming z-score / EWMA + ADWIN change-point:** Welford + EWMA + [ADWIN (Bifet 2007)](https://github.com/online-ml/river) is the workhorse combination.
- **Page-Hinkley / CUSUM:** change-point detectors for mean shift.
- **Half-Space Trees (Tan 2011):** streaming iForest-analog, fixed memory. `river.anomaly.HalfSpaceTrees`.

**[river-ml](https://github.com/online-ml/river)** — Python streaming ML library. ADWIN, Half-Space Trees, streaming KMeans, online gaussian models.

**Amortized cost guidance:** sub-microsecond per-event is the bar for the ingest hot path. EWMA fits. RRCF at 10 trees × 256 samples is ~10–50µs per point — OK for aggregated streams, not per-event. Above that, run off the hot path on a sampled/aggregated tier.

**[NAB](https://github.com/numenta/NAB)** (Numenta Anomaly Benchmark) — benchmark + scoring for streaming detectors. Directional, not gospel; biased univariate.

### 6. Cardinality and Cost

**Many series, few points each** (per-tenant × per-service × per-metric = millions of thin series): per-series models infeasible. Strategies:
- **Hierarchical aggregation:** detect at service level; drill into series only when parent fires.
- **Clustering:** group by shape or tags, fit one model per cluster.
- **Cross-series distributional detection:** model the distribution of values across the fleet, flag outlier series. Google's SRE fleet-wide pattern.
- **Top-N:** fit forecasters only on high-traffic series; cheap EWMA on the long tail.

**Few series, many points each** (aggregate gateway metrics): full forecasting stack is fine.

**Memory bounds per series:** EWMA ~16 bytes. Rolling quantile (t-digest, 100 centroids) ~2 KB. RRCF (10 trees × 256 sample) ~200 KB. At 1M active series: RRCF-per-series = 200 GB (don't). EWMA-per-series = 16 MB (fine).

**Distributed considerations for GastroLog:**
- Per-node detectors see only traffic routed to that node. Global anomalies require cross-node aggregation — same `PeerState` broadcast pattern used for route stats.
- Broadcast aggregated counters/histograms, not raw events. t-digests-per-series merge cheaply (associative).
- Alerting should be centralized (one leader decides) to avoid N nodes firing the same alert. Raft-leader-as-alerter pattern.

### 7. Evaluation

**Standard metrics:** Precision/Recall/F1 on labeled anomalies; prefer PR-AUC over ROC-AUC (anomalies are always imbalanced).

**Point-adjusted F1 is misleading.** [Kim et al. 2022](https://arxiv.org/abs/2109.05257) show random scoring beats SOTA under point adjustment. Report raw point-wise F1 too.

**Production-relevant metrics:**
- Alert-to-incident ratio (want >50%, excellent >80%).
- MTTD.
- Human-review rate (>5/shift and on-call ignores).
- Alert noise per day per service — track distribution, not mean.

**Common pitfalls:**
1. Benchmark distributions don't match production.
2. Injected anomalies are easier than real ones — real outages are subtle rate changes, not stepped spikes.
3. Labeled data is biased — labels reflect what humans noticed, not silent failures.
4. Point-adjusted scores mislead.
5. Leakage — split by time, never random.
6. Cold-start behavior matters. Most evaluations skip the first N points; prod can't.

**Benchmarks:** [NAB](https://github.com/numenta/NAB), [Loghub](https://github.com/logpai/loghub), MSL/SMAP (NASA), Yahoo S5, SMD (OmniAnomaly).

### Online vs Offline Split

| Online | Batch-offline | Hybrid (train offline, score online) |
|---|---|---|
| EWMA, CUSUM, streaming z-score | Grubbs, STL, Prophet, SARIMA | iForest, Autoencoder, DeepLog, LogBERT |
| RRCF, HTM, Half-Space Trees | OC-SVM, LOF (base), DBSCAN | LogAnomaly |
| ADWIN, Page-Hinkley | k-Shape clustering | Holt-Winters |
| Drain (online parsing) | LogMine | |

Hot-path: left column only. Dashboards/post-hoc: anything.

### Labeled vs Unlabeled

Production reality is unlabeled. Assume no ground truth, ever.

**Works unlabeled:** all §1–§2, iForest/LOF/DBSCAN/autoencoder (§3), all streaming (§5), DeepLog/LogBERT (self-supervised).

**Needs labels (avoid as primary):** supervised classifiers on anomaly class.

**Semi-supervised ("known normal" + flag deviations):** OC-SVM, iForest, autoencoders, DeepLog all admit this framing. Practical path: tag a 2-week window as "clean" after human review, train, deploy. Re-tag quarterly.

**Human feedback loop:** thumbs-up/down on fired alerts → weak labels → train a post-filter ranker (LambdaMART, GBDT) on top of an unsupervised base. How Netflix Atlas and Datadog Watchdog get lift.

### Explainability

Ordered best → worst:

1. Threshold / EWMA / z-score — "value X was N stddev from moving mean." Fully explainable.
2. STL / Holt-Winters — "residual after trend+seasonality decomposition was anomalous." Decomposition plot = explanation.
3. Pattern-rarity on templates — "template 'DB connection refused' had freq F vs baseline B." Directly actionable.
4. iForest / RRCF — feature-importance (SHAP/displacement-per-dimension) says which features are weird, not why. Partial.
5. LOF / DBSCAN — "point is in low-density region." Geometric, opaque >3D.
6. DeepLog / LogBERT / Autoencoders — black-box. Plan UX compensation: show top-k predicted-vs-actual templates, nearest-normal sequence, template diff.

**Design implication for GastroLog:** every alert must surface the contribution — template, dimension, time window. If the detector can't tell you, wrap it with a secondary attributor. Unexplainable alerts get silenced.

---

## Recommended Starting Point for GastroLog

**Layer 1 (build first):** EWMA + t-digest percentile tracking per (service, metric) series + **template-rarity on Drain-parsed log templates.** Covers 80% of practical alerts at O(1) per event, fully explainable, unlabeled, baseline every fancier method must beat.

**Layer 2:** RRCF on aggregated per-service feature vectors (rate, error rate, p99, template entropy) for multivariate streaming detection. Per-node, results merged leader-side via the existing PeerState broadcast pipeline.

**Layer 3 (only if layers 1–2 leave gaps):** nightly iForest retrain on longer windows for post-hoc investigations; per-template count vectors fed to a simple autoencoder if template-rarity is too noisy on interleaved microservice logs.

**Defer:** DeepLog / LogBERT / Prophet until clear evidence the cheap stack is saturated. Most platforms never need them.

---

## Key References / Libraries

- [PyOD](https://github.com/yzhao062/pyod) — consistent API over 40+ detectors.
- [river](https://github.com/online-ml/river) — streaming ML.
- [Drain3](https://github.com/logpai/Drain3) — log templating.
- [logparser benchmark](https://github.com/logpai/logparser).
- [logdeep](https://github.com/donglee-afar/logdeep) — DeepLog/LogAnomaly/RobustLog.
- [AWS RRCF](https://github.com/aws/random-cut-forest-by-aws).
- [t-digest](https://github.com/tdunning/t-digest), [DDSketch](https://github.com/DataDog/sketches-go).
- [Prophet](https://github.com/facebook/prophet), statsmodels, sklearn.
- [NAB benchmark](https://github.com/numenta/NAB).
- [ICSE 2022 deep-learning log anomaly survey](https://arxiv.org/abs/2202.04301) — read before committing to a deep model.
- [Kim et al. 2022 on rigorous evaluation](https://arxiv.org/abs/2109.05257).
