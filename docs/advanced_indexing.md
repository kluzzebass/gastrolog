# Advanced Indexing & Compression Approaches for Chunk-Local Search

**Status:** Research artifact for [gastrolog-4dh5i](https://github.com/kluzzebass/gastrolog/issues). Reviewed against the GLCB chunk format ([gastrolog-2n697](https://github.com/kluzzebass/gastrolog/issues)) and the existing per-tier file/cloud chunk managers.

**Scope:** What indexing + compression techniques should GastroLog use *inside a single sealed chunk* to make field-level search fast without requiring full decompression? All approaches must be applicable chunk-locally — there is no global cluster-wide index, by design.

**Audience:** Engineers picking the next implementation issue. The recommendation in §1 is the canonical answer; the per-approach assessments in §2 exist so a future reader can question the recommendation with full context.

---

## 1. Executive summary

**Recommended path: a Lucene-style per-chunk codec built on Vellum + Roaring + per-block bloom filters + a Quickwit-style hotcache footer, with optional CLP-derived template extraction layered on top of the raw payload.**

In one sentence: build per-chunk inverted indexes the way Bluge already does in Go, then add ClickHouse's granule-level bloom filters and Quickwit's hotcache footer on top, so cross-node search can open a remote chunk's index with one small range read.

Concrete shape:

- **Term dictionary per indexed field**: `vellum` FST. Pure Go, mmap-friendly, log-friendly compression ratios.
- **Posting list per term**: `RoaringBitmap/roaring` v2 with `RunOptimize()` after build, mmap'd via `FromUnsafeBytes`. The on-disk format is portable, stable, and readable cross-language.
- **Per-block bloom filter**: cheap first-pass skip before any column or postings are touched. ClickHouse-style granularity (8K-32K rows / block).
- **Columnar "fast field" store** for low-cardinality numeric fields (status code, response size buckets, level): bit-packed, gcd-encoded, dictionary-encoded — read directly without scan-decompress. Enables fast aggregations.
- **Hotcache footer**: a small (KB-scale) dense blob at the end of every sealed chunk file containing the FST roots, posting-list directory, bloom filters, and column metadata. A remote node opens the chunk by reading the footer alone via one HTTP range GET, then planning targeted byte-range fetches into the body.
- **Optional, layered on top**: CLP-style template extraction (or pure-Go Drain) at ingest, with the variable streams stored in the columnar fast-field store. Skip if you want to keep the chunk format format-agnostic; revisit when ratio matters more than format simplicity.

The single biggest performance lever from production systems (Lucene, ClickHouse, Splunk, Husky, Quickwit) is **always decide-then-decompress**, never decompress-then-search. Every component above respects that ordering. The single biggest engineering lever is **the codec abstraction** — Lucene proved this in 2010 and Bluge proved it works in Go; pick a versioned codec interface from day one so the chunk format can evolve without rewrites.

What this is **not**: it is not Loki's "scan everything" model (only works with strict label discipline that GastroLog's "any vault, any tier" doesn't enforce); it is not Honeycomb Retriever's pure-column-scan model (works for analytics queries on wide events, loses to bloom-skipping for grep-style log queries); and it is not a CLP-only design (cgo cost is real, and CLP's search engine is not exposed in `clp-ffi-go` as of April 2026).

---

## 2. Per-approach assessment

### 2.1 Finite State Transducers (FSTs) — recommended for term dictionaries

**Data structure.** An FST encodes a sorted dictionary of M unique terms as a minimised, deterministic acyclic state machine. Vellum, like Lucene's `org.apache.lucene.util.fst` and BurntSushi's Rust `fst` crate it borrows from, is a **byte-level Mealy transducer**: transitions carry single bytes plus an output value (typically `uint64`, e.g. a posting-list offset). Bytes — not runes — are the transition unit, so the structure is encoding-agnostic; UTF-8 awareness is layered on top inside automaton implementations (Levenshtein, regex). Construction requires keys in lexicographic order and is single-pass and streaming, so memory during build is bounded by the suffix-sharing register, not by M.

**Compression characteristics.** An FST simultaneously deduplicates **prefixes** (trie-like) and **suffixes** (DAG minimisation), which is qualitatively different from a hash map (no structural sharing; ~57–80% load factor plus per-entry overhead) or a B-tree (≈50% node occupancy plus key duplication across leaves). Concrete numbers from BurntSushi's reference benchmark: 119K dictionary words → 324 KB (29% of raw); 49M DOI URLs → 113 MB (4%); 1.6B Common Crawl URLs → 27 GB (20%); 15.7M Wikipedia titles → 157 MB (41%). For redundant-prefix corpora (URLs, paths, structured log fields, repeated tokens) the FST routinely beats gzip on both ratio and decode speed; for high-entropy random strings it loses to a hash map. Build cost: ~18 s for 15.7M sorted keys; minutes-to-hours for billions. Sorted input is mandatory.

**Query performance.** Exact `Get(key)` is **O(k)** in key length and **independent of M** — one byte-compare and one pointer-chase per key byte against an mmap'd byte slice. Prefix scan and lexicographic range iteration are native, streaming, no materialization of the dictionary. Fuzzy and regex queries work by **intersecting** the FST with a Levenshtein DFA or regex automaton: branches that cannot match are pruned at byte boundaries, so a Levenshtein-2 search over 15M Wikipedia titles completes in ~25–95 ms. No native suffix or substring queries — that requires either a separate reversed FST or a different structure (n-gram bloom or wavelet tree, see §2.5).

**Go libraries.** [`github.com/blevesearch/vellum`](https://github.com/blevesearch/vellum) is the canonical option. **v1.2.0 tagged 2026-01-22** (FIPS-140 sha256 swap, dependency refresh), 215 stars, Apache-2.0, **pure Go, no cgo, mmap-based** at query time (with a `nommap` build tag). Maintenance is **slow but alive** — Couchbase engineers commit when Bleve/zapx need it, not on a regular cadence. Used in production by Bleve, Couchbase Server's full-text index, and the `zapx` segment format. Known watch-outs: a heap-allocation overhead in the Reader (issue #15), a panic in `isEncodedSingle` (issue #21). Ships [`vellum/levenshtein`](https://pkg.go.dev/github.com/blevesearch/vellum/levenshtein) (DFA up to edit distance ~5; build cost grows exponentially beyond) and a regex automaton. Forks (`couchbase/vellum`, `m3dbx/vellum`) are downstream of, or behind, the canonical repo. The original Go FST `smartystreets/mafsa` is unmaintained research code. There is no actively maintained pure-Go alternative; the closest non-Go peer is BurntSushi's Rust crate, which is faster and more featureful but unusable here without cgo.

### 2.2 Roaring Bitmaps — recommended for posting lists

**Data structure.** Roaring partitions a bitmap by the high 16 bits (32-bit form) or high 48 bits (64-bit form) into containers, each holding the low 16 bits in one of three encodings: **array** (sorted `uint16[]`, used at cardinality ≤ 4096), **bitset** (1024 × `uint64`, used at cardinality > 4096), or **run** container (sorted `(value, length)` pairs, chosen via `RunOptimize()` when RLE beats array/bitset). Per-container size is bounded at ~8 KiB (bitset case), making per-container ops cache-friendly and SIMD-amenable. Use **32-bit** for chunk-local row IDs — it has the only portable cross-language on-disk spec, and per-chunk row IDs are 0..N-1 where N « 2³². Use 64-bit only when row IDs genuinely span >32 bits.

**Compression vs. alternatives.** For **IngestTS-ordered, monotone** posting lists (which is GastroLog's case), dense and clustered ranges collapse into run containers — competitive with RLE/PFOR. Sparse posting lists end up in array containers (16-bit-per-doc within a 65k window — comparable to varint-delta after the high-16 prefix is shared). **Elias-Fano** generally beats roaring on raw size for monotone integers, especially sparse ones (close to the information-theoretic lower bound `2n + n⌈log(m/n)⌉` bits). However, roaring's edge is **operations on compressed form**: array∩array uses galloping, bitset∩bitset is 64-bit AND on a fixed 8 KiB region, run∩array is interval scan — none require full decompression and all dispatch container-by-container by shared key. Varint-delta forces sequential scan for any seek; EF supports rank/select but compound AND/OR/NOT across many lists is harder to make branch-predictable than roaring's per-container kernels. For a chunk-local index that intersects 3–10 posting lists per query, roaring is the pragmatic winner on speed × code complexity × library maturity.

**Query performance.** `Rank`/`Select` are O(log n) on container index + O(1)..O(log 4096) inside. Pairwise `And`/`Or`/`AndNot`/`Xor` walk both containers' key arrays in lockstep; per-pair work is O(container size) with type-specialised kernels. Iteration uses an `IntIterable` with `Next()/HasNext()` and an `AdvanceIfNeeded(x)` for skip-to (essential for zig-zag joins). `ParAnd`/`ParHeapOr` parallelise across containers.

**Go libraries.** [`github.com/RoaringBitmap/roaring`](https://github.com/RoaringBitmap/roaring) **v2.18.0** (released 2026-04-14), pure Go, no cgo, 2.9k stars. Used by InfluxDB, Bleve, Datadog. **Maintained by Daniel Lemire** himself — active. Mmap / zero-copy: `FromUnsafeBytes(buf)` deserializes without allocation by aliasing the input buffer (containers become COW-immutable — perfect for read-only chunk-local indexes mmap'd off sealed chunk files); `FrozenView(buf)` exposes CRoaring's "frozen" layout (data/keys colocated, faulting-friendly for large mmaps). The portable little-endian format is documented in [`RoaringFormatSpec`](https://github.com/RoaringBitmap/RoaringFormatSpec/) and stable across languages (32-bit only). Alternatives: `gocroaring` (cgo, rejected by no-cgo constraint), `dgraph-io/sroar` (64-bit, single contiguous buffer, ~15× less RAM than roaring64 on dgraph workloads but custom format and smaller ecosystem). **Stick with `RoaringBitmap/roaring/v2`** — canonical, actively maintained, no-cgo, first-class mmap, stable on-disk spec.

### 2.3 CLP-style log type extraction — viable as a **layer**, not a replacement

**Core technique.** CLP (Rodrigues, Luo, Yuan; OSDI '21, Y-scope / U. Toronto) tokenizes each log line at ingest using a delimiter-driven, pushdown-automata-based parser. Each line is split into a **logtype** (the static skeleton with placeholders, e.g. `Receiving block <var> src: <var> dest: <var>`) and a sequence of **variables** classified by schema rules — integers, floats, dictionary-eligible strings (IDs, hostnames, paths), and small-domain strings get encoded inline as fixed-width integer IDs while large-domain strings are interned in a per-segment **variable dictionary**. Timestamps get their own delta-encoded column. The line is reconstructed losslessly from `(logtype_id, var_ids[], timestamp)`. The same template-mining idea drives the academic family — **Drain** (He et al., ICWS '17, fixed-depth parse tree), **Spell** (LCS-based), **IPLoM** (iterative partitioning) — but those produce templates only, not a full encoded archive with searchable variable streams.

**Compression and search-on-encoded form.** Because the logtype dictionary is tiny (hundreds of entries for millions of lines) and each variable column is homogeneous, downstream zstd compresses 2–8× better than zstd over raw text; the OSDI paper reports ~2× of gzip on real workloads. The same separation makes search work without full decompression: a query string is parsed into the *same* logtype/variable tokens, the logtype dictionary is filtered first (cheap, in-memory), and only matching segments scan their variable columns. **Messy logs** (multi-line stack traces, free-form messages, mixed schemas) degrade gracefully — CLP falls back to treating long unmatched runs as dictionary strings, losing some ratio but never correctness. Drain/Spell are lossier in spirit (templates only, no re-encoding), so they don't give you the compression-plus-search property.

**Query performance.** Keyword search hits the logtype dictionary in O(templates), then narrows to segments. Variable-value search (`status=500`, `ip=10.0.0.1`) hits the per-segment variable dictionary as an integer-ID join — effectively a tiny columnar index per chunk. Wildcards (`*`, `?`) are supported natively: CLP handles the hard case where `*` may straddle the delimiter that separates a variable from static text by enumerating both tokenizations. Regex is supported via CLP's custom parser (claimed ~3× faster than RE2 for log-shaped patterns) but only over what *can* be expressed in the schema; arbitrary regex over the static text still requires touching candidate segments.

**Go availability — the honest gap.** Y-scope ships [`github.com/y-scope/clp-ffi-go`](https://github.com/y-scope/clp-ffi-go) — official Go bindings via cgo over the C++ core. As of March 2026 it's on the `irv2-beta` branch (~9 stars, 6 releases, beta), and crucially it exposes **encode/decode of CLP IR streams plus statistical collectors — not the full search engine**. To get search-on-compressed you still call out to the C++ `clp` / `clp-s` binary or run it as a sidecar. Pure-Go template miners exist and are production-grade for the *parsing* half: [`github.com/faceair/drain`](https://pkg.go.dev/github.com/faceair/drain) and [`github.com/axiomhq/drain3`](https://github.com/axiomhq/drain3) (Go port of `logpai/Drain3`) both do online template extraction, but neither builds a queryable compressed archive — you'd own the variable-column encoding, dictionary persistence, and search planner.

**Three realistic integration paths**, in increasing risk:

1. **Drain (or similar) + custom columnar variable store, pure Go.** Templates extracted at seal time, variable streams stored alongside the existing chunk format. Lower compression ratio than CLP, but no cgo, full control over the format, and it composes cleanly with §2.1/§2.2/§2.4. Lowest risk.
2. **`clp-ffi-go` for IR encoding only, Go-native search planner.** Use CLP's encoding format but never call its search engine. Get the compression win; build search on top of FST + Roaring as if the encoded streams were just another column. Medium risk (cgo cost, IR format stability).
3. **Full `clp-s` binary as a sidecar per node.** Highest fidelity to CLP, but operationally heavy and fits poorly with GastroLog's "every node serves any vault" topology — would require the binary on every node, format-version coordination via Raft, and a process supervisor. Not recommended.

### 2.4 Columnar layout (struct-of-arrays vs array-of-structs) — recommended for fast fields

**The basic question.** For a chunk holding N records each with K fields, do you store records contiguously (row-major / array-of-structs / AoS) or fields contiguously (column-major / struct-of-arrays / SoA)? GastroLog's current chunk format is row-major: one record's full payload is contiguous on disk in `raw.log`, with `idx` and `attr.log` providing pointers and structured attrs sidecar. Most analytics-oriented log stores (Honeycomb, Husky, ClickHouse, Quickwit, VictoriaLogs) use column-major.

**Compression characteristics with zstd.** Column-major dramatically improves general-purpose compression because each column is **homogeneous in domain**: a status_code column has ~5–20 distinct values, a level column has ~5, a timestamp column is monotone, a duration_ms column is small-magnitude integers. zstd's dictionary builds quickly on this kind of homogeneity; compression ratios of 5–20× are typical per column vs ~3–5× for the same data row-major. The cost is per-column overhead — a chunk with 200 fields has 200 columns, each with its own zstd frames. For sparse fields (95% absent), null-bitmaps + dense value runs win further. Bit-packing, GCD encoding, dictionary encoding, frame-of-reference, and delta encoding are all per-column techniques that beat general zstd on integers and low-cardinality strings; zstd then compresses *those* encoded forms further but cheaply.

**Query performance implications.** Column-major lets you read **only the columns the query touches**, which is the single biggest scan-time win. Honeycomb's entire architecture is built on this. For log search where users do `level=error msg=*timeout*`, you read the level column (skip ~99% of records via filter), then read the msg column for the surviving rows — never touching the other 198 columns. Combined with per-block bloom filters, scans get pruned both vertically (which blocks) and horizontally (which columns).

**The interaction with the inverted index.** SoA columns store **values**; an inverted index stores **value → row IDs**. They're complementary: posting lists answer "which rows match this term", columns answer "what's the value of field X for these rows". Lucene's `DocValues` and Tantivy's "fast fields" exist precisely because the inverted index alone is bad at sort/aggregate/group-by — you need a columnar projection too. ClickHouse goes the other way: columns are primary, skip indexes are sidecar.

**Go libraries.** No single canonical "log columnar store" library. The building blocks: [`apache/arrow-go`](https://github.com/apache/arrow-go) for Arrow IPC and Parquet (heavy, but the canonical Go columnar runtime); [`klauspost/compress/zstd`](https://github.com/klauspost/compress) (already in use); your own bit-packing / dictionary / RLE codecs (small and well-understood, no library needed). For per-block bloom filters, [`bits-and-blooms/bloom`](https://github.com/bits-and-blooms/bloom) is the canonical Go option, no cgo, well-maintained.

### 2.5 Approaches the issue invited me to consider beyond the listed four

**Per-block / per-granule bloom filters (ClickHouse pattern).** Cheaper than any inverted index. A bloom over tokens in each 8K-row block lets you skip the entire block on a `tokenbf` miss before any column is touched. Variants: `tokenbf` (exact tokens), `ngrambf` (substrings), plain `bloom_filter` (set membership). False-positive rate is a tunable knob (memory vs skip rate). **Highly recommended as a first-pass skip layer**, regardless of what else gets built. Library: `bits-and-blooms/bloom`.

**Bluge's Ice segment format (Go).** Bluge is `blugelabs`' Go-native fork of Bleve, designed around a versioned **Ice** segment format with a pluggable codec interface. Vellum FST per field, postings, doc-values, mmap by default. Pure Go, no cgo. **This is the closest existing reference for "Lucene-style indexing in Go."** Read the Ice format before designing GastroLog's chunk index — there's a high probability Bluge's format is good enough to *embed* directly inside the GLCB blob, or to use as the inspiration for a GastroLog-tuned codec. Repo: [github.com/blugelabs/bluge](https://github.com/blugelabs/bluge).

**Quickwit hotcache footer.** Quickwit's `hotcache` is a small (KB to single-digit MB) blob at the end of every split that contains the metadata structures (term dict roots, fast field metadata, file pointers) needed to *open* the split. With it, a searcher answers queries by issuing precisely-targeted byte-range GETs to S3 instead of downloading the index. **For GastroLog's cross-node and cloud-tier search story, this is the most directly portable idea in the survey.** Co-locate the footer with the chunk; teach the cluster's record forwarder to fetch just the footer, plan, then fetch only the body bytes the query needs.

**Succinct data structures, FM-indexes, wavelet trees, learned indexes.** All exist; none have a mature Go library at the level of Vellum or Roaring. FM-indexes give substring search in O(p log n) on compressed text but the implementation effort is large. Wavelet trees give rank/select on arbitrary alphabets and are theoretically great for log token streams but again no Go library worth using. Learned indexes (RMI, ALEX) are interesting for monotone numeric data but don't compose with the inverted-index world. **Not recommended for v1**; revisit if Roaring + FST hit a wall and the team has appetite for a research-grade implementation.

**ANN / embedding-based search.** Out of scope for grep-style log search; relevant if GastroLog ever adds semantic search over log content. Not part of v1.

**Production-system patterns worth stealing**, beyond what's covered above:

- **Splunk's TSIDX reduction** (drop the lexicon for cold buckets, keep the journal): a clean tier story for the cloud tier — store the index when warm, drop it when cold and accept slow scan.
- **VictoriaLogs's stream-as-partition**: collapse the index size by treating the stream identity as the primary key. If GastroLog's natural stream key (vault × ingester × source) has high selectivity, much of the per-chunk index work can be avoided entirely.
- **Husky's column-skip-list + sketch-instead-of-bloom**: their "superset regex" sketch is a real design point for log content where users do `error` and `err.*timeout`.
- **ClickHouse's deprecation of `tokenbf_v1` / `ngrambf_v1` in favor of a real `text` inverted index** (2025+): bloom variants are useful but not sufficient for full-text search at scale. Don't try to skip the inverted index entirely.

---

## 3. Synergy matrix

Approaches compose well or poorly. The matrix below names the pairs that are explicitly synergistic, redundant, or mutually exclusive.

| | FST term dict | Roaring postings | CLP/Drain templates | SoA columnar | Per-block bloom | Hotcache footer |
|---|---|---|---|---|---|---|
| **FST term dict** | — | **Synergistic** (Lucene's canonical pairing: FST keys to posting offsets) | Independent (FST indexes tokens, CLP indexes templates — different surfaces) | Synergistic (FST for indexed string fields, columnar for numeric fast fields) | Synergistic (bloom is first-pass, FST is exact-match second-pass) | **Synergistic** (footer carries FST roots) |
| **Roaring postings** | **Synergistic** | — | Independent (Roaring stores row IDs of records matching CLP-encoded values) | Synergistic (Roaring identifies rows; columns provide values for those rows) | Synergistic (bloom skips blocks, Roaring intersects within surviving blocks) | **Synergistic** (footer carries posting-list directory) |
| **CLP/Drain templates** | Independent | Independent | — | **Synergistic** (CLP variable streams ARE columnar; CLP enables column-store benefits) | Independent (templates and blooms are different first-pass filters) | Synergistic (footer can carry the template dictionary) |
| **SoA columnar** | Synergistic | Synergistic | **Synergistic** | — | **Synergistic** (skip whole blocks before touching columns) | Synergistic (footer carries column metadata) |
| **Per-block bloom** | Synergistic | Synergistic | Independent | **Synergistic** | — | **Synergistic** (footer holds the blooms) |
| **Hotcache footer** | **Synergistic** | **Synergistic** | Synergistic | Synergistic | **Synergistic** | — |

**Key synergies to call out:**

- **FST + Roaring + per-block bloom + hotcache footer** is the Lucene/Bluge pattern, refined with ClickHouse's first-pass skip and Quickwit's remote-friendly footer. This is the recommended core stack.
- **CLP/Drain + SoA columnar** is intrinsically the same idea (CLP's variable streams *are* columns); choosing one selects an architecture for the other.
- **Per-block bloom + everything** — the bloom is cheap, evaluates before any decompression, and short-circuits the more expensive layers. There is no reason to skip it.

**Mutually exclusive choices:**

- **CLP-encoded payload vs raw payload + Drain extraction.** Either the chunk format embeds CLP IR (path 2 in §2.3) or it stores raw text and uses Drain on the side (path 1). You don't do both.
- **32-bit vs 64-bit Roaring.** Per chunk, pick one. 32-bit if row IDs are 0..N-1 within the chunk (which is GastroLog's case); 64-bit only if sharing posting lists across chunks (which is forbidden by the chunk-local constraint).

**Redundancies:**

- `tokenbf_v1` style ngram bloom + FST term dict are partially redundant — both answer "could this token be in this block?". The bloom is cheaper but lossy (false positives). Use both layered, not one or the other.

---

## 4. Constraint compliance check

| Approach | Chunk-local | No full decompression to search | Verdict |
|---|---|---|---|
| **FST term dictionary (Vellum)** | ✅ Per-chunk dictionary, mmap'd from chunk file | ✅ Search is byte-walk over mmap'd FST; nothing decompressed | **Eligible** |
| **Roaring posting lists** | ✅ Per-chunk row IDs (32-bit, 0..N-1) | ✅ Boolean ops run on compressed container forms | **Eligible** |
| **CLP-encoded chunks (via cgo)** | ✅ Per-segment logtype + variable dict | ✅ Search-on-encoded form is the headline feature | **Eligible** (cgo caveat) |
| **CLP-encoded chunks (Drain + columnar in pure Go)** | ✅ Per-chunk template dict | ✅ Per-column reads, no full chunk decompress | **Eligible** |
| **SoA columnar layout** | ✅ Columns live inside the chunk | ✅ Read only the columns the query touches | **Eligible** |
| **Per-block bloom filters** | ✅ One bloom per block within the chunk | ✅ Bloom evaluated before any read | **Eligible** |
| **Quickwit hotcache footer** | ✅ Per-chunk footer at end of file | ✅ Footer-only read suffices to plan; body fetched targeted | **Eligible** |
| **FM-index / wavelet trees** | ✅ Per-chunk | ✅ Search on compressed form | **Eligible but no mature Go library — defer** |
| **Learned indexes** | ⚠️ Per-chunk possible but not the right shape for string keys | ✅ Constant-time-ish lookups | **Not a fit for log content; defer** |
| **Lucene `DocValues` / Tantivy fast fields** | ✅ Per-segment | ✅ Bit-packed columnar reads | **Eligible (subsumed by §2.4 columnar)** |
| **VictoriaLogs stream-as-partition** | ⚠️ This is a partitioning strategy, not a chunk-local index | n/a | **Different layer; complementary** |
| **Loki's no-index-grep model** | ⚠️ Chunk-local in the trivial sense (no index) | ❌ Requires decompression to search | **Rejected — fails the no-decompress constraint** |
| **Honeycomb Retriever pure-scan** | ⚠️ Chunk-local but scan-only | ❌ Requires decompression of the searched columns | **Rejected for grep-style search; OK for analytics, not v1 priority** |
| **Splunk TSIDX format** | ✅ Per-bucket | ✅ Lexicon-first, journal seek | **Pattern is eligible; format is closed-source — adopt the *idea*, not the file format** |
| **Datadog Husky fragments** | ✅ Per-fragment | ✅ Skip-list of column offsets, sketch-based skip | **Pattern is eligible; format is internal — adopt the *idea*** |
| **ClickHouse skip indexes (`tokenbf_v1`, `bloom_filter`, etc.)** | ✅ Per-granule | ✅ Sketch evaluated before decompression | **Eligible — covered by §2.5 per-block bloom** |

---

## 5. Recommended path forward

### 5.1 Implementation phases

**Phase 0 — Spike and benchmark (1–2 weeks).** Before committing format, build a minimal per-chunk index against an existing GLCB chunk: Vellum FST over a single tokenized field + Roaring posting list per term + a flat bloom over all tokens in the chunk. Measure (a) build time at seal, (b) index size as a fraction of the chunk's raw payload, (c) search latency vs the current scan path. Concrete acceptance: 5× search speedup on `level=error` style queries, ≤15% index size overhead. **Open issue: spike issue, no production code.**

**Phase 1 — Codec abstraction + base format (3–4 weeks).** Land a versioned codec interface for the GLCB chunk format, modeled on Lucene's `Codec` and Bluge's Ice. The interface MUST allow:
- mmap-based reads
- format version embedded in the chunk header
- old chunks readable after format evolution (no rewrite required)
- pluggable index types per field

Land v1 of the codec with: per-field FST term dict (Vellum), per-term Roaring posting list, per-block bloom filter (`bits-and-blooms/bloom`), and the hotcache footer with the index directory. Do NOT add columnar fast fields or CLP yet.

**Phase 2 — Cross-node footer-only opening (1–2 weeks).** Teach the cluster's chunk forwarder and the cloud-tier reader to fetch just the hotcache footer for remote chunks. Search planner uses the footer to decide which body byte ranges to fetch. Quickwit's `quickwit-search` is the reference. Concrete acceptance: cross-node search of cloud-tier chunks issues O(log N) range GETs instead of full-blob fetches.

**Phase 3 — Columnar fast fields (3–4 weeks).** Add SoA columnar storage for low-cardinality numeric and enum fields (level, status, response time buckets). Bit-pack, GCD-encode, dictionary-encode as appropriate. Read-only at search time, no rewrite of historical chunks (codec abstraction earns its keep here).

**Phase 4 — Optional: template extraction (variable scope).** Either path 1 (Drain + custom columnar variable store, pure Go) or path 2 (`clp-ffi-go` for IR encoding only). Path 1 is the safer default; path 2 is on the table if the compression ratio gap proves to matter. **Defer the decision** until production data shows the gap is worth the cgo cost.

### 5.2 Concrete next-step issues to file

1. **Spike: per-chunk Vellum + Roaring + bloom prototype.** Acceptance: benchmark report, decision on continuing.
2. **Codec abstraction for GLCB chunk format.** Versioned interface, format header bump, migration story for existing chunks.
3. **Hotcache footer in chunk format.** On-disk layout, builder API, remote-open path in cluster forwarder.
4. **Per-field inverted index v1.** FST + Roaring + bloom for tokenized string fields. Default on for newly-sealed chunks; opt-in for replay of historical chunks.
5. **Columnar fast fields v1.** Bit-packed/dictionary-encoded numeric columns. Aggregations (count, sum, avg) read columns directly.
6. **Cross-node search via footer-only open.** Forwarder fetches footer; search planner issues targeted byte-range GETs. Tested against the cluster reliability matrix in [gastrolog-5ff7z](https://github.com/kluzzebass/gastrolog/issues).
7. **Decision issue (defer): template extraction strategy.** Path 1 vs path 2 vs none. Decide after Phase 1 production data is available.

### 5.3 Benchmarks to plan against

- **Index size overhead.** Cap at 15% of raw chunk size. If FST + posting lists + blooms exceed this, revisit field selection (don't index everything).
- **Search latency.** 5× speedup on field-equality queries (`level=error`), 2× on substring queries (`msg=*timeout*`), no regression on time-range-only queries (already fast via TS index).
- **Cross-node remote-open bytes.** Hotcache footer + planned body GETs should sum to <5% of full-chunk size for typical queries.
- **Build time at seal.** No more than 2× the current seal time.
- **No regression in existing reliability tests.** [gastrolog-5ff7z](https://github.com/kluzzebass/gastrolog/issues) matrix, including transition + retention paths.

### 5.4 Integration points with the GLCB chunk format ([gastrolog-2n697](https://github.com/kluzzebass/gastrolog/issues))

The current GLCB blob carries:
- A header with format version + flags
- The compressed body (seekable zstd frames)
- Embedded TS indexes (per gastrolog-2n697)

The proposed extension:
- Bump the format version
- Add a **hotcache footer** at the tail of the file: index directory, FST roots, bloom filters, column metadata
- The body retains seekable zstd; index data lives in the footer (also zstd'd or not — the footer is small enough that compression ratio matters less than parse speed)
- Old chunks (without footer) remain readable; the codec dispatches on the format version in the header

This is the same shape as Quickwit splits (Tantivy index + hotcache footer) and works equally well for local file chunks and S3-backed cloud chunks — the footer is the universal "open me cheaply" entry point.

---

## Bibliography

### Foundational papers
- [CLP: Efficient and Scalable Search on Compressed Text Logs (Rodrigues et al., OSDI '21)](https://www.usenix.org/system/files/osdi21-rodrigues.pdf)
- [Drain: An Online Log Parsing Approach (He et al., ICWS '17)](https://jiemingzhu.github.io/pub/pjhe_icws2017.pdf)
- [Better bitmap performance with Roaring (Lemire et al., Software: Practice and Experience, 2014; arXiv:1402.6407)](https://arxiv.org/pdf/1402.6407)
- [Consistently faster and smaller compressed bitmaps with Roaring (arXiv:1603.06549)](https://arxiv.org/pdf/1603.06549)
- [Index 1,600,000,000 Keys with Automata and Rust — Andrew Gallant](https://burntsushi.net/transducers/)

### Production engineering blogs (most useful entries)
- [Introducing Husky, Datadog's third-generation event store](https://www.datadoghq.com/blog/engineering/introducing-husky/)
- [Husky: Efficient compaction at Datadog scale](https://www.datadoghq.com/blog/engineering/husky-storage-compaction/)
- [Why Observability Requires a Distributed Column Store (Honeycomb)](https://www.honeycomb.io/blog/why-observability-requires-distributed-column-store)
- [Quickwit 101 — architecture on object storage](https://quickwit.io/blog/quickwit-101)
- [Tantivy 0.22 release notes (Quickwit blog)](https://quickwit.io/blog/tantivy-0.22)
- [Using Finite State Transducers in Lucene — Mike McCandless (2010)](https://blog.mikemccandless.com/2010/12/using-finite-state-transducers-in.html)
- [VictoriaLogs Source Reading (Wonderland blog, Jan 2025)](https://blog.waynest.com/2025/01/victorialogs-source-reading/)
- [ClickHouse Black Magic: Skipping Indices (Altinity)](https://altinity.com/blog/clickhouse-black-magic-skipping-indices)
- [Frame of Reference and Roaring Bitmaps (Elastic)](https://www.elastic.co/blog/frame-of-reference-and-roaring-bitmaps)

### Go libraries cited
- [`blevesearch/vellum`](https://github.com/blevesearch/vellum) — FST term dictionary
- [`RoaringBitmap/roaring`](https://github.com/RoaringBitmap/roaring) — Roaring bitmaps
- [`bits-and-blooms/bloom`](https://github.com/bits-and-blooms/bloom) — bloom filters
- [`blugelabs/bluge`](https://github.com/blugelabs/bluge) — Lucene-style indexing in Go (codec reference)
- [`blugelabs/ice`](https://github.com/blugelabs/ice) — Bluge's segment format
- [`y-scope/clp-ffi-go`](https://github.com/y-scope/clp-ffi-go) — CLP IR encode/decode (cgo)
- [`faceair/drain`](https://pkg.go.dev/github.com/faceair/drain) — Drain log parser (pure Go)
- [`axiomhq/drain3`](https://github.com/axiomhq/drain3) — Drain3 port (pure Go)
- [`apache/arrow-go`](https://github.com/apache/arrow-go) — Arrow / Parquet runtime (heavy)

### Reference systems' source
- [Apache Lucene file formats](https://github.com/apache/lucene/blob/main/dev-docs/file-formats.md)
- [`quickwit-oss/tantivy`](https://github.com/quickwit-oss/tantivy)
- [Loki storage architecture](https://grafana.com/docs/loki/latest/get-started/architecture/)
- [Splunk tsidx file (Splexicon)](https://docs.splunk.com/Splexicon:Tsidxfile)
- [ClickHouse skip indexes documentation](https://clickhouse.com/docs/optimize/skipping-indexes)
