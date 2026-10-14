# Data at rest, integrity, and the cloud/archive path

Scope: GLCB chunk format (`backend/internal/chunk/glcb`), the raftwal shared
WAL (`backend/internal/raftwal`), the cloud-backed chunk path
(`backend/internal/blobstore`, `backend/internal/chunk/file/manager.go`,
`backend/internal/orchestrator/glcb_catchup.go`), file permissions, deletion
semantics, and the vault multi-tenant boundary. Read-only review (grep +
read); no code changed, no cluster commands run.

All findings are **verified by reading the code** unless explicitly marked
"suspected." File:line citations point at the exact evidence.

---

## Finding 1 — Peer-to-peer GLCB replica pulls skip content-hash verification even when one is available

**Severity: High**
**Threat model: attacker who already controls (or has compromised) one cluster node with valid Raft/RPC membership** — not a remote unauthenticated attacker, and not blob-store access.

### Evidence

- `backend/internal/orchestrator/glcb_catchup.go:281-297` — `verifyAndPromoteGLCB` is the sole gate before a GLCB pulled from a peer node is renamed into the local chunk directory and becomes a servable replica:

  ```go
  func verifyAndPromoteGLCB(tmp, glcbPath string, e vaultctlfsm.ManifestEntry) error {
  	res, err := chunking.BuildResultFromExistingGLCB(tmp, e.SealedAt)
  	if err != nil {
  		_ = os.Remove(tmp)
  		return fmt.Errorf("pulled GLCB failed verification: %w", err)
  	}
  	if int64(res.RecordCount) != e.RecordCount {
  		_ = os.Remove(tmp)
  		return fmt.Errorf("pulled GLCB record count %d != manifest %d", res.RecordCount, e.RecordCount)
  	}
  	return os.Rename(tmp, glcbPath)
  }
  ```

  This checks only that the header parses (`BuildResultFromExistingGLCB` →
  `backend/internal/pipeline/chunking/recover.go:20` → `readGLCBSealMeta` →
  `backend/internal/pipeline/chunking/build.go:169`, which reads layout
  metadata only — no section or whole-blob hash is touched) and that the
  **record count** matches the manifest. It never reads or compares
  `TOCEntry.Hash` / `BlobTOC.BlobDigest`.

- `vaultctlfsm.ManifestEntry` **does** carry a cryptographic digest field,
  `Hash [32]byte` (`backend/internal/vaultraft/vaultctlfsm/fsm.go:221-226`,
  "the GLCB whole-blob digest from the TOC footer"), stamped only by
  `CmdUploadChunk` when a chunk is uploaded to cloud storage
  (`backend/internal/vaultraft/vaultctlfsm/fsm.go:1618-1619, 1707`).
  `orchestratorManifestReader.ExpectedDigest`
  (`backend/internal/orchestrator/manifest_reader.go:55-64`) is the only
  reader of `e.Hash` anywhere in the codebase (confirmed by grep — no other
  reference to `e.Hash`/`entry.Hash`), and it is consulted **exclusively**
  from the cold-cache cloud-download path
  (`verifyDownloadedBlob`, `backend/internal/chunk/file/manager.go:3894-3927`).

### Attacker scenario

A node that is a legitimate Raft/RPC member of the cluster (compromised via
some other vector, or simply running attacker-modified code) can, when
asked by `pullGLCBFromNode` to serve a GLCB
(`backend/internal/orchestrator/glcb_catchup.go:252-279`), stream back a
GLCB with the same `RecordCount` as the manifest but altered record content
— rewritten `Raw` payloads, altered attributes, forged timestamps, whatever
fits the same frame-length envelope (nothing in `verifyAndPromoteGLCB`
checks per-record content). The pulling node accepts it, renames it into
place, and thereafter serves the tampered bytes to queries as if they were
an authentic replica. Because chunks that have never been uploaded to cloud
storage carry a zero `Hash` in the manifest, this is not just "the check was
skipped" — for most locally-replicated chunks there is currently **no
cryptographic anchor available at all** to check against; record count is
the only signal.

This matters specifically because the codebase already solved this problem
correctly for the cloud path (Finding 2) — the gap here is an
inconsistency, not a missing capability.

### Remediation

Add a `Hash [32]byte` (or reuse the existing one when non-zero) to every
sealed `ManifestEntry`, populated by the leader from the GLCB's own TOC
`BlobDigest` at `CmdSealChunk` time (not just at cloud upload), and have
`verifyAndPromoteGLCB` read the pulled blob's TOC and compare
`toc.BlobDigest` against `e.Hash` the same way
`verifyDownloadedBlob` does, rejecting the pull on mismatch.

---

## Finding 2 — CRC32 (WAL) and unkeyed SHA-256 (GLCB) are corruption detectors, not tamper-evidence, against an attacker with disk or blob-store write access

**Severity: Low / Informational** — this is architecturally inherent to
checksum-in-the-same-file designs (true of virtually every WAL/log-structured
store: LevelDB, RocksDB, etcd, Kafka segments all do the same thing) and the
task asked for this to be stated plainly rather than treated as a bug to fix.

**Threat model: attacker with write access to a node's local disk, or to the
cloud/blob-store bucket.** Under either of these threat models the attacker
already has very broad capabilities (read all plaintext data, replace the
`gastrolog` binary, etc.), so this is reported as a fact about the system's
integrity guarantees, not as something to "fix" with a bigger checksum.

### Evidence

- **WAL**: every record is framed as `[groupID:4][type:1][length:4][payload][crc32:4]`
  using `crc32.Castagnoli` (`backend/internal/raftwal/wal.go:66-88`,
  `668-673`), verified at replay
  (`backend/internal/raftwal/wal.go:1039`: `if crc32.Checksum(payload, crc32Table) != storedCRC`).
  CRC32 is a 32-bit, non-cryptographic, linear checksum: an attacker who can
  rewrite `payload` in place can compute a matching `crc32` for the new bytes
  in microseconds. It defends against bit rot, torn writes, and truncation —
  not against deliberate modification.
- **GLCB**: sections and the whole blob are hashed with SHA-256 at write time
  (`backend/internal/chunk/glcb/writer.go:157,408,497,529` — `blobHash`,
  per-section hashes, `BlobDigest`), and the hash is stored **in the same
  file** it protects (`tocEntrySize`/`tocFooterSize` layout,
  `backend/internal/chunk/glcb/format.go:68-76`). SHA-256 is cryptographically
  strong against *accidental* collision, but because the hash and the data it
  covers live in the same attacker-writable file with no external key, an
  attacker with disk write access can rewrite record frames and recompute a
  matching `BlobDigest` / section hash, exactly as with CRC32.
  `backend/internal/chunk/glcb/section.go:31-34` makes this explicit for the
  local read path: *"MapSection does not verify the section's SHA-256 against
  its TOC entry on every call. Local sealed blobs are trusted; corruption is
  the caller's problem to detect..."* — i.e. even the accidental-corruption
  detection this hash could offer is **not exercised** on the hot local-read
  path; it's exercised only by the specific callers noted below.

### What this means concretely

- A local attacker who can write to a node's chunk-store directory or WAL
  directory can rewrite log records (message bodies, attributes, timestamps)
  in a sealed GLCB or an unreplayed WAL segment and the corresponding
  checksums will happily "verify," because the checksum is recomputed over
  the tampered bytes by the same write, not compared against an independent
  reference. **There is no mechanism in this codebase (local disk) that would
  catch this** — no periodic re-hash-against-Raft-truth sweep for local
  chunk files.
- The one place this is actually mitigated is the cloud path via the
  Raft-replicated `ManifestEntry.Hash` (see Finding 1) — because that digest
  is stored in a *different* trust domain (the Raft log, checked before the
  bytes it describes are ever written) from the blob content it verifies, it
  is genuinely tamper-evident against a compromised blob store, provided the
  verifier is actually consulted (Finding 3 covers when it is not).

### Remediation

None recommended as a hard requirement — this is normal for a WAL/blob
format and the fix (signing every local write with a key the attacker
doesn't have) is disproportionate unless the threat model specifically
includes "attacker with node disk access but not process/key access," which
nothing in the current design assumes. Worth stating in the project's
threat-model documentation so operators do not mistake "the blob has a
SHA-256" for "the blob is tamper-proof."

---

## Finding 3 — Cloud-download digest verification is conditional and silently skips on two common conditions

**Severity: Medium**
**Threat model: malicious or compromised blob store / bucket, or a
man-in-the-middle on the object-store connection (mitigated separately by
TLS, see Finding 5).**

### Evidence

`backend/internal/chunk/file/manager.go:3894-3927`:

```go
func (m *Manager) verifyDownloadedBlob(id chunk.ChunkID, path string) error {
	if m.cfg.IntegrityVerifier == nil {
		return nil
	}
	expected, ok := m.cfg.IntegrityVerifier.ExpectedDigest(id)
	if !ok {
		// No FSM expectation on file (entry predates digest recording,
		// or the upload's CmdUploadChunk hasn't applied locally yet).
		// Skip; a later read will re-verify once the FSM catches up.
		return nil
	}
	...
	if toc.BlobDigest != expected {
		return fmt.Errorf("downloaded blob digest mismatch for %s: rejecting cache populate (FSM=%x, blob=%x)", id, expected[:8], toc.BlobDigest[:8])
	}
	return nil
}
```

Verification is skipped (returns `nil`, i.e. "trust it") when:

1. **No `IntegrityVerifier` is wired.** It is wired unconditionally for
   FSM/Raft-backed vaults in `reconfig_vaults.go:862-865`, but a chunk
   manager built any other way (tests, or any future non-standard wiring)
   silently accepts unverified cloud content — the nil check is a silent
   opt-out with no logged warning.
2. **The FSM has no recorded `Hash` yet** for this chunk id — either because
   the entry predates digest recording, or, per the comment, because
   `CmdUploadChunk` hasn't applied locally yet on this node (a real,
   expected race between an upload and this node's Raft apply catching up).
   In this window a cold-cache download of a just-uploaded chunk is
   promoted into the warm cache **without any digest check**, on the sole
   strength of the self-referential transport-frame hashes described next.

Separately, `DownloadAndUnwrap` / `reassembleFromSpill`
(`backend/internal/chunk/glcb/remote_reader.go:83-123`) verifies each
transport frame's SHA-256 against a directory that is embedded in the
**same downloaded object**, produced by the uploader at upload time
(`backend/internal/chunk/glcb/transport.go:36-41`, the `dirEntry` /
`sha256` fields). This check catches truncated/corrupted downloads and
partial transfers, but it is not tamper-evident against a party who
controls the object store: replacing the entire object (content +
directory + hashes) produces a new object that still passes this check,
because nothing here is compared to anything outside the object itself.

### Attacker scenario

An attacker with write access to the cloud bucket (compromised cloud
credentials, insider access to the storage backend, or a
supply-chain-compromised storage provider) replaces a chunk's object with
fabricated content plus a self-consistent transport directory. If the
pulling node's FSM has not yet recorded that chunk's `Hash` (race window
after upload) — or, in a deployment where `IntegrityVerifier` isn't wired —
the forged content passes every check in the download path and is promoted
into the local warm cache, then served to queries as authentic.

### Remediation

- Log at `Warn` (not silently `nil`) whenever verification is skipped for
  either reason, so an operator can see how often downloads land unverified.
- Consider deferring cache promotion (or marking the chunk `suspect`) until
  the FSM's `Hash` is available, rather than trusting the download in the
  interim window — the existing `chunk.ErrChunkSuspect` sentinel
  (`backend/internal/chunk/glcb/remote_reader.go:41`) already models exactly
  this kind of "don't trust yet" state elsewhere in the same file.

---

## Finding 4 — Chunk data, WAL segments, and index files are created world-unreadable only because of directory permissions, not their own mode

**Severity: Low**
**Threat model: another local user on the same host/container who shares a
group with the gastrolog process** (e.g. a shared "docker"/service group, or
a misconfigured deployment). Not exploitable by an unprivileged unrelated
user under the current directory tree, and not exploitable remotely.

### Evidence — actual modes cited

- Chunk data files (`data.glcb`, raw/idx/attr/dict files): `DefaultFileMode
  = 0o644` (`backend/internal/chunk/file/factory.go:35`), applied via
  `cfg.FileMode = cmp.Or(cfg.FileMode, 0o644)`
  (`backend/internal/chunk/file/manager.go:436`) and used at every
  `os.OpenFile` call in that file (e.g. `manager.go:459, 1468, 1479, 1490,
  1501, 2076, 2098, 2123, 2160, 2182, 4384`) — **world-readable** (`rw-r--r--`).
- WAL segment files: `os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND,
  0o644)` (`backend/internal/raftwal/wal.go:809`, and the spare-segment path
  at `:865`) — **world-readable**.
- Index sidecar files (token/JSON/KV/attr): `tmpFile.Chmod(0o644)`
  (`backend/internal/index/file/attr/indexer.go:304`,
  `.../json/indexer.go:565`, `.../kv/indexer.go:416`,
  `.../token/indexer.go:125`) — **world-readable**.
- By contrast, the node's own identity files are deliberately tighter:
  `os.WriteFile(p, ..., 0o640)` for `node_id` and `node_name`
  (`backend/internal/home/home.go:157, 178`), explicitly commented as "not
  secret" — i.e. the codebase already has a convention for a stricter mode
  and simply didn't apply it to the files that actually hold customer log
  data.
- Every directory in the path down to these files is created with `0o750`
  (`rwxr-x---`, no permission bits at all for "other"): home root
  (`home.go:146`), vault stores (`manager.go:453, 1988, 2316, 3830, 4331`),
  index chunk dirs (`index/file/{attr,json,kv,token}/indexer.go`), and Raft
  group dirs (`raftgroup/groupmanager.go:225`). Since Unix directory
  traversal requires execute permission on **every** ancestor directory, an
  unrelated "other" user cannot reach any of these files today — the `0o750`
  ancestor is the only thing standing between "world-readable" and "actually
  world-reachable."
- `hashicorp/raft`'s `FileSnapshotStore` (vendored, not gastrolog code)
  creates its `snapshots/` subdirectory and per-snapshot temp directories
  with `0o755` (`raft@v1.7.3/file_snapshot.go:102, 173`) — world-traversable
  — but this directory is always created *inside* an already-`0o750`
  gastrolog-owned group directory, so the same ancestor-permission argument
  applies: unrelated users still can't reach it, but the "other" bits on the
  snapshot dir itself are meaningless dead weight that would become live
  exposure the moment any ancestor's mode is loosened (e.g. by an operator
  manually `chmod`-ing the home directory, or a future refactor that
  constructs one of these paths via a laxer `MkdirAll` call).

### Why this is still worth flagging

The system currently relies entirely on directory-level "other" bits being
absent, applied consistently, at *every* level of a fairly deep path, to
keep chunk/WAL/index files invisible to non-group users. That is a single
mechanism with no redundancy: one dir created with a wrong literal (`0o755`
instead of `0o750` in a future change, or a third-party dependency like the
raft library given a base path that isn't already locked down) turns
`0o644` chunk files into genuinely world-readable log data. The files
themselves carry no defense-in-depth. Group members, meanwhile, can already
read all of it today (both `0o750` dirs and `0o644` files grant group
access) — if the process's primary/supplementary group is broad on a given
host, this is live today, not hypothetical.

### Remediation

Tighten `DefaultFileMode` for chunk data, WAL segments, and index files to
`0o640` to match the standard already applied to `node_id`/`node_name` —
removing "other" read access at the file level removes the single-point-of-
failure on directory permissions, and does not change behavior for the
`gastrolog` process itself (owner keeps read/write either way).

---

## Finding 5 — Custom S3/GCS endpoints may use plaintext HTTP by explicit operator choice; this is validated, not silently defaulted

**Severity: Informational** — documenting existing, already-fixed-in-code
behavior at the task's request, not reporting a new bug.

### Evidence

`backend/internal/blobstore/factory.go:93-107`:

```go
// validateEndpoint requires an explicit scheme on a custom endpoint. A bare
// host:port used to be silently upgraded to plaintext "http://", which pointed
// data-plane credentials at an unencrypted endpoint whenever an operator
// merely forgot the scheme. Misconfiguration now fails loudly at config time...
func validateEndpoint(ep string) (string, error) {
	if ep == "" {
		return "", nil
	}
	if !strings.Contains(ep, "://") {
		return "", fmt.Errorf("endpoint %q has no scheme: use \"https://%s\", or \"http://%s\" for a plaintext local/dev endpoint", ep, ep, ep)
	}
	return ep, nil
}
```

`ValidateConfig` (`factory.go:118-159`) runs this for both `s3` and `gcs`
providers, at both `createStore` time and the `PutCloudService` RPC handler,
so config-time and init-time validation cannot drift. **An operator can
still explicitly configure `http://`** — the S3 client honors it via
`BaseEndpoint` (`backend/internal/blobstore/s3.go:91-94`), and the upload
path explicitly documents plain-HTTP MinIO/mock compatibility
(`s3.go:80-83`: "our upload path streams GLCB through zstd + io.Pipe over
plain HTTP to MinIO and similar mocks"). If an operator points a **production**
`CloudService` at an `http://` endpoint (rather than a local/dev MinIO),
chunk contents and the SigV4-signed request (which does not encrypt the
body) travel in plaintext, visible to any network position between the node
and the object store.

### Attacker scenario

Network-position attacker (on-path between node and object store) reads
chunk bytes and authorization headers in transit, if and only if the
operator configured a plaintext endpoint for a store that isn't actually
local/trusted-network. This requires an operator misconfiguration that the
code already surfaces distinctly in its error message; it is not a default
or a silent behavior.

### Remediation

None required in code — the guardrail described in the comment already
exists. Consider a startup warning log (not a hard failure, since local dev
against MinIO is a legitimate and documented use) whenever a non-memory
cloud service is configured with a plaintext endpoint that isn't a
loopback/private address, to catch the case where a plaintext dev config
leaks into a production `CloudService`.

---

## Finding 6 — Multi-tenant (vault) isolation on a shared cloud bucket is enforced by key-prefix convention in application code, not by storage-layer access control

**Severity: Low** (design fact, not a bug found in the reviewed code paths)
**Threat model: a node whose cloud credentials are shared across vaults —
i.e. any node with valid credentials for a `CloudService` that multiple
vaults reference.**

### Evidence

- `system.CloudService` is explicitly documented as "a cluster-wide cloud
  storage endpoint" (`backend/internal/system/storage.go:150`) — a single
  bucket + credential set (`ParamBucket`, `ParamAccessKey`/`ParamSecretKey`,
  or an ambient AWS credential chain) that any number of vaults can
  reference via `Vault.CloudServiceID`
  (`backend/internal/system/vault.go:38-39`). There is one `S3Store` /
  `GCSStore` client per `CloudService`, not per vault.
- Isolation between vaults sharing that one `CloudService` is implemented
  entirely as an object-key prefix, computed in application code:
  `cloudPrefix() = "vault-" + VaultID + "/"`
  (`backend/internal/chunk/file/manager.go:4234-4241`), and every `List`
  call is scoped to that prefix
  (`backend/internal/chunk/file/cloud_index_audit.go:26`,
  `manager.go:5054`). This was verified by grepping every call site of
  `CloudStore.List` in the codebase — none list without the per-vault
  prefix.
- There is no bucket policy, IAM condition, or server-side ACL enforced by
  gastrolog itself that would prevent a node holding this shared
  credential from reading or deleting `vault-<other-vault-id>/...` keys
  directly — nothing stops `Download`/`Delete` from being called with an
  arbitrary key string, since `blobstore.Store` takes a bare `key string`
  parameter to every method
  (`backend/internal/blobstore/blobstore.go`, `s3.go:150-227`).

### On the local-disk side (checked and sound)

- `chunk.ChunkID` is a fixed-format, strictly-parsed 26-character
  base32hex UUIDv7 (`backend/internal/chunk/types.go:90-124`,
  `ParseChunkID` rejects anything not exactly 26 chars of the `0-9a-v`
  alphabet). Local chunk directories are `filepath.Join(m.cfg.Dir,
  id.String())` (`manager.go:2724-2725`) where `m.cfg.Dir` is already the
  per-vault store root. There is no way to construct a `ChunkID` string
  containing `/` or `..` — path traversal into another vault's directory
  via a crafted chunk ID is not possible through this API.
- Vault IDs (`glid.GLID`, a UUID) are similarly fixed-format.

### Why this is worth stating plainly

The question asked was "can data from vault A ever land in or be read via
vault B" — the answer, on the evidence read, is **no code path was found
that does this**, but the isolation is a convention (every caller happens to
always compute and use the correct prefix) rather than a boundary the
storage layer itself enforces. A single new code path added later that calls
`CloudStore.Download`/`Delete`/`List` with a hand-built key instead of going
through `blobKey`/`cloudPrefix` would silently cross the tenant boundary
with no defense to catch it. This is the same "single source of truth /
one fill path" pattern already emphasized elsewhere in this codebase's
conventions — applied here it would mean: consider a wrapper type
(`vaultScopedKey`) that only `blobKey`/`cloudPrefix` can construct, rather
than a bare `string`, so a future direct-key call site fails to compile
instead of fails silently at runtime. Not urgent given today's code is
already correct, but worth filing given multiple vaults sharing one
`CloudService` is an explicit, intended feature.

---

## Finding 7 — Deletion is a plain unlink / object-store delete; no secure erase, by design and consistent with the platform

**Severity: N/A — stated as fact per the task's instruction not to invent a
requirement that doesn't exist.**

### Evidence

- Local chunk deletion: `os.RemoveAll(m.chunkDir(id))`
  (`backend/internal/chunk/file/manager.go:3278`, and the orphan-directory
  path at `:3247`) — an ordinary recursive unlink. No overwrite pass, no
  `msync`+zero, nothing beyond what the OS/filesystem does for a normal
  `rm -rf`.
- Cloud chunk deletion: `m.cfg.CloudStore.Delete(ctx, key)`
  (`manager.go:3267`) → provider `DeleteObject` calls (`s3.go:221-227`,
  `gcs.go:92-98`) — an ordinary API delete request. Whether the bytes are
  actually gone, retained under a bucket-versioning/soft-delete policy, or
  recoverable from the provider's own backups is entirely up to the bucket
  configuration, which is outside gastrolog's code and control.
- WAL reclamation removes fully-drained segment files by ordinary
  `os.Remove`/configurable `SegmentRemove` hook
  (`backend/internal/raftwal/wal.go` reclamation path; see also the
  existing `fix(raftwal): report failed segment unlinks` history on this
  branch) — again a normal unlink, not a wipe.

### What this means

Retention "delete" removes the directory entry / object reference. The
underlying disk blocks (local) or provider-internal storage (cloud) are not
guaranteed to be overwritten and may be recoverable via filesystem forensics,
volume snapshots, or the cloud provider's own versioning/backup/retention
policies until those separately expire. This is standard behavior for
essentially all log-aggregation and database systems that don't implement
explicit crypto-shredding, and nothing in the design or its documentation
(`docs/`) claims otherwise — this is reported as a fact for the audit record,
not as a defect to fix.

---

## Checked and sound

- **WAL length-prefixed parsing is hardened against a hostile length
  field.** `replaySegment` explicitly bounds-checks a record's claimed
  `length` against `fileSize - payloadOff` **before** allocating the
  payload buffer, with the allocation-DoS risk called out in a comment
  (`backend/internal/raftwal/wal.go:1025-1034`: "a garbage header can claim
  up to 4GB and the blind allocation would blow the heap for bytes that
  cannot exist"). A zero `entryType` (a zeroed/preallocated region) is also
  detected and stops replay rather than mis-parsing garbage
  (`wal.go:1017-1023`).
- **GLCB header/TOC parsing is bounds-checked before every mmap/allocation.**
  `ReadTOC` rejects any TOC entry whose `Offset+Size` exceeds the file size
  before any downstream code maps or reads it
  (`backend/internal/chunk/glcb/reader.go:64-72`); `parseMappedBlob` and
  `loadRecordTablesLocked` separately bounds-check `DictOff+DictSize` and
  `IndexOff+IndexSize` against the mapping length before slicing
  (`backend/internal/chunk/glcb/mmap_blob.go:194-227`); `MapSection` computes
  its `mmap` window from a TOC entry that was already validated by `ReadTOC`
  (`section.go:38-73`).
- **Record-frame decoding is field-by-field bounds-checked, not
  length-trusting.** `decodeFrame`/`decodeFrameAttrsProjection`
  (`backend/internal/chunk/glcb/reader.go:270-369`) check `off+fieldSize >
  len(frame)` before every single field read (timestamps, GLIDs, attr count,
  attr block, raw length, raw body) — an attacker-influenced or corrupted
  frame produces a returned `error`, not an out-of-bounds read or panic.
- **Object metadata (the cloud-cache-only chunk-meta shortcut) is never
  trusted blindly.** `DecodeObjectMetadata`
  (`backend/internal/chunk/glcb/codec.go:73-127`) treats a missing
  `record_count` key or any unparseable field as a hard error rather than a
  fabricated zero, with a comment explicitly warning that a fabricated zero
  here could wrongly drop or hide a chunk in retention/query paths; callers
  fall back to the authoritative GLCB footer (`BlobMetaToChunkMeta`) on
  error.
- **Chunk IDs cannot be used for path traversal.** `ParseChunkID` enforces
  an exact 26-character base32hex format (`backend/internal/chunk/types.go
  :107-119`); there is no way to encode `/` or `..` in a valid `ChunkID`.
- **Cloud key construction is consistently vault-scoped.** Every
  `CloudStore.List` call site found in the codebase passes
  `m.cloudPrefix()`; no call was found that lists or resolves cloud objects
  without the per-vault prefix.
- **S3 endpoint scheme is validated, not silently defaulted to plaintext.**
  `validateEndpoint` (`backend/internal/blobstore/factory.go:93-107`)
  requires an explicit `://` scheme and is run at both config-save and
  store-construction time, closing a previously-real "bare host:port
  silently becomes http://" gap (per the code's own comment).
- **Directories are consistently created `0o750`** across home root, vault
  store roots, chunk directories, index chunk directories, and Raft group
  directories — verified by grep across `home/home.go`,
  `chunk/file/manager.go`, `index/file/*/indexer.go`, and
  `raftgroup/groupmanager.go`. This is what currently keeps the
  `0o644`-mode files in Finding 4 from being reachable by unrelated users.
