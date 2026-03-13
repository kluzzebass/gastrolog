# File Vault

Type: `file`

Persists logs to disk. Each [chunk](help:general-concepts) becomes a directory containing the record data, an [index](help:indexers) for fast lookups, and the record attributes. Sealed chunks are compressed with zstd and memory-mapped for efficient reads.

| Setting | Description | Default |
|---------|-------------|---------|
| Directory | Vault directory | `<vaults>/<name>` |
| Sealed Backing | Where sealed chunks are stored after sealing | Local |

## What You Should Know

- When left empty, the directory defaults to `vaults/<name>` under the `--vaults` directory (which itself defaults to `--home`)
- Only one process can open a vault directory at a time (enforced by a lock file). In a [cluster](help:clustering), each node must have its own vault directories on local storage
- Sealed chunks are automatically compressed using seekable zstd — no configuration needed
- If GastroLog crashes, it recovers on restart — at most the last partially-written record is lost
- Maximum log file size within a chunk is **4 GB** (32-bit offsets in the file format)
- File vaults can optionally upload sealed chunks to cloud storage — see [Sealed Backing](help:storage-cloud)
