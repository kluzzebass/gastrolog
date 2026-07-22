# Storages

The Storages tab shows each configured [file storage](help:storage-config) with its disk-guard state — the same published signals the admission gate itself consults. In a [cluster](help:clustering), storages are grouped by their owning [node](help:clustering-nodes), since a storage is a single physical volume that only its owning node can sample.

## Storage Overview

Each card's header shows the storage's name, its owning node, a **warn** or **protected** badge when the storage is below one of its thresholds (nothing when healthy), and the last-sampled free/total bytes.

- **warn** — free space has crossed below the warn threshold. The disk-space alarm is raised, naming this storage. Ingestion is unaffected.
- **protected** — free space has crossed below the floor threshold. Every vault placed on this storage is refused admission, cluster-wide, until space frees. Vaults on other storages keep ingesting.

Nothing here is computed in the browser: both badges reflect the same hysteresis-aware verdicts the admission gate reads before accepting or refusing a write.

## Expanded Detail

Expand a storage card to see:

- **Identity** — the storage's path on its owning node.
- **Thresholds** — the effective warn and floor values in bytes, with the configured expression (`10%`, `10GB`) or **inherited** when the storage falls back to its node's default.
- **Live State** — free and total bytes from the last sample, and how long ago that sample was taken. A storage just added to the cluster shows no sample until its owning node's disk guard runs.
- **Placements** — every [vault](help:inspector-vaults) placed on this storage, config-derived so it's correct even before the next guard sample. Click a vault to jump to its card.

## Per-Node View

In [Nodes mode](help:inspector), each node's detail pane lists the storages hosted on that node, alongside its vaults and ingesters.
