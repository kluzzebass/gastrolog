# Vault Configuration

The Vaults settings tab is where you create and manage vaults and their storage shape.

## Creating a Vault

A vault needs a **Name** and a single **storage shape** — memory, file, or JSONL sink. Configure the shape inline when you click "Add Vault".

The **Enabled** checkbox controls whether the vault starts accepting records immediately. Uncheck it to create the vault in a disabled state — useful when you want to finish configuring storage before routes start directing traffic into it. Toggle it on later from the vault card.

## Storage Type

Each vault has a **type** that determines how it stores data:

| Type | Description |
|------|-------------|
| Memory | RAM-only. Fast, but lost on restart. |
| File | Local disk. Optionally cloud-backed by selecting a Cloud Storage. |
| JSONL | Append-only JSON lines file. Write-only — cannot be searched or queried. Useful for exporting raw records to external tools. |

A **File** vault is local-only by default. Selecting a Cloud Storage on it makes the vault *cloud-backed* — sealed chunks upload to S3/GCS/Azure while the active chunk and a warm cache stay on local disk. There is no separate "Cloud" type; the binding is what makes the difference.

### Common Settings

- **Rotation Policy** — when to seal the active chunk and start a new one. Select a policy from the dropdown, or leave empty for no automatic rotation.
- **Retention Policy** — when sealed chunks become eligible for retention. Select a policy that defines the trigger (max age, max chunk count, etc.); leave empty for no retention.
- **Retention Disposition** — what happens to records when retention triggers. The default is **Delete records on retention**: records drop, storage frees, the routing engine is never invoked. **Send records to routing engine** (`route`) streams the records through the routing table with synthetic `_source = "retention"` and `_vault = "<vault-id>"`, so routes you configure can forward them to an archive vault, cold-storage vault, etc. — use this only when you have a specific archival or forwarding pipeline in mind, otherwise a misconfigured route can re-inject records into this vault and create a cascade; the original chunk is destroyed and its records re-ingested through routing. **Transfer records to another vault unchanged** (`transfer`) re-homes the sealed chunk itself to the **Transfer Target** vault — no record decode, no re-ingest, no routing table involvement — this is the common archive pattern (age old data into a cheaper vault without touching it). The transfer target must be a different, non-cloud file vault; the source vault must also be a plain (non-cloud) file vault. The chunk keeps its own record timestamps and identity, but its retention clock restarts at the destination (a fresh anchor), so a shorter destination TTL does not immediately re-fire retention on arrival. If the target is missing, disabled, or its transfer can't complete, the chunk is retained and the sweep records a `retention-deferred` alarm rather than losing data.
- **Transfer Target** — shown only when Retention Disposition is set to Transfer. The destination vault sealed chunks re-home to when this vault's retention fires; must be a different, non-cloud file vault.
- **Replication Factor** — how many copies of each chunk to maintain. 1 = no replication. 2 = one extra copy (redundancy). 3+ = fault-tolerant quorum. The maximum depends on how many [file storages](help:storage-config) have the matching storage class.

### File Vault Settings

- **Cloud Storage** — optional. Select a [cloud service](help:storage-config) to make the vault cloud-backed; leave as "Local-only" to keep all data on disk. Fixed at vault creation — to change, create a new vault and migrate data.
- **Storage Class** — which [file storages](help:storage-config) this vault uses. For local-only vaults this hosts all chunks; for cloud-backed vaults it hosts the active chunk and warm cache (sealed chunks live in the cloud). The placement manager assigns one file storage per replica.
- **Disk Free Warn** — free space on the vault's backing volume below which the disk-space alarm raises for this vault. An absolute size like `10GB`, or a percentage of the volume like `10%`. Leave empty to inherit the node default.
- **Disk Free Floor** — free space below which the cluster stops accepting new records destined to this vault, on every node, until space frees. Records for other vaults keep flowing. A size like `3GB` or a percentage like `3%`; leave empty to inherit the node default.

There is no vault-level size cap here anymore — the disk-claim bound lives on the retention policy's **Max Size**, which drains oldest chunks past it, and — with that policy's **Refuse** flag on — also refuses admission while over it. See [**Max Size**](help:policy-retention) on the Retention Policies help for the combined mechanics, the min-wins resolution across a vault's attached policies, and the default floor that applies when no attached policy states a size.

Cloud-backed vaults also have:

- **Cache Eviction**, **Cache Budget**, **Cache TTL** — how the warm cache (the local copy of cloud-uploaded chunks) gets reclaimed when disk pressure or age limits are reached.

### Memory Vault Settings

- **Budget** — maximum memory for this vault. Leave empty for the system default.

### JSONL Vault Settings

- **Path** — file path, relative to the node's home directory.

## Editing a Vault

Expand a vault card to edit its name or enable/disable it. The storage shape is fixed once the vault has chunks.

## Rotation and Retention Policies

Policies are shared resources created in the [Policies](settings:policies) tab and referenced by name in vault settings. This lets multiple vaults share the same rotation or retention schedule.
