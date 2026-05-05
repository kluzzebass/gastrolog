# Vault Configuration

The Vaults settings tab is where you create and manage vaults and their tier chains.

## Creating a Vault

A vault needs a **Name** and at least one **tier**. Click "+ Add Tier" to add tiers to the chain. Tiers are ordered — records enter the first tier and flow down via retention eject rules.

The **Enabled** checkbox controls whether the vault starts accepting records immediately. Uncheck it to create the vault in a disabled state — useful when you want to finish configuring the tier chain before routes start directing traffic into it. Toggle it on later from the vault card.

## Tier Settings

Each tier has a **type** that determines how it stores data:

| Type | Description |
|------|-------------|
| Memory | RAM-only. Fast, but lost on restart. |
| File | Local disk. Optionally cloud-backed by selecting a Cloud Storage. |
| JSONL | Append-only JSON lines file. Write-only — cannot be searched or queried. Useful for debugging tier chains or exporting raw records to external tools. |

A **File** tier is local-only by default. Selecting a Cloud Storage on it makes the tier *cloud-backed* — sealed chunks upload to S3/GCS/Azure while the active chunk and a warm cache stay on local disk. There is no separate "Cloud" tier kind; the binding is what makes the difference.

### Common Settings

- **Rotation Policy** — when to seal the active chunk and start a new one. Select a policy from the dropdown, or leave empty for no automatic rotation.
- **Retention Policy** — what happens to sealed chunks that age out. Select a policy and choose an action: delete the chunk, or eject its records to another tier via a route.
- **Replication Factor** — how many copies of each chunk to maintain. 1 = no replication. 2 = one extra copy (redundancy). 3+ = fault-tolerant quorum. The maximum depends on how many [file storages](help:storage-config) have the matching storage class.

### File Tier Settings

- **Cloud Storage** — optional. Select a [cloud service](help:storage-config) to make the tier cloud-backed; leave as "Local-only" to keep all data on disk. Fixed at tier creation — to change, create a new tier and migrate data via retention rules.
- **Storage Class** — which [file storages](help:storage-config) this tier uses. For local-only tiers this hosts all chunks; for cloud-backed tiers it hosts the active chunk and warm cache (sealed chunks live in the cloud). The placement manager assigns one file storage per replica.

Cloud-backed tiers also have:

- **Cache Eviction**, **Cache Budget**, **Cache TTL** — how the warm cache (the local copy of cloud-uploaded chunks) gets reclaimed when disk pressure or age limits are reached.

### Memory Tier Settings

- **Budget** — maximum memory for this tier. Leave empty for the system default.

### JSONL Tier Settings

- **Node** — which node writes the JSONL file.
- **Path** — file path, relative to the node's home directory.

## Editing a Vault

Expand a vault card to edit its name, enable/disable it, or modify its tiers. Changes take effect immediately after saving.

Removing a tier from a vault stops the tier and discards any in-flight data in that tier. Sealed chunks that were already ejected to other tiers are not affected.

## Rotation and Retention Policies

Policies are shared resources created in the [Policies](settings:policies) tab and referenced by name in tier settings. This lets multiple tiers share the same rotation or retention schedule.
