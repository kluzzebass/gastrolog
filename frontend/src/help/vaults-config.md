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
| File | Local disk. The most common choice for durable storage. |
| Cloud | Sealed chunks in S3/GCS/Azure, active chunk on local disk. |
| JSONL | Append-only JSON lines file. Write-only — cannot be searched or queried. Useful for debugging tier chains or exporting raw records to external tools. |

### Common Settings

- **Rotation Policy** — when to seal the active chunk and start a new one. Select a policy from the dropdown, or leave empty for no automatic rotation.
- **Retention Policy** — what happens to sealed chunks that age out. Select a policy and choose an action: delete the chunk, or eject its records to another tier via a route.
- **Replication Factor** — how many copies of each chunk to maintain. 1 = no replication. 2 = one extra copy (redundancy). 3+ = fault-tolerant quorum. The maximum depends on how many [file storages](help:storage-config) have the matching storage class.

### File Tier Settings

- **Storage Class** — which [file storages](help:storage-config) this tier uses. The placement manager assigns one file storage per replica.

### Cloud Tier Settings

- **Cloud Storage** — which [cloud service](help:storage-config) to use for sealed chunks.
- **Active Chunk Class** — the [file storage](help:storage-config) class for the active chunk (before upload). Use fast storage.
- **Cache Class** — the [file storage](help:storage-config) class for cached cloud chunks during queries. Can be slower.

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
