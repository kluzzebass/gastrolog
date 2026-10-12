# Memory Vault

Serves its sealed chunks from RAM. A memory vault takes records through the same pipeline as a file vault: they are durable on disk from the moment they are accepted, every home builds the vault's sealed chunks, and a restart loses nothing. What differs is how the sealed chunks are read and how much the vault keeps: each home holds them in RAM, and the vault's **memory budget** bounds them. Useful as the fast, bounded upstream end of a route chain — records flow in, are searched while recent, then leave by retention.

## Settings

| Setting | Description |
|---------|-------------|
| Memory Budget | The most RAM the vault's sealed chunks may take on each node. Leave empty for the system default. |
| Replication Factor | Number of homes across cluster nodes; each home builds and holds every sealed chunk. |
| Rotation Policy | When to seal the active chunk. |
| Retention Rules | What to do with sealed chunks — typically `Send records to routing engine` so they flow to a file or cloud-backed vault. |

## What You Should Know

- Memory vaults don't need file storages: they are placed on any node, and keep their on-disk pipeline data under the node's home directory.
- Over budget, retention retires the oldest sealed chunks with the vault's retention disposition — exactly like any other retention trigger, so a route or delete disposition decides where the records go.
- Records in the open chunk are searchable before it seals; sealed chunks are read from RAM.
- Commonly paired with the [Chatterbox ingester](help:ingester-chatterbox) for quick experimentation.
