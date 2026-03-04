# Memory Vault

Type: `memory`

Keeps everything in memory. Fast, but data is lost on restart. In a [cluster](help:clustering), the data exists only on the hosting node. Useful for development, testing, and throwaway environments. Often paired with the [Chatterbox ingester](help:ingester-chatterbox) for quick experimentation.

Memory vaults have no user-facing settings. Records are kept in memory until the chunk rotates (default: 10,000 records).
