# Vaults

The Vaults tab shows each configured [vault](help:storage) with its [chunks](help:general-concepts) and [indexes](help:indexers).

## Vault Overview

Each vault lists its name, type, enabled/disabled status, total chunk count, and record count. Expand a vault to see its chunk timeline.

## Chunks

Each chunk shows its ID, time range, status (active or sealed), record count, and size. The active chunk is the one currently accepting writes — all others are sealed and immutable. Chunks are sealed according to the vault's [rotation policy](help:policy-rotation).

## Indexes

Sealed chunks list their [indexes](help:indexers) with name, status, entry count, and size. An index in **ready** status is being used by the [query engine](help:query-engine). A **building** index is still being constructed in the background.

## Validate

The Validate button checks data integrity for a vault — verifying that chunk files are consistent and indexes match their data. Use it if you suspect corruption after a crash or disk issue.
