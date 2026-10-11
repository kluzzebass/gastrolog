# Storage Configuration

The Storage settings tab manages two types of storage resources that vaults reference for data placement.

## File Storage

File storages are locally-attached disk resources declared per node. Each file storage has:

- **Name** — a human-readable label for the storage (e.g. "nvme-fast", "hdd-archive").
- **Path** — where chunk data is stored, relative to the node's home directory. Absolute paths (starting with /) are also supported. Defaults to `storage/<name>`.
- **Storage Class** — a numeric rank that indicates speed. Lower numbers mean faster storage (e.g. 1 for NVMe, 2 for SSD, 3 for HDD). Multiple file storages can share the same class to form a pool.
- **Disk Free Warn** — free space on this storage below which the disk-space alarm raises. An absolute size like `10GB`, or a percentage of the volume like `10%`. Leave empty to inherit the node default.
- **Disk Free Floor** — free space below which every vault placed on this storage is refused admission, cluster-wide, until space frees. Vaults on other storages keep ingesting. A size like `3GB` or a percentage like `3%`; leave empty to inherit the node default.

File storages on every node are listed together, sorted by name and badged with their node, and any of them can be edited or removed from whichever node you are connected to. **Add File Storage** asks which **Node** the new storage belongs to, defaulting to the node you are connected to: the path is resolved on that node's disk, and vaults placed on the storage live there.

The thresholds are evaluated once per storage, not once per vault — a below-floor storage refuses every vault placed on it with a single alarm, rather than one alarm per vault sharing the volume.

### How vaults use file storages

When you create a file vault, you assign it a **Storage Class**. The placement manager finds file storages with that class across the cluster and assigns one per replica. For example, a file vault with RF=3 and storage class 1 needs three file storages with class 1 — they can be on different nodes (availability) or the same node (redundancy).

The number of file storages with a matching class is the vault's replication factor cap. The Storage Class picker lists that count per class, the Replication Factor field states the cap and its cause (`Cap 2: 2 file storages with class 1`), and the vault card's [Placement](help:vaults-config#placement) block shows which nodes hold the leader and follower placements, or why the vault is unplaced. To raise the cap, add a file storage with the class on any node.

## Cloud Storage

Cloud storage endpoints are cluster-wide — not tied to any specific node. Cloud-backed vaults reference a cloud service by name to store sealed chunks in object storage.

**Providers:**

- **S3** — Amazon S3 or S3-compatible services (MinIO, Ceph, DigitalOcean Spaces, etc.). Requires Bucket, Region, and access credentials (Access Key + Secret Key). Set the Endpoint field for non-AWS services — it must include an explicit scheme (`https://…`, or `http://…` for a plaintext local/dev endpoint).
- **GCS** — Google Cloud Storage. Requires Bucket and a service account Credentials JSON.
- **Azure** — Azure Blob Storage. Requires a Container name and Connection String.
- **Memory** — an in-process store with no durability: its contents vanish when the node stops. For tests and local development only; the picker labels it that way.

**Restore Speed** and **Restore Days** are always shown; they are disabled, with the reason, for providers that have no restore step (GCS reads archived objects directly; Memory has nothing to restore) and Restore Days applies to S3 only, where a restored object expires after that many days.

### Cloud storage class transitions

With **Cloud storage class transitions** set to *Managed by GastroLog*, sealed chunks move between the provider's cloud storage classes by age: each transition names an age (**After**) and the **Cloud storage class** a chunk older than that moves to. Transitions apply in order, so each **After** must be later than the one before it. The cloud storage class is the provider's own (`GLACIER`, `cool`, …) and has nothing to do with the numeric [storage class](help:storage-config#how-vaults-use-file-storages) that places a vault on a disk.

The **Delete** step is not a cloud storage class and has its own row: a chunk older than its **After** is erased from the cloud store, and nothing restores it. Add it with **+ Delete After**; a service has at most one. Providers charge for deleting a chunk before its class's minimum storage duration, and the save reports that as a warning.

With *Managed externally*, GastroLog never changes a chunk's class or deletes it; the provider's own lifecycle rules do.

### Credentials are write-only

Stored credentials are never sent back to the browser or returned by the API. A cloud service card shows whether credentials are stored, not what they are, and its credential fields are always empty when the card opens:

- **Leave a credential field empty** and the stored value is kept, so saving an unrelated edit never disturbs it.
- **Type a value** and it replaces the stored one for that field alone — enough to rotate a secret key without re-entering the access key.
- **Remove the stored credentials** is the explicit way to drop them and fall back to the provider's ambient chain, since an empty field no longer means "remove this". The CLI spells it `--clear-credentials`.
- **Test Connection** on a saved service tests it as saved — stored credentials against the stored bucket, region and endpoint, ignoring unsaved edits on the card. To test a change first, enter the credentials for it and the test runs against exactly what you typed. Stored credentials are only ever spent on the destination they were stored for, and only for an admin.

`gastrolog config export` is the one path that retrieves credentials; it requires the admin role and names what it carries in the document's `contains_secrets` section.

### Cloud-backed vaults and file storage

A cloud-backed vault is a file vault with a cloud service binding. It still needs file storage on each replica's node:

- The active chunk is buffered locally before it seals and uploads.
- A warm cache holds frequently-read sealed chunks downloaded from the cloud during queries.

Both use the vault's selected **Storage Class** — the same class governs active-chunk placement and cache placement. File storages with that class must exist on every node that hosts a cloud-backed vault replica.
