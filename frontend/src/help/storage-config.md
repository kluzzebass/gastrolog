# Storage Configuration

The Storage settings tab manages two types of storage resources that vaults reference for data placement.

## File Storage

File storages are locally-attached disk resources declared per node. Each file storage has:

- **Name** — a human-readable label for the storage (e.g. "nvme-fast", "hdd-archive").
- **Path** — where chunk data is stored, relative to the node's home directory. Absolute paths (starting with /) are also supported. Defaults to `storage/<name>`.
- **Storage Class** — a numeric rank that indicates speed. Lower numbers mean faster storage (e.g. 1 for NVMe, 2 for SSD, 3 for HDD). Multiple file storages can share the same class to form a pool.

File storages on the local node can be added, edited, or removed. Peer node file storages are displayed read-only.

### How vaults use file storages

When you create a file vault, you assign it a **Storage Class**. The placement manager finds file storages with that class across the cluster and assigns one per replica. For example, a file vault with RF=3 and storage class 1 needs three file storages with class 1 — they can be on different nodes (availability) or the same node (redundancy).

The number of file storages with a matching class determines the maximum replication factor for that vault.

## Cloud Storage

Cloud storage endpoints are cluster-wide — not tied to any specific node. Cloud-backed vaults reference a cloud service by name to store sealed chunks in object storage.

**Providers:**

- **S3** — Amazon S3 or S3-compatible services (MinIO, Ceph, DigitalOcean Spaces, etc.). Requires Bucket, Region, and access credentials (Access Key + Secret Key). Set the Endpoint field for non-AWS services — it must include an explicit scheme (`https://…`, or `http://…` for a plaintext local/dev endpoint).
- **GCS** — Google Cloud Storage. Requires Bucket and a service account Credentials JSON.
- **Azure** — Azure Blob Storage. Requires a Container name and Connection String.

### Cloud-backed vaults and file storage

A cloud-backed vault is a file vault with a cloud service binding. It still needs file storage on each replica's node:

- The active chunk is buffered locally before it seals and uploads.
- A warm cache holds frequently-read sealed chunks downloaded from the cloud during queries.

Both use the vault's selected **Storage Class** — the same class governs active-chunk placement and cache placement. File storages with that class must exist on every node that hosts a cloud-backed vault replica.
