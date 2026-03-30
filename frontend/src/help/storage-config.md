# Storage Configuration

The Storage settings tab manages two types of storage resources that tiers reference for data placement.

## File Storage

File storages are locally-attached disk resources declared per node. Each file storage has:

- **Name** — a human-readable label for the storage (e.g. "nvme-fast", "hdd-archive").
- **Path** — where chunk data is stored, relative to the node's home directory. Absolute paths (starting with /) are also supported. Defaults to `storage/<name>`.
- **Storage Class** — a numeric rank that indicates speed. Lower numbers mean faster storage (e.g. 1 for NVMe, 2 for SSD, 3 for HDD). Multiple file storages can share the same class to form a pool.

File storages on the local node can be added, edited, or removed. Remote node file storages are displayed read-only.

### How tiers use file storages

When you create a file tier, you assign it a **Storage Class**. The placement manager finds file storages with that class across the cluster and assigns one per replica. For example, a file tier with RF=3 and storage class 1 needs three file storages with class 1 — they can be on different nodes (availability) or the same node (redundancy).

The number of file storages with a matching class determines the maximum replication factor for that tier.

## Cloud Storage

Cloud storage endpoints are cluster-wide — not tied to any specific node. Cloud tiers reference a cloud service by name to store sealed chunks in object storage.

**Providers:**

- **S3** — Amazon S3 or S3-compatible services (MinIO, Ceph, DigitalOcean Spaces, etc.). Requires Bucket, Region, and access credentials (Access Key + Secret Key). Set the Endpoint field for non-AWS services.
- **GCS** — Google Cloud Storage. Requires Bucket and a service account Credentials JSON.
- **Azure** — Azure Blob Storage. Requires a Container name and Connection String.

### Cloud tier file storage

Cloud tiers also need file storages for two purposes:

- **Active Chunk Class** — the file storage class for active (writable) chunks before they are sealed and uploaded. Fast storage (low class number) is recommended.
- **Cache Class** — the file storage class for cached copies of sealed chunks downloaded from the cloud during queries. Can be a slower class since cache reads are less latency-sensitive.

Both fields reference file storage classes. File storages with matching classes must exist on any node that hosts a cloud tier replica.
