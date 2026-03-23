# Storage Configuration

The Storage settings tab manages two types of storage resources:

## Cloud Services

Cloud services are cluster-wide cloud storage endpoints (S3, GCS, Azure Blob Storage). They are not tied to any specific node. Tiers reference cloud services by ID to store sealed chunks in the cloud.

**Providers:**
- **S3** — Amazon S3 or S3-compatible services (MinIO, Ceph, etc.). Requires bucket, region, and access credentials. Set the Endpoint field for non-AWS services.
- **GCS** — Google Cloud Storage. Requires bucket, region, and a service account credentials JSON.
- **Azure** — Azure Blob Storage. Requires a container name and connection string.

**Storage class fields:**
- **Active Chunk Class** — the local storage class used for active (writable) chunks before they are sealed and uploaded. Lower = faster storage.
- **Cache Class** — the local storage class used for cached copies of sealed chunks downloaded from the cloud. Lower = faster storage.

## Node Storage Areas

Storage areas are locally-attached storage resources declared per node. Each area has:

- **Storage Class** — a numeric rank (lower = faster). Multiple areas can share the same class to form a pool.
- **Label** — a human-readable name for the area.
- **Path** — the filesystem path on the node.
- **Capacity** — maximum bytes the area can use.

Storage areas on the local node can be added or removed. Remote node storage areas are displayed read-only.
