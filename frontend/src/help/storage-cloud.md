# Cloud-backed Vault

A **file vault with a cloud service binding**. The active chunk lives on local disk — only sealed chunks are uploaded to S3, GCS, or Azure Blob Storage. After upload, queries fetch the chunk's records via HTTP range requests; a local **warm cache** keeps frequently-read chunks on disk.

## Settings

| Setting | Description |
|---------|-------------|
| Cloud Service | Which [cloud storage endpoint](help:storage-config) to use for sealed chunks. |
| Storage Class | The [file storage](help:storage-config) class for the local active chunk and the warm cache. |
| Replication Factor | Number of copies. Each replica has its own local active chunk; sealed chunks are shared in the cloud. |
| Rotation Policy | When to seal the active chunk and upload it. |
| Retention Rules | What to do with aged-out cloud chunks — delete from the cloud store, or send records through the routing engine to another vault. |
| Cache Eviction | `lru` (default) or `ttl` for the warm cache. |
| Cache Budget | Soft cap on warm-cache disk usage (e.g. `1GiB`). |
| Cache TTL | Eviction age for `ttl` mode (e.g. `1h`, `7d`). |

## Cloud Providers

### S3 / S3-compatible

Supports Amazon S3, MinIO, Cloudflare R2, Backblaze B2, Wasabi, Hetzner, DigitalOcean Spaces, and other S3-compatible services.

| Field | Required | Notes |
|-------|----------|-------|
| Bucket | Yes | |
| Region | No | AWS region (e.g. `us-east-1`) |
| Access Key | Depends | Leave empty for IAM roles / default credential chain |
| Secret Key | Depends | |
| Endpoint | Depends | Required for non-AWS S3-compatible services. Must include an explicit scheme: `https://…`, or `http://…` for a plaintext local/dev endpoint — a bare `host:port` is rejected. |

When credentials are left empty, the AWS SDK resolves them automatically: environment variables, shared credentials file (`~/.aws/credentials`), and IAM instance roles. For S3-compatible services, set **Endpoint** (with its scheme, e.g. `https://minio.example.com:9000`) and provide explicit credentials.

### Azure Blob Storage

| Field | Required |
|-------|----------|
| Container | Yes |
| Connection String | Yes |

### Google Cloud Storage

| Field | Required | Notes |
|-------|----------|-------|
| Bucket | Yes | |
| Credentials JSON | Depends | Leave empty for Application Default Credentials |
| Endpoint | No | For emulators and local dev (e.g. `fake-gcs-server`). Must include an explicit scheme: `https://…`, or `http://…` for a plaintext local/dev endpoint — a bare `host:port` is rejected. Leave empty for the default Google endpoint. |

When Credentials JSON is empty, the GCS client uses ADC: the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, attached service accounts on GCE/GKE, or `gcloud auth application-default login`.

## What You Should Know

- The active chunk is always local — ingestion latency is unaffected by cloud storage.
- Each sealed chunk is stored as a single blob in GLCB format (seekable zstd compression).
- Queries read cloud chunks via HTTP range requests — only the needed frames are downloaded, not the entire blob.
- Cloud services are configured in the [Storage settings](help:storage-config) tab and referenced by name on a file vault's **Cloud Storage** field to make it cloud-backed.
- Use the **Test Connection** button in cloud service settings to verify credentials.
- Follower replicas keep a local compressed copy for queries — they do not upload to the cloud (only the leader uploads).
