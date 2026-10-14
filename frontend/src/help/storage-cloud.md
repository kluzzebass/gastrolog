# Cloud-backed Vault

A **file vault with a cloud service binding**. The active chunk lives on local disk — only sealed chunks are uploaded to S3, GCS, or Azure Blob Storage. After upload, a query that reads a chunk with no local copy downloads the chunk's object and verifies it into a local **cached copy**, which keeps recently-read chunks on disk.

## Settings

| Setting | Description |
|---------|-------------|
| Cloud Service | Which [cloud storage endpoint](help:storage-config) to use for sealed chunks. |
| Storage Class | The [file storage](help:storage-config) class for the local active chunk and the cached copies. |
| Replication Factor | Number of copies. Each replica has its own local active chunk; sealed chunks are shared in the cloud. |
| Rotation Policy | When to seal the active chunk and upload it. |
| Retention Rules | What to do with aged-out cloud-backed chunks — delete from the cloud store, or send records through the routing engine to another vault. |
| Cache Eviction | `lru` (default) or `ttl` for the cached copies. |
| Cache Budget | Soft cap on the disk usage of cached copies on each node (e.g. `1GiB`). |
| Cache TTL | Eviction age for `ttl` mode (e.g. `1h`, `7d`). |

## Cloud Providers

Credentials for every provider are write-only: the fields below are always empty when a saved service is opened, an empty field keeps the stored credential, and a typed one replaces it. See [Storage settings](help:storage-config) for the full rule.

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
- A query that reads a cloud-backed chunk with no local copy downloads the whole object once, verifies it, and reads its cached copy from then on.
- Cloud services are configured in the [Storage settings](help:storage-config) tab and referenced by name on a file vault's **Cloud Storage** field to make it cloud-backed.
- Use the **Test Connection** button in cloud service settings to verify credentials. On a saved service it runs against the saved configuration and its stored credentials, so there is nothing to retype — enter credentials to test an unsaved change instead.
- Only the placement leader uploads, once per sealed chunk, after its own copy is built. When the placement leader moves to another node, the new leader uploads every sealed chunk that is not in the cloud yet. Every node in the placement keeps the copy of each sealed chunk it built; once the chunk is uploaded that copy is a cached copy, held to the vault's cache eviction policy on every node, and an evicted chunk is fetched back from the cloud on its next read.
