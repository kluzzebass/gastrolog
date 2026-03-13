# Sealed Backing (Cloud Storage)

File vaults can optionally upload sealed chunks to cloud object storage. When **Sealed Backing** is set to a cloud provider, chunks are compressed locally, converted to GLCB format, uploaded, and the local copy is deleted. The active chunk always lives on local disk — only sealed chunks move to the cloud.

This replaces the old `cloud` vault type. Existing cloud vaults are automatically migrated to file vaults with sealed backing on startup.

Supported providers:

| Provider | Description |
|----------|-------------|
| **S3 / S3-compatible** | Amazon S3, MinIO, Cloudflare R2, Backblaze B2, Wasabi, Hetzner, DigitalOcean Spaces |
| **Azure Blob Storage** | Microsoft Azure |
| **Google Cloud Storage** | Google Cloud |

## Settings

### S3 / S3-compatible

| Setting | Description | Required |
|---------|-------------|----------|
| Bucket | S3 bucket name | Yes |
| Region | AWS region (e.g. `us-east-1`) | No |
| Access Key | AWS access key ID | No (uses default credentials if empty) |
| Secret Key | AWS secret access key | No |
| Endpoint | Custom endpoint URL for S3-compatible services | No |

### Azure Blob Storage

| Setting | Description | Required |
|---------|-------------|----------|
| Container | Blob container name | Yes |
| Connection String | Azure storage connection string | Yes |

### Google Cloud Storage

| Setting | Description | Required |
|---------|-------------|----------|
| Bucket | GCS bucket name | Yes |
| Credentials JSON | Service account key (JSON) | No (falls back to Application Default Credentials) |
| Endpoint | Custom endpoint URL | No |

## What You Should Know

- File vaults with sealed backing support **live ingestion** — the active chunk is always local, so `Append` and eject both work normally
- Cloud-backed sealed chunks appear in search results like any other chunk — queries download the blob and scan it (no pre-built indexes in cloud storage)
- Each sealed chunk is stored as a single compressed blob (GLCB format with seekable zstd compression)
- On startup, the vault lists blobs from the cloud store and merges them into the chunk metadata alongside any local chunks
- Use the **Test Connection** button to verify credentials before saving
- In a [cluster](help:clustering), vaults with sealed backing are assigned to a node like any other vault — the active chunk is local to that node, sealed chunks live in the cloud
