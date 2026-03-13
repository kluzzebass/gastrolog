# Cloud Vault

Type: `cloud`

Archives logs to cloud object storage. Cloud vaults are **sealed-only** — they do not accept live ingestion. Data arrives via a [retention](help:policy-retention) rule with the **eject** action, which streams records from sealed chunks in a file or memory vault through [eject-only routes](help:routing) into cloud storage.

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

- Cloud vaults appear in search results like any other vault — queries scan the full blob since no pre-built indexes exist in cloud storage
- Each chunk is stored as a single compressed blob (GLCB format with zstd compression)
- Use the **Test Connection** button to verify credentials before saving
- In a [cluster](help:clustering), cloud vaults are assigned to a node like any other vault, but the data lives in the cloud rather than on local disk
