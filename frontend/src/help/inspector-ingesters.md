# Ingesters

The Ingesters tab shows each configured [ingester](help:ingestion) with its runtime status and metrics. In a [cluster](help:clustering), ingesters are grouped by their owning [node](help:clustering-nodes).

Each ingester displays its name, type, and whether it's currently running or stopped. Expand an ingester to see:

- **Messages ingested**: Total messages received since the ingester started
- **Bytes ingested**: Total data volume received
- **Errors**: Number of messages that failed to parse or process
