# Ingesters

The Ingesters tab shows each configured [ingester](help:ingestion) with its runtime status and metrics. The list is flat and sorted by name; in a [cluster](help:clustering), each card names the [node](help:clustering-nodes) it runs on.

Each ingester displays its name, type, and whether it's currently running or stopped. Expand an ingester to see:

- **Messages ingested**: Total messages received since the ingester started
- **Bytes ingested**: Total data volume received
- **Errors**: Failed ingester runs — times the ingester exited with an error and was restarted
