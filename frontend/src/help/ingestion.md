# Ingestion

Ingestion is the first stage of the pipeline — where logs enter GastroLog from the outside world. Each source is handled by an **ingester** that listens for messages, wraps them as records with raw text, protocol-level attributes, and an arrival timestamp, then passes them downstream for [digestion](help:digesters) and [storage](help:storage).

You can run multiple ingesters simultaneously, each feeding into the same pipeline. Ingesters are configured in [Settings](help:ingesters).

Select an ingester from the sidebar for protocol and attribute details.
