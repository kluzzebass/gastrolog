# Ingestion

Ingestion is the first stage of the pipeline — where logs enter GastroLog from the outside world. Each source is handled by an **ingester** that listens for messages, wraps them as records with the raw text, protocol-level attributes, and an arrival timestamp, then passes them downstream for [digestion](help:digesters) and [storage](help:storage).

You can run multiple ingesters simultaneously, each feeding into the same pipeline.

## Available Ingesters

| Type | What it does |
|------|-------------|
| [**Syslog**](help:ingester-syslog) | Receives syslog messages over UDP/TCP (RFC 3164 and RFC 5424) |
| [**HTTP**](help:ingester-http) | Accepts Loki-compatible HTTP pushes — drop-in replacement for a Loki endpoint |
| [**RELP**](help:ingester-relp) | Reliable Event Logging Protocol with delivery acknowledgements |
| [**Tail**](help:ingester-tail) | Follows local log files, like `tail -f` |
| [**Docker**](help:ingester-docker) | Streams container logs from a Docker daemon |
| [**Chatterbox**](help:ingester-chatterbox) | Generates random test messages for development |

Select an ingester from the sidebar for configuration details.
