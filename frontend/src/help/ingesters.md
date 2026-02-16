# Ingestion

Ingestion is the first stage of the pipeline — where logs enter GastroLog from the outside world. Each source is handled by an **ingester** that listens for messages, wraps them as records with the raw text, protocol-level attributes, and an arrival timestamp, then passes them downstream for digestion and storage.

You can run multiple ingesters simultaneously, each feeding into the same pipeline.

## Available Ingesters

| Type | What it does |
|------|-------------|
| **Syslog** | Receives syslog messages over UDP/TCP (RFC 3164 and RFC 5424) |
| **HTTP** | Accepts Loki-compatible HTTP pushes — drop-in replacement for a Loki endpoint |
| **RELP** | Reliable Event Logging Protocol with delivery acknowledgements |
| **Tail** | Follows local log files, like `tail -f` |
| **Docker** | Streams container logs from a Docker daemon |
| **Chatterbox** | Generates random test messages for development |

See the sub-pages for configuration details on each ingester.
