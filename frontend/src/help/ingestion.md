# Ingestion

Ingestion is the first stage of the pipeline — where logs enter GastroLog from the outside world. Each source is handled by an **ingester** that listens for messages, wraps them as records with raw text, protocol-level attributes, and an arrival timestamp, then passes them downstream for [digestion](help:digesters) and [storage](help:storage).

You can run multiple ingesters simultaneously, each feeding into the same pipeline. Ingesters are configured in [Settings → Ingesters](settings:ingesters). In a [cluster](help:clustering), each ingester runs on the [node](help:clustering-nodes) it is assigned to. Records that match a vault on a different node are automatically forwarded.

## Available Ingesters

| Type | What it does |
|------|-------------|
| [**Syslog**](help:ingester-syslog) | Receives syslog messages over UDP/TCP (RFC 3164 and RFC 5424) |
| [**HTTP**](help:ingester-http) | Accepts Loki-compatible HTTP pushes — drop-in replacement for a Loki endpoint |
| [**RELP**](help:ingester-relp) | Reliable Event Logging Protocol with delivery acknowledgements |
| [**OTLP**](help:ingester-otlp) | OpenTelemetry log records via HTTP and gRPC |
| [**Fluent Forward**](help:ingester-fluentfwd) | Fluent Forward protocol (Fluentd / Fluent Bit) over TCP |
| [**Kafka**](help:ingester-kafka) | Consumes messages from a Kafka topic |
| [**MQTT**](help:ingester-mqtt) | Subscribes to MQTT topics on a broker |
| [**Tail**](help:ingester-tail) | Follows local log files, like `tail -f` |
| [**Docker**](help:ingester-docker) | Streams container logs from a Docker daemon |
| [**Metrics**](help:ingester-metrics) | Emits process-level system metrics (CPU, memory, queue depth) |
| [**Chatterbox**](help:ingester-chatterbox) | Generates random test messages for development |

Select an ingester from the sidebar for protocol and configuration details.
