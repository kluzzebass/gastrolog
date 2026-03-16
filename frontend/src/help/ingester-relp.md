# RELP

Type: `relp`

Receives messages via the Reliable Event Logging Protocol. RELP provides transaction-based delivery with acknowledgements — the sender knows whether each message was received and stored. Commonly used with rsyslog.

| Setting | Description | Default |
|---------|-------------|---------|
| Listen Address | TCP address for RELP | `:2514` |
| Enable TLS | Wrap connections in TLS | off |
| Certificate | Server certificate from the certificate manager | |
| CA Certificate File | CA for verifying client certificates (mutual TLS) | |
| Allowed Client CN | Wildcard pattern for client certificate Common Name | |

RELP messages are parsed as syslog. Acknowledgement is sent only after the record is written to the chunk vault, providing an end-to-end delivery guarantee.

## TLS

When TLS is enabled, all RELP connections are wrapped in TLS. Select a certificate from the certificate manager to use as the server identity — certificates are managed in the Certificates settings tab.

For mutual TLS (mTLS), also provide a CA Certificate File path — clients must then present a certificate signed by that CA. The Allowed Client CN field optionally restricts which client certificates are accepted using a wildcard pattern (e.g. `*.example.com`).

**Attributes set:** Same as [Syslog](help:ingester-syslog) (facility, severity, hostname, app_name, etc.) plus `remote_ip`.

## Timestamps

IngestTS is set to GastroLog arrival time. SourceTS is not set by this ingester — the syslog timestamp in the RELP payload is unreliable for the same reasons as the [syslog ingester](help:ingester-syslog). The [timestamp digester](help:digester-timestamp) extracts SourceTS during digestion.

## Recipes

- [rsyslog](help:recipe-rsyslog) — forward logs from rsyslog over RELP
- [systemd journal](help:recipe-journald) — ship journal entries via rsyslog and RELP
