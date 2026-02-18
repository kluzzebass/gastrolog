# RELP

Type: `relp`

Receives messages via the Reliable Event Logging Protocol. RELP provides transaction-based delivery with acknowledgements — the sender knows whether each message was received and stored. Commonly used with rsyslog.

| Param | Description | Default |
|-------|-------------|---------|
| `addr` | Listen address | `:2514` |

RELP messages are parsed as syslog. Acknowledgement is sent only after the record is written to the chunk store, providing an end-to-end delivery guarantee.

**Attributes set:** Same as [Syslog](help:ingester-syslog) (facility, severity, hostname, app_name, etc.) plus `remote_ip`.

## Recipes

- [rsyslog](help:recipe-rsyslog) — forward logs from rsyslog over RELP
- [systemd journal](help:recipe-journald) — ship journal entries via rsyslog and RELP
