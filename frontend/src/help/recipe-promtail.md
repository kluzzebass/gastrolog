# Promtail / Grafana Agent

Ship logs to GastroLog using Promtail or Grafana Agent (or Grafana Alloy). If you're already sending logs to Loki, you can point the same pipeline at GastroLog instead — the [HTTP ingester](help:ingester-http) speaks the Loki push API.

## Promtail

Install Promtail following the [Grafana documentation](https://grafana.com/docs/loki/latest/send-data/promtail/). Then configure it to push to GastroLog.

Example `promtail.yaml`:

```yaml
server:
  http_listen_port: 9080

positions:
  filename: /var/lib/promtail/positions.yaml

clients:
  - url: http://gastrolog.example.com:3100/loki/api/v1/push

scrape_configs:
  - job_name: system
    static_configs:
      - targets: [localhost]
        labels:
          job: syslog
          host: myserver
          __path__: /var/log/syslog

  - job_name: nginx
    static_configs:
      - targets: [localhost]
        labels:
          job: nginx
          host: myserver
          __path__: /var/log/nginx/*.log
```

The `labels` become record attributes in GastroLog. Use them for [filtering](help:routing).

## Grafana Agent / Alloy

Grafana Agent (now Alloy) can forward logs with a `loki.write` component.

Example `config.alloy`:

```
local.file_match "syslogs" {
  path_targets = [{"__path__" = "/var/log/syslog"}]
}

loki.source.file "syslogs" {
  targets    = local.file_match.syslogs.targets
  forward_to = [loki.write.gastrolog.receiver]
}

loki.write "gastrolog" {
  endpoint {
    url = "http://gastrolog.example.com:3100/loki/api/v1/push"
  }
}
```

---

**In GastroLog:** Go to [Settings → Ingesters](settings:ingesters) and create an [HTTP ingester](help:ingester-http) with `addr` set to `:3100` (the default).

## Delivery guarantees

By default, the HTTP ingester returns `204` immediately (fire-and-forget). For stronger guarantees, configure Promtail to send the `X-Wait-Ack: true` header via `client_configs.headers` — GastroLog will then wait for the record to be persisted before responding:

```yaml
clients:
  - url: http://gastrolog.example.com:3100/loki/api/v1/push
    headers:
      X-Wait-Ack: "true"
```

## Multiple sources

You can run a single HTTP ingester and have many Promtail instances ship to it. Use labels (`job`, `host`, `env`, etc.) to distinguish sources, then use [filters](help:routing) to direct them to different stores.
