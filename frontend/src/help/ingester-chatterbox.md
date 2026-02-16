# Chatterbox

Type: `chatterbox`

A test ingester that generates random log messages in various formats. Useful for development and trying things out. Messages pass through [digestion](help:digesters) like any other source.

| Param | Description | Default |
|-------|-------------|---------|
| `minInterval` | Minimum delay between messages | `100ms` |
| `maxInterval` | Maximum delay between messages | `1s` |
| `formats` | Comma-separated format list | All formats |
| `formatWeights` | Format=weight pairs for selection | Equal weights |
| `hostCount` | Number of simulated hosts | `10` |
| `serviceCount` | Number of simulated services | `5` |

**Supported formats**: plain, kv, json, access, syslog, weird, multirecord
