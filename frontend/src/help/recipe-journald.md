# systemd journal

Forward logs from systemd's journal to GastroLog. The journal doesn't natively support remote shipping, so you need a forwarder. The two main options are rsyslog (recommended) and `systemd-journal-remote`.

## Option 1: rsyslog (recommended)

rsyslog can read from the journal and forward via syslog or RELP. This is the most flexible and reliable approach.

Install rsyslog and the journal input module:

```bash
# Debian/Ubuntu
sudo apt install rsyslog rsyslog-relp

# RHEL/CentOS/Fedora
sudo dnf install rsyslog rsyslog-relp
```

### Forward via RELP (guaranteed delivery)

Add to `/etc/rsyslog.d/gastrolog.conf`:

```
module(load="imjournal")
module(load="omrelp")

*.* action(
    type="omrelp"
    target="gastrolog.example.com"
    port="2514"
    action.resumeRetryCount="-1"
)
```

**GastroLog side:** Create a [RELP ingester](help:ingester-relp) with `addr` set to `:2514`.

### Forward via syslog TCP

If you don't need RELP's delivery guarantees:

```
module(load="imjournal")

*.* @@gastrolog.example.com:514
```

**GastroLog side:** Create a [Syslog ingester](help:ingester-syslog) with `tcp_addr` set to `:514`.

### Filtering

Forward only specific units:

```
if $programname == "nginx" then @@gastrolog.example.com:514
if $programname == "sshd" then @@gastrolog.example.com:514
```

Or by severity:

```
*.warning @@gastrolog.example.com:514
```

Apply changes:

```bash
sudo systemctl restart rsyslog
```

See the [rsyslog recipe](help:recipe-rsyslog) for more configuration options.

## Option 2: systemd-journal-upload

systemd includes `systemd-journal-upload` which can push journal entries to a remote receiver. However, GastroLog does not implement the `systemd-journal-remote` protocol, so this is **not directly compatible**. Use rsyslog instead.

## Verifying

Check that logs are flowing:

```bash
# Check rsyslog is running
systemctl status rsyslog

# Check for forwarding errors
journalctl -u rsyslog --since "5 min ago"

# Generate a test message
logger -t test "hello from journald"
```

Then search for `test` in GastroLog to confirm the message arrived.
