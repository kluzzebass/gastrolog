# rsyslog

Forward logs from rsyslog to GastroLog over UDP, TCP, or RELP. rsyslog is the default syslog daemon on most Linux distributions.

## UDP (simplest)

The fastest option with the least overhead. Messages can be lost if the network drops packets or GastroLog is temporarily unavailable.

Add to `/etc/rsyslog.d/gastrolog.conf`:

```
*.* @gastrolog.example.com:514
```

`@` means UDP. Replace `gastrolog.example.com` with your GastroLog host.

---

**In GastroLog:** Go to [Settings → Ingesters](settings:ingesters) and create a [Syslog ingester](help:ingester-syslog) with `udp_addr` set to `:514`.

## TCP (reliable)

TCP guarantees delivery order and retransmits lost packets. rsyslog will buffer messages if the connection drops and retry.

Add to `/etc/rsyslog.d/gastrolog.conf`:

```
*.* @@gastrolog.example.com:514
```

`@@` means TCP.

---

**In GastroLog:** Go to [Settings → Ingesters](settings:ingesters) and create a [Syslog ingester](help:ingester-syslog) with `tcp_addr` set to `:514`.

## RELP (guaranteed delivery)

RELP adds application-level acknowledgements on top of TCP — rsyslog knows exactly which messages GastroLog has persisted. No messages are lost even if GastroLog restarts mid-stream. This is the recommended option for production.

Install the rsyslog RELP module:

```bash
# Debian/Ubuntu
sudo apt install rsyslog-relp

# RHEL/CentOS/Fedora
sudo dnf install rsyslog-relp
```

Add to `/etc/rsyslog.d/gastrolog.conf`:

```
module(load="omrelp")

*.* action(
    type="omrelp"
    target="gastrolog.example.com"
    port="2514"
    action.resumeRetryCount="-1"
)
```

`action.resumeRetryCount="-1"` means retry forever if the connection drops.

---

**In GastroLog:** Go to [Settings → Ingesters](settings:ingesters) and create a [RELP ingester](help:ingester-relp) with `addr` set to `:2514`.

## Filtering what gets forwarded

Instead of `*.*` (everything), you can be selective:

```
# Only auth and kernel messages
auth,authpriv.* @@gastrolog.example.com:514
kern.* @@gastrolog.example.com:514

# Everything except debug
*.info @@gastrolog.example.com:514

# Specific program
:programname, isequal, "nginx" @@gastrolog.example.com:514
```

## Apply changes

After editing rsyslog configuration:

```bash
sudo systemctl restart rsyslog
```

Check for errors:

```bash
journalctl -u rsyslog --since "1 min ago"
```
