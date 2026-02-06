package chatterbox

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// SyslogFormat generates RFC 3164-style syslog messages.
type SyslogFormat struct {
	pools *AttributePools
}

// NewSyslogFormat creates a syslog format generator.
func NewSyslogFormat(pools *AttributePools) *SyslogFormat {
	return &SyslogFormat{pools: pools}
}

func (f *SyslogFormat) Generate(rng *rand.Rand) ([]byte, map[string]string, time.Time) {
	// Facility (0-23) * 8 + Severity (0-7) = Priority
	facilities := []struct {
		name string
		code int
	}{
		{"kern", 0},
		{"user", 1},
		{"mail", 2},
		{"daemon", 3},
		{"auth", 4},
		{"syslog", 5},
		{"lpr", 6},
		{"cron", 9},
		{"authpriv", 10},
		{"local0", 16},
		{"local7", 23},
	}
	severities := []int{0, 1, 2, 3, 4, 5, 6, 7} // emerg to debug

	programs := []struct {
		name     string
		messages []string
	}{
		{"sshd", []string{
			"Failed password for root from 192.168.1.100 port 22 ssh2",
			"Accepted publickey for admin from 10.0.0.5 port 54321 ssh2",
			"Connection closed by authenticating user root 192.168.1.100 port 22",
			"Disconnected from user admin 10.0.0.5 port 54321",
			"pam_unix(sshd:session): session opened for user admin",
			"pam_unix(sshd:session): session closed for user admin",
		}},
		{"sudo", []string{
			"admin : TTY=pts/0 ; PWD=/home/admin ; USER=root ; COMMAND=/bin/systemctl restart nginx",
			"pam_unix(sudo:session): session opened for user root",
			"admin : 3 incorrect password attempts",
		}},
		{"kernel", []string{
			"Out of memory: Kill process 1234 (java) score 900 or sacrifice child",
			"TCP: request_sock_TCP: Possible SYN flooding on port 80. Sending cookies.",
			"EXT4-fs (sda1): mounted filesystem with ordered data mode",
			"usb 1-1: new high-speed USB device number 2 using xhci_hcd",
			"ata1.00: exception Emask 0x0 SAct 0x0 SErr 0x0 action 0x6 frozen",
		}},
		{"systemd", []string{
			"Started nginx.service - A high performance web server and a reverse proxy server.",
			"Stopped nginx.service - A high performance web server and a reverse proxy server.",
			"Starting postgresql.service - PostgreSQL RDBMS...",
			"Reached target Multi-User System.",
			"Unit docker.service entered failed state.",
		}},
		{"cron", []string{
			"(root) CMD (/usr/local/bin/backup.sh)",
			"(www-data) CMD (php /var/www/app/artisan schedule:run)",
			"(CRON) INFO (No MTA installed, discarding output)",
		}},
		{"postfix/smtp", []string{
			"connect to mail.example.com[93.184.216.34]:25: Connection timed out",
			"A1B2C3D4E5: to=<user@example.com>, relay=mail.example.com[93.184.216.34]:25, status=sent",
			"warning: hostname mx.example.com does not resolve to address 1.2.3.4",
		}},
	}

	facility := pick(rng, facilities)
	severity := pick(rng, severities)
	priority := facility.code*8 + severity
	program := pick(rng, programs)
	message := pick(rng, program.messages)
	pid := rng.IntN(65535)
	host := pick(rng, f.pools.Hosts)

	now := time.Now()
	ts := now.Format("Jan  2 15:04:05")
	line := fmt.Sprintf("<%d>%s %s %s[%d]: %s", priority, ts, host, program.name, pid, message)

	attrs := map[string]string{
		"service":  program.name,
		"facility": facility.name,
		"host":     host,
	}

	return []byte(line), attrs, now
}
