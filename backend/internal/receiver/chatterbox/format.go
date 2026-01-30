package chatterbox

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
)

// LogFormat generates synthetic log messages with associated source attributes.
type LogFormat interface {
	// Generate returns a raw log message and source attributes.
	// The attrs map should contain stable attribute dimensions that
	// can be used to form meaningful SourceIDs.
	Generate(rng *rand.Rand) (raw []byte, attrs map[string]string)
}

// AttributePools holds pre-generated pools of attribute values.
// These are shared across format implementations to ensure consistent
// cardinality and enable source grouping.
type AttributePools struct {
	Hosts    []string
	Services []string
	Envs     []string
	VHosts   []string
}

// NewAttributePools creates attribute pools with the specified sizes.
func NewAttributePools(hostCount, serviceCount int) *AttributePools {
	hosts := make([]string, hostCount)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("host-%d", i+1)
	}

	services := make([]string, serviceCount)
	serviceNames := []string{"api", "web", "backend", "worker", "gateway", "auth", "cache", "db-proxy", "scheduler", "metrics"}
	for i := range services {
		services[i] = serviceNames[i%len(serviceNames)]
	}

	envs := []string{"prod", "staging", "dev", "test"}

	vhosts := []string{
		"example.com",
		"api.example.com",
		"admin.example.com",
		"cdn.example.com",
		"static.example.com",
	}

	return &AttributePools{
		Hosts:    hosts,
		Services: services,
		Envs:     envs,
		VHosts:   vhosts,
	}
}

// pick returns a random element from the slice.
func pick[T any](rng *rand.Rand, s []T) T {
	return s[rng.IntN(len(s))]
}

// PlainTextFormat generates simple unstructured log messages.
type PlainTextFormat struct {
	pools *AttributePools
}

// NewPlainTextFormat creates a plain text format generator.
func NewPlainTextFormat(pools *AttributePools) *PlainTextFormat {
	return &PlainTextFormat{pools: pools}
}

func (f *PlainTextFormat) Generate(rng *rand.Rand) ([]byte, map[string]string) {
	messages := []string{
		"starting worker pool",
		"connection failed",
		"shutting down gracefully",
		"waiting for pending requests",
		"configuration reloaded",
		"health check passed",
		"memory pressure detected",
		"disk space low",
		"certificate expires soon",
		"rate limit exceeded",
		"connection pool exhausted",
		"cache warmed up",
		"leader election completed",
		"follower synced",
		"snapshot created",
		"compaction started",
		"index rebuilt",
		"migration completed",
		"backup finished",
		"restore in progress",
	}

	msg := pick(rng, messages)

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"host":    pick(rng, f.pools.Hosts),
	}

	return []byte(msg), attrs
}

// KeyValueFormat generates structured key=value log lines.
type KeyValueFormat struct {
	pools *AttributePools
}

// NewKeyValueFormat creates a key-value format generator.
func NewKeyValueFormat(pools *AttributePools) *KeyValueFormat {
	return &KeyValueFormat{pools: pools}
}

func (f *KeyValueFormat) Generate(rng *rand.Rand) ([]byte, map[string]string) {
	levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	messages := []string{
		"request completed",
		"database query executed",
		"cache lookup",
		"authentication attempt",
		"authorization check",
		"file uploaded",
		"email sent",
		"webhook delivered",
		"task queued",
		"job processed",
		"metric recorded",
		"event published",
		"message consumed",
		"transaction committed",
		"session created",
	}
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths := []string{"/api/users", "/api/orders", "/api/products", "/health", "/metrics", "/api/auth/login", "/api/search"}
	userAgents := []string{"Mozilla/5.0", "curl/7.68.0", "Go-http-client/1.1", "python-requests/2.25.1", "okhttp/4.9.0"}

	level := pick(rng, levels)
	msg := pick(rng, messages)

	var line string
	switch rng.IntN(4) {
	case 0:
		// HTTP request style
		line = fmt.Sprintf(`level=%s msg=%q method=%s path=%s status=%d latency_ms=%d`,
			level, msg, pick(rng, methods), pick(rng, paths), 200+rng.IntN(300), rng.IntN(500))
	case 1:
		// Database query style
		line = fmt.Sprintf(`level=%s msg=%q table=%s rows=%d duration_ms=%d cached=%t`,
			level, msg, pick(rng, []string{"users", "orders", "products", "sessions", "events"}),
			rng.IntN(1000), rng.IntN(100), rng.IntN(2) == 1)
	case 2:
		// User action style
		line = fmt.Sprintf(`level=%s msg=%q user_id=%d action=%s ip=%s user_agent=%q`,
			level, msg, rng.IntN(100000),
			pick(rng, []string{"login", "logout", "view", "edit", "delete", "create"}),
			fmt.Sprintf("10.%d.%d.%d", rng.IntN(256), rng.IntN(256), rng.IntN(256)),
			pick(rng, userAgents))
	default:
		// Generic with trace context
		line = fmt.Sprintf(`level=%s msg=%q trace_id=%016x span_id=%08x duration_ms=%d`,
			level, msg, rng.Uint64(), rng.Uint32(), rng.IntN(1000))
	}

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"env":     pick(rng, f.pools.Envs),
		"host":    pick(rng, f.pools.Hosts),
	}

	return []byte(line), attrs
}

// JSONFormat generates JSON-structured log messages.
type JSONFormat struct {
	pools *AttributePools
}

// NewJSONFormat creates a JSON format generator.
func NewJSONFormat(pools *AttributePools) *JSONFormat {
	return &JSONFormat{pools: pools}
}

func (f *JSONFormat) Generate(rng *rand.Rand) ([]byte, map[string]string) {
	levels := []string{"debug", "info", "warn", "error"}
	messages := []string{
		"request handled",
		"database connection established",
		"cache invalidated",
		"user session expired",
		"rate limit applied",
		"circuit breaker opened",
		"retry succeeded",
		"fallback activated",
		"feature flag evaluated",
		"A/B test enrolled",
	}

	level := pick(rng, levels)
	msg := pick(rng, messages)

	// Build JSON object with varied fields
	obj := map[string]any{
		"level": level,
		"msg":   msg,
		"ts":    time.Now().UnixMilli(),
	}

	switch rng.IntN(5) {
	case 0:
		// HTTP metrics
		obj["method"] = pick(rng, []string{"GET", "POST", "PUT", "DELETE"})
		obj["path"] = pick(rng, []string{"/api/v1/users", "/api/v1/orders", "/graphql", "/ws"})
		obj["status"] = 200 + rng.IntN(300)
		obj["latency_ms"] = rng.Float64() * 500
		obj["bytes_in"] = rng.IntN(10000)
		obj["bytes_out"] = rng.IntN(100000)
	case 1:
		// Error details
		obj["error"] = pick(rng, []string{"connection refused", "timeout", "invalid input", "not found", "permission denied"})
		obj["stack"] = pick(rng, []string{"main.go:42", "handler.go:156", "service.go:89"})
		obj["retry_count"] = rng.IntN(5)
	case 2:
		// Business event
		obj["event_type"] = pick(rng, []string{"order.created", "payment.processed", "user.registered", "item.shipped"})
		obj["entity_id"] = fmt.Sprintf("%08x", rng.Uint32())
		obj["amount"] = rng.Float64() * 1000
		obj["currency"] = pick(rng, []string{"USD", "EUR", "GBP"})
	case 3:
		// System metrics
		obj["cpu_percent"] = rng.Float64() * 100
		obj["mem_mb"] = rng.IntN(8192)
		obj["goroutines"] = rng.IntN(1000)
		obj["gc_pause_ms"] = rng.Float64() * 10
	default:
		// Distributed tracing
		obj["trace_id"] = fmt.Sprintf("%032x", rng.Uint64())
		obj["span_id"] = fmt.Sprintf("%016x", rng.Uint64())
		obj["parent_id"] = fmt.Sprintf("%016x", rng.Uint64())
		obj["duration_us"] = rng.IntN(100000)
	}

	data, _ := json.Marshal(obj)

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"env":     pick(rng, f.pools.Envs),
		"host":    pick(rng, f.pools.Hosts),
	}

	return data, attrs
}

// AccessLogFormat generates Apache/Nginx-style access logs.
type AccessLogFormat struct {
	pools *AttributePools
}

// NewAccessLogFormat creates an access log format generator.
func NewAccessLogFormat(pools *AttributePools) *AccessLogFormat {
	return &AccessLogFormat{pools: pools}
}

func (f *AccessLogFormat) Generate(rng *rand.Rand) ([]byte, map[string]string) {
	methods := []string{"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"}
	paths := []string{
		"/",
		"/index.html",
		"/api/users",
		"/api/users/123",
		"/api/products",
		"/api/orders/456/items",
		"/static/js/app.js",
		"/static/css/style.css",
		"/images/logo.png",
		"/favicon.ico",
		"/robots.txt",
		"/sitemap.xml",
		"/health",
		"/metrics",
		"/graphql",
		"/ws/connect",
	}
	protocols := []string{"HTTP/1.0", "HTTP/1.1", "HTTP/2.0"}
	statuses := []int{200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 500, 502, 503}
	userAgents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
		"curl/7.68.0",
		"Go-http-client/1.1",
		"python-requests/2.28.0",
		"Googlebot/2.1 (+http://www.google.com/bot.html)",
		"Bingbot/2.0 (+http://www.bing.com/bingbot.htm)",
	}
	referers := []string{
		"-",
		"https://www.google.com/",
		"https://example.com/",
		"https://example.com/products",
		"https://twitter.com/",
	}

	ip := fmt.Sprintf("%d.%d.%d.%d", rng.IntN(256), rng.IntN(256), rng.IntN(256), rng.IntN(256))
	user := "-"
	if rng.IntN(10) == 0 {
		user = fmt.Sprintf("user%d", rng.IntN(100))
	}

	method := pick(rng, methods)
	path := pick(rng, paths)
	protocol := pick(rng, protocols)
	status := pick(rng, statuses)
	size := rng.IntN(100000)
	referer := pick(rng, referers)
	ua := pick(rng, userAgents)

	// Combined log format
	ts := time.Now().Format("02/Jan/2006:15:04:05 -0700")
	line := fmt.Sprintf(`%s - %s [%s] "%s %s %s" %d %d "%s" "%s"`,
		ip, user, ts, method, path, protocol, status, size, referer, ua)

	attrs := map[string]string{
		"service": "nginx",
		"vhost":   pick(rng, f.pools.VHosts),
		"host":    pick(rng, f.pools.Hosts),
	}

	return []byte(line), attrs
}

// SyslogFormat generates RFC 3164-style syslog messages.
type SyslogFormat struct {
	pools *AttributePools
}

// NewSyslogFormat creates a syslog format generator.
func NewSyslogFormat(pools *AttributePools) *SyslogFormat {
	return &SyslogFormat{pools: pools}
}

func (f *SyslogFormat) Generate(rng *rand.Rand) ([]byte, map[string]string) {
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

	ts := time.Now().Format("Jan  2 15:04:05")
	line := fmt.Sprintf("<%d>%s %s %s[%d]: %s", priority, ts, host, program.name, pid, message)

	attrs := map[string]string{
		"service":  program.name,
		"facility": facility.name,
		"host":     host,
	}

	return []byte(line), attrs
}

// WeirdFormat generates random/malformed data to stress tokenization.
type WeirdFormat struct {
	pools *AttributePools
}

// NewWeirdFormat creates a weird format generator.
func NewWeirdFormat(pools *AttributePools) *WeirdFormat {
	return &WeirdFormat{pools: pools}
}

func (f *WeirdFormat) Generate(rng *rand.Rand) ([]byte, map[string]string) {
	var data []byte

	switch rng.IntN(8) {
	case 0:
		// Random bytes
		data = make([]byte, 50+rng.IntN(200))
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
	case 1:
		// Control characters mixed with text
		text := "normal log message with\x00null\x07bell\x1bescape\x0bnewlines"
		data = []byte(text)
	case 2:
		// High-bit / UTF-8 edge cases
		samples := []string{
			"日本語ログメッセージ with mixed 文字",
			"Ошибка подключения к серверу",
			"🔥 Error: something went wrong 💥",
			"café résumé naïve",
			"\xc0\xc1\xfe\xff invalid UTF-8 sequences",
		}
		data = []byte(pick(rng, samples))
	case 3:
		// Very long tokens
		data = make([]byte, 1000+rng.IntN(1000))
		for i := range data {
			data[i] = 'a' + byte(rng.IntN(26))
		}
	case 4:
		// Repeated patterns
		pattern := pick(rng, []string{"AAAA", "abab", "123123", "....", "====", "----"})
		count := 10 + rng.IntN(50)
		for i := 0; i < count; i++ {
			data = append(data, pattern...)
		}
	case 5:
		// Empty or whitespace
		spaces := []string{"", " ", "\t", "\n", "   \t\n   ", "\r\n"}
		data = []byte(pick(rng, spaces))
	case 6:
		// JSON-like but malformed
		malformed := []string{
			`{"key": "value"`,
			`{key: value}`,
			`{"nested": {"deep": {"broken":`,
			`["array", "without", "end"`,
			`{"escape": "bad \q escape"}`,
		}
		data = []byte(pick(rng, malformed))
	default:
		// Mixed binary and text
		data = []byte("START")
		for i := 0; i < 20; i++ {
			if rng.IntN(2) == 0 {
				data = append(data, byte(rng.IntN(32))) // control char
			} else {
				data = append(data, byte('A'+rng.IntN(26)))
			}
		}
		data = append(data, []byte("END")...)
	}

	attrs := map[string]string{
		"service": "unknown",
		"host":    pick(rng, f.pools.Hosts),
	}

	return data, attrs
}
