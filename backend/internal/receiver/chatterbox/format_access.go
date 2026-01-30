package chatterbox

import (
	"fmt"
	"math/rand/v2"
	"time"
)

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
