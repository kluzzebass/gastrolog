package server

import (
	"net/http"
	"strings"
)

// Google Fonts origins. The frontend stylesheet opens with an absolute
// @import for the three Observatory typefaces, which Vite leaves in place, and
// that stylesheet in turn pulls its woff2 files from the static host. Drop
// either origin and the UI silently falls back to system fonts.
const (
	fontStylesheetOrigin = "https://fonts.googleapis.com"
	fontFileOrigin       = "https://fonts.gstatic.com"
)

// contentSecurityPolicy governs the embedded frontend the binary serves.
//
// script-src without 'unsafe-inline' is the load-bearing part: it makes inline
// event-handler attributes and javascript: URLs inert, so an injection into
// chart or log-derived markup cannot reach the operator's token. The Vite
// production build emits only external module scripts, so no allowance beyond
// 'self' is needed.
//
// style-src does allow 'unsafe-inline': ECharts tooltips and Mermaid's rendered
// SVG both carry style attributes in markup they insert themselves. img-src
// allows data: for the SVG textures the stylesheet embeds.
var contentSecurityPolicy = strings.Join([]string{
	"default-src 'self'",
	"script-src 'self'",
	"style-src 'self' 'unsafe-inline' " + fontStylesheetOrigin,
	"img-src 'self' data:",
	"font-src 'self' " + fontFileOrigin,
	"connect-src 'self'",
	"object-src 'none'",
	"base-uri 'self'",
	"form-action 'self'",
	"frame-ancestors 'none'",
}, "; ")

// securityHeadersMiddleware sets standard HTTP security headers on every response.
func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
		next.ServeHTTP(w, r)
	})
}
