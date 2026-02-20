package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

// rateLimitedPaths is the set of RPC procedures subject to rate limiting.
var rateLimitedPaths = map[string]bool{
	gastrologv1connect.AuthServiceLoginProcedure:    true,
	gastrologv1connect.AuthServiceRegisterProcedure: true,
}

// ipLimiter tracks the rate limiter and last-seen time for a single IP.
type ipLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// rateLimiter tracks per-IP rate limiters for auth endpoints.
type rateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*ipLimiter
	rate     rate.Limit
	burst    int
}

func newRateLimiter(r rate.Limit, burst int) *rateLimiter {
	return &rateLimiter{
		limiters: make(map[string]*ipLimiter),
		rate:     r,
		burst:    burst,
	}
}

// getLimiter returns the rate.Limiter for the given IP, creating one if needed.
func (rl *rateLimiter) getLimiter(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry, ok := rl.limiters[ip]
	if !ok {
		entry = &ipLimiter{
			limiter: rate.NewLimiter(rl.rate, rl.burst),
		}
		rl.limiters[ip] = entry
	}
	entry.lastSeen = time.Now()
	return entry.limiter
}

// cleanup removes entries that haven't been seen for the given duration.
func (rl *rateLimiter) cleanup(staleAfter time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cutoff := time.Now().Add(-staleAfter)
	for ip, entry := range rl.limiters {
		if entry.lastSeen.Before(cutoff) {
			delete(rl.limiters, ip)
		}
	}
}

// startCleanup launches a background goroutine that periodically evicts stale
// entries. It stops when ctx is cancelled. The caller must call wg.Wait() to
// ensure the goroutine has exited.
func (rl *rateLimiter) startCleanup(ctx context.Context, wg *sync.WaitGroup, interval, staleAfter time.Duration) {
	wg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.cleanup(staleAfter)
			}
		}
	})
}

// connectError is the JSON shape of a Connect error response.
type connectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// rateLimitMiddleware returns HTTP middleware that rate-limits auth endpoints.
// It returns 429 with a Connect-compatible JSON error when the limit is exceeded.
func rateLimitMiddleware(rl *rateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !rateLimitedPaths[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			ip, _, _ := net.SplitHostPort(r.RemoteAddr)
			if ip == "" {
				ip = r.RemoteAddr
			}

			limiter := rl.getLimiter(ip)
			if !limiter.Allow() {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(connectError{
					Code:    "resource_exhausted",
					Message: "too many requests, try again later",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
