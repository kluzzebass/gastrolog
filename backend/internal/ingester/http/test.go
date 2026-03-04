package http

import (
	"cmp"
	"fmt"
	"net"
)

// TestConnection verifies that the configured HTTP address is available
// to bind. Returns a human-readable summary on success.
func TestConnection(params map[string]string) (string, error) {
	addr := cmp.Or(params["addr"], ":3100")

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("TCP %s: %w", addr, err)
	}
	_ = ln.Close()

	return fmt.Sprintf("OK — HTTP %s available", addr), nil
}
