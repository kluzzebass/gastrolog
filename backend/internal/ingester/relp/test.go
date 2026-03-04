package relp

import (
	"fmt"
	"net"
)

// TestConnection verifies that the configured RELP address is available
// to bind. Returns a human-readable summary on success.
func TestConnection(params map[string]string) (string, error) {
	addr := params["addr"]
	if addr == "" {
		addr = ":2514"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("TCP %s: %w", addr, err)
	}
	_ = ln.Close()

	return fmt.Sprintf("OK — RELP %s available", addr), nil
}
