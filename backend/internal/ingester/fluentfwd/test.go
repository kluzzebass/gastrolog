package fluentfwd

import (
	"cmp"
	"fmt"
	"net"
)

// TestConnection verifies that the configured Fluent Forward address is
// available to bind. Returns a human-readable summary on success.
func TestConnection(params map[string]string) (string, error) {
	addr := cmp.Or(params["addr"], ":24224")

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("TCP %s: %w", addr, err)
	}
	_ = ln.Close()

	return fmt.Sprintf("OK — FluentFwd %s available", addr), nil
}
