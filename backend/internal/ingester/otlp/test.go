package otlp

import (
	"cmp"
	"fmt"
	"net"
	"strings"
)

// TestConnection verifies that the configured OTLP HTTP and gRPC addresses
// are available to bind. Returns a human-readable summary on success.
func TestConnection(params map[string]string) (string, error) {
	httpAddr := cmp.Or(params["http_addr"], ":4318")
	grpcAddr := cmp.Or(params["grpc_addr"], ":4317")

	var verified []string

	for _, entry := range []struct{ label, addr string }{
		{"gRPC", grpcAddr},
		{"HTTP", httpAddr},
	} {
		ln, err := net.Listen("tcp", entry.addr)
		if err != nil {
			return "", fmt.Errorf("%s %s: %w", entry.label, entry.addr, err)
		}
		_ = ln.Close()
		verified = append(verified, entry.label+" "+entry.addr)
	}

	return fmt.Sprintf("OK — OTLP %s available", strings.Join(verified, ", ")), nil
}
