package syslog

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// TestConnection verifies that the configured UDP/TCP addresses are available
// to bind. Returns a human-readable summary on success.
func TestConnection(params map[string]string) (string, error) {
	udpAddr := params["udp_addr"]
	tcpAddr := params["tcp_addr"]

	if udpAddr == "" && tcpAddr == "" {
		return "", errors.New("at least one of udp_addr or tcp_addr is required")
	}

	var verified []string

	if udpAddr != "" {
		pc, err := net.ListenPacket("udp", udpAddr)
		if err != nil {
			return "", fmt.Errorf("UDP %s: %w", udpAddr, err)
		}
		_ = pc.Close()
		verified = append(verified, "UDP "+udpAddr)
	}

	if tcpAddr != "" {
		ln, err := net.Listen("tcp", tcpAddr)
		if err != nil {
			return "", fmt.Errorf("TCP %s: %w", tcpAddr, err)
		}
		_ = ln.Close()
		verified = append(verified, "TCP "+tcpAddr)
	}

	return fmt.Sprintf("OK — %s available", strings.Join(verified, ", ")), nil
}
