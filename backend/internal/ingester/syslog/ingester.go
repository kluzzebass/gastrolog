// Package syslog provides a syslog ingester that accepts messages via UDP and TCP.
package syslog

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// Ingester accepts syslog messages via UDP and/or TCP.
// It implements orchestrator.Ingester.
//
// Supports both RFC 3164 (BSD) and RFC 5424 (IETF) formats with auto-detection.
// Messages are parsed and relevant fields extracted into attributes.
type Ingester struct {
	udpAddr string
	tcpAddr string
	out     chan<- orchestrator.IngestMessage
	logger  *slog.Logger

	mu          sync.Mutex
	udpConn     *net.UDPConn
	tcpListener net.Listener
}

// Config holds syslog ingester configuration.
type Config struct {
	// UDPAddr is the UDP address to listen on (e.g., ":514").
	// Empty string disables UDP.
	UDPAddr string

	// TCPAddr is the TCP address to listen on (e.g., ":514").
	// Empty string disables TCP.
	TCPAddr string

	// Logger for structured logging.
	Logger *slog.Logger
}

// New creates a new syslog ingester.
func New(cfg Config) *Ingester {
	return &Ingester{
		udpAddr: cfg.UDPAddr,
		tcpAddr: cfg.TCPAddr,
		logger:  logging.Default(cfg.Logger).With("component", "ingester", "type", "syslog"),
	}
}

// Run starts the syslog listeners and blocks until ctx is cancelled.
func (r *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	r.out = out

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Start UDP listener if configured.
	if r.udpAddr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.runUDP(ctx); err != nil {
				errCh <- err
			}
		}()
	}

	// Start TCP listener if configured.
	if r.tcpAddr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.runTCP(ctx); err != nil {
				errCh <- err
			}
		}()
	}

	if r.udpAddr == "" && r.tcpAddr == "" {
		return errors.New("syslog ingester: no UDP or TCP address configured")
	}

	// Wait for context cancellation or error.
	select {
	case <-ctx.Done():
		r.logger.Info("syslog ingester stopping")
		r.shutdown()
		wg.Wait()
		return nil
	case err := <-errCh:
		r.logger.Info("syslog ingester stopping", "error", err)
		r.shutdown()
		wg.Wait()
		return err
	}
}

// shutdown closes all listeners.
func (r *Ingester) shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.udpConn != nil {
		r.udpConn.Close()
		r.udpConn = nil
	}
	if r.tcpListener != nil {
		r.tcpListener.Close()
		r.tcpListener = nil
	}
}

// runUDP handles UDP syslog messages.
func (r *Ingester) runUDP(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", r.udpAddr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.udpConn = conn
	r.mu.Unlock()

	r.logger.Info("syslog UDP listener starting", "addr", conn.LocalAddr().String())

	buf := make([]byte, 65536) // Max UDP packet size
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Set read deadline to allow checking context.
		conn.SetReadDeadline(time.Now().Add(time.Second))

		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			r.logger.Warn("UDP read error", "error", err)
			continue
		}

		if n == 0 {
			continue
		}

		msg := r.parseMessage(buf[:n], remoteAddr.IP.String())
		select {
		case r.out <- msg:
		case <-ctx.Done():
			return nil
		}
	}
}

// runTCP handles TCP syslog connections.
func (r *Ingester) runTCP(ctx context.Context) error {
	listener, err := net.Listen("tcp", r.tcpAddr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.tcpListener = listener
	r.mu.Unlock()

	r.logger.Info("syslog TCP listener starting", "addr", listener.Addr().String())

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		default:
		}

		// Set accept deadline to allow checking context.
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))

		conn, err := listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			r.logger.Warn("TCP accept error", "error", err)
			continue
		}

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			r.handleTCPConn(ctx, conn)
		}(conn)
	}
}

// handleTCPConn handles a single TCP connection.
// TCP syslog uses either newline-delimited or octet-counted framing.
func (r *Ingester) handleTCPConn(ctx context.Context, conn net.Conn) {
	remoteIP := ""
	if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = tcpAddr.IP.String()
	}

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set read deadline.
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Try to detect framing: octet-counted starts with a digit.
		firstByte, err := reader.Peek(1)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
					r.logger.Debug("TCP read error", "error", err)
				}
			}
			return
		}

		var line []byte
		if firstByte[0] >= '0' && firstByte[0] <= '9' {
			// Octet-counted framing: "123 <message>"
			line, err = r.readOctetCounted(reader)
		} else {
			// Newline-delimited framing.
			line, err = reader.ReadBytes('\n')
			if err == nil && len(line) > 0 && line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
			}
		}

		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				r.logger.Debug("TCP read error", "error", err)
			}
			return
		}

		if len(line) == 0 {
			continue
		}

		msg := r.parseMessage(line, remoteIP)
		select {
		case r.out <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// readOctetCounted reads an octet-counted syslog message.
// Format: "123 <message>" where 123 is the length of <message>.
func (r *Ingester) readOctetCounted(reader *bufio.Reader) ([]byte, error) {
	// Read the length prefix.
	var length int
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == ' ' {
			break
		}
		if b < '0' || b > '9' {
			return nil, errors.New("invalid octet count")
		}
		length = length*10 + int(b-'0')
		if length > 1<<20 { // 1MB sanity limit
			return nil, errors.New("octet count too large")
		}
	}

	// Read the message.
	msg := make([]byte, length)
	_, err := io.ReadFull(reader, msg)
	return msg, err
}

// UDPAddr returns the UDP listener address. Only valid after Run() has started.
func (r *Ingester) UDPAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.udpConn == nil {
		return nil
	}
	return r.udpConn.LocalAddr()
}

// TCPAddr returns the TCP listener address. Only valid after Run() has started.
func (r *Ingester) TCPAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tcpListener == nil {
		return nil
	}
	return r.tcpListener.Addr()
}

// parseMessage parses a syslog message and extracts attributes.
// Auto-detects RFC 3164 vs RFC 5424 format.
func (r *Ingester) parseMessage(data []byte, remoteIP string) orchestrator.IngestMessage {
	attrs := make(map[string]string, 8)
	if remoteIP != "" {
		attrs["remote_ip"] = remoteIP
	}

	var sourceTS time.Time

	// Parse priority if present.
	raw := data
	if len(data) > 0 && data[0] == '<' {
		pri, rest, ok := parsePriority(data)
		if ok {
			facility := pri / 8
			severity := pri % 8
			attrs["facility"] = strconv.Itoa(facility)
			attrs["severity"] = strconv.Itoa(severity)
			attrs["facility_name"] = facilityName(facility)
			attrs["severity_name"] = severityName(severity)
			data = rest
		}
	}

	// Detect RFC 5424 vs RFC 3164 by looking for version number.
	if len(data) > 2 && data[0] >= '1' && data[0] <= '9' && data[1] == ' ' {
		// RFC 5424: version followed by space.
		sourceTS = r.parseRFC5424(data, attrs)
	} else {
		// RFC 3164 (BSD) format.
		sourceTS = r.parseRFC3164(data, attrs)
	}

	return orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      raw,
		SourceTS: sourceTS,
		IngestTS: time.Now(),
	}
}

// parsePriority extracts the priority value from <PRI>.
func parsePriority(data []byte) (int, []byte, bool) {
	if len(data) < 3 || data[0] != '<' {
		return 0, data, false
	}

	end := 1
	for end < len(data) && end < 5 && data[end] != '>' {
		end++
	}

	if end >= len(data) || data[end] != '>' {
		return 0, data, false
	}

	pri, err := strconv.Atoi(string(data[1:end]))
	if err != nil || pri < 0 || pri > 191 {
		return 0, data, false
	}

	return pri, data[end+1:], true
}

// parseRFC3164 parses BSD syslog format.
// Format: MMM DD HH:MM:SS HOSTNAME TAG: MESSAGE
// Returns the parsed timestamp (zero if parsing fails).
// Note: RFC 3164 timestamps have no year, so we use the current year.
func (r *Ingester) parseRFC3164(data []byte, attrs map[string]string) time.Time {
	var sourceTS time.Time

	// Try to parse timestamp: "Jan  2 15:04:05" or "Jan 02 15:04:05"
	if len(data) < 15 {
		return sourceTS
	}

	// Parse timestamp (first 15 characters).
	// Format: "Jan  2 15:04:05" or "Jan 02 15:04:05"
	tsStr := string(data[:15])
	now := time.Now()

	// Try both formats (single-digit day with space, double-digit day).
	if ts, err := time.Parse("Jan  2 15:04:05", tsStr); err == nil {
		sourceTS = ts.AddDate(now.Year(), 0, 0)
		// Handle year rollover: if parsed time is in the future, use previous year.
		if sourceTS.After(now.Add(24 * time.Hour)) {
			sourceTS = sourceTS.AddDate(-1, 0, 0)
		}
	} else if ts, err := time.Parse("Jan 02 15:04:05", tsStr); err == nil {
		sourceTS = ts.AddDate(now.Year(), 0, 0)
		if sourceTS.After(now.Add(24 * time.Hour)) {
			sourceTS = sourceTS.AddDate(-1, 0, 0)
		}
	}

	// Find first space after timestamp area.
	pos := 15
	for pos < len(data) && data[pos] == ' ' {
		pos++
	}

	// Find hostname (next space-delimited token).
	start := pos
	for pos < len(data) && data[pos] != ' ' && data[pos] != ':' {
		pos++
	}
	if pos > start {
		hostname := string(data[start:pos])
		if len(hostname) <= 64 {
			attrs["hostname"] = hostname
		}
	}

	// Skip space.
	for pos < len(data) && data[pos] == ' ' {
		pos++
	}

	// Find tag (ends with : or [).
	start = pos
	for pos < len(data) && data[pos] != ':' && data[pos] != '[' && data[pos] != ' ' {
		pos++
	}
	if pos > start {
		tag := string(data[start:pos])
		if len(tag) <= 64 {
			attrs["app_name"] = tag
		}
	}

	// Look for PID in brackets.
	if pos < len(data) && data[pos] == '[' {
		pos++
		pidStart := pos
		for pos < len(data) && data[pos] != ']' {
			pos++
		}
		if pos > pidStart && pos < len(data) {
			pid := string(data[pidStart:pos])
			if len(pid) <= 16 {
				attrs["proc_id"] = pid
			}
		}
	}

	return sourceTS
}

// parseRFC5424 parses IETF syslog format.
// Format: VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [SD] MESSAGE
// Returns the parsed timestamp (zero if parsing fails).
// RFC 5424 timestamps are ISO 8601 format: 2003-10-11T22:14:15.003Z or 2003-10-11T22:14:15.003-07:00
func (r *Ingester) parseRFC5424(data []byte, attrs map[string]string) time.Time {
	var sourceTS time.Time

	fields := splitFields(data, 7)
	if len(fields) < 1 {
		return sourceTS
	}

	// VERSION (already verified as digit)
	attrs["version"] = string(fields[0])

	// TIMESTAMP
	if len(fields) > 1 && string(fields[1]) != "-" {
		tsStr := string(fields[1])
		// Try RFC 3339 (ISO 8601) formats.
		if ts, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
			sourceTS = ts
		} else if ts, err := time.Parse(time.RFC3339, tsStr); err == nil {
			sourceTS = ts
		}
	}

	// HOSTNAME
	if len(fields) > 2 && string(fields[2]) != "-" && len(fields[2]) <= 64 {
		attrs["hostname"] = string(fields[2])
	}

	// APP-NAME
	if len(fields) > 3 && string(fields[3]) != "-" && len(fields[3]) <= 64 {
		attrs["app_name"] = string(fields[3])
	}

	// PROCID
	if len(fields) > 4 && string(fields[4]) != "-" && len(fields[4]) <= 16 {
		attrs["proc_id"] = string(fields[4])
	}

	// MSGID
	if len(fields) > 5 && string(fields[5]) != "-" && len(fields[5]) <= 64 {
		attrs["msg_id"] = string(fields[5])
	}

	// STRUCTURED-DATA and MESSAGE are in fields[6] if present.
	// We don't parse structured data into attrs to avoid the injection issue.

	return sourceTS
}

// splitFields splits data into up to n space-delimited fields.
func splitFields(data []byte, n int) [][]byte {
	var fields [][]byte
	pos := 0
	for len(fields) < n && pos < len(data) {
		// Skip leading spaces.
		for pos < len(data) && data[pos] == ' ' {
			pos++
		}
		if pos >= len(data) {
			break
		}

		// Find end of field.
		start := pos
		if len(fields) == n-1 {
			// Last field gets the rest.
			fields = append(fields, data[start:])
			break
		}
		for pos < len(data) && data[pos] != ' ' {
			pos++
		}
		fields = append(fields, data[start:pos])
	}
	return fields
}

// facilityName returns the human-readable facility name.
func facilityName(f int) string {
	names := []string{
		"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
		"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "alert", "clock",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
	}
	if f >= 0 && f < len(names) {
		return names[f]
	}
	return "unknown"
}

// severityName returns the human-readable severity name.
func severityName(s int) string {
	names := []string{
		"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
	}
	if s >= 0 && s < len(names) {
		return names[s]
	}
	return "unknown"
}
