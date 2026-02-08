package tokenizer

// ExtractAccessLog parses a log message as an Apache/Nginx Common Log Format
// (CLF) or Combined Log Format line and extracts named fields as key=value pairs.
//
// Common Log Format:
//
//	127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326
//
// Combined Log Format (adds referer and user agent):
//
//	127.0.0.1 - frank [...] "GET /index.html HTTP/1.1" 200 2326 "http://example.com" "Mozilla/5.0"
//
// Returns nil if the message does not match the access log pattern.
func ExtractAccessLog(msg []byte) []KeyValue {
	if len(msg) == 0 {
		return nil
	}

	p := accessLogParser{data: msg}

	// remote_host (IP or hostname)
	remoteHost := p.readField()
	if remoteHost == "" {
		return nil
	}

	// ident (usually "-")
	_ = p.readField()
	if p.err {
		return nil
	}

	// remote_user
	remoteUser := p.readField()
	if p.err {
		return nil
	}

	// [timestamp] — enclosed in brackets
	if !p.expect('[') {
		return nil
	}
	_ = p.readUntil(']')
	if p.err {
		return nil
	}

	// "request line" — enclosed in double quotes
	if !p.skipSpace().expect('"') {
		return nil
	}
	requestLine := p.readUntil('"')
	if p.err {
		return nil
	}

	// Parse request line: METHOD PATH PROTOCOL
	method, path, protocol := parseRequestLine(requestLine)
	if method == "" {
		return nil
	}

	// status code
	status := p.readField()
	if p.err || status == "" {
		return nil
	}
	// Validate status is numeric.
	if !isNumericString(status) {
		return nil
	}

	// body_bytes
	bodyBytes := p.readField()
	if p.err {
		return nil
	}

	var result []KeyValue
	addAccessLogField(&result, "remote_host", remoteHost)
	if remoteUser != "-" && remoteUser != "" {
		addAccessLogField(&result, "remote_user", remoteUser)
	}
	addAccessLogField(&result, "method", method)
	addAccessLogField(&result, "path", path)
	if protocol != "" {
		addAccessLogField(&result, "protocol", protocol)
	}
	addAccessLogField(&result, "status", status)
	if bodyBytes != "-" && bodyBytes != "0" && bodyBytes != "" {
		addAccessLogField(&result, "body_bytes", bodyBytes)
	}

	// Combined Log Format: referer and user_agent in quotes.
	if p.skipSpace().tryExpect('"') {
		referer := p.readUntil('"')
		if !p.err && referer != "-" && referer != "" {
			addAccessLogField(&result, "referer", referer)
		}

		if p.skipSpace().tryExpect('"') {
			userAgent := p.readUntil('"')
			if !p.err && userAgent != "" {
				addAccessLogField(&result, "user_agent", userAgent)
			}
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func addAccessLogField(result *[]KeyValue, key, value string) {
	if len(value) > MaxValueLength {
		return
	}
	*result = append(*result, KeyValue{
		Key:   key,
		Value: ToLowerASCII([]byte(value)),
	})
}

// accessLogParser is a simple stateful parser for access log lines.
type accessLogParser struct {
	data []byte
	pos  int
	err  bool
}

// readField reads a space-delimited token.
func (p *accessLogParser) readField() string {
	p.skipSpaceInPlace()
	if p.pos >= len(p.data) {
		p.err = true
		return ""
	}

	start := p.pos
	for p.pos < len(p.data) && p.data[p.pos] != ' ' && p.data[p.pos] != '\t' {
		p.pos++
	}
	return string(p.data[start:p.pos])
}

// readUntil reads bytes until the delimiter is found (exclusive).
// The delimiter is consumed.
func (p *accessLogParser) readUntil(delim byte) string {
	start := p.pos
	for p.pos < len(p.data) && p.data[p.pos] != delim {
		p.pos++
	}
	if p.pos >= len(p.data) {
		p.err = true
		return string(p.data[start:])
	}
	result := string(p.data[start:p.pos])
	p.pos++ // skip delimiter
	return result
}

// expect checks if the next non-space byte is the expected byte and consumes it.
func (p *accessLogParser) expect(b byte) bool {
	p.skipSpaceInPlace()
	if p.pos >= len(p.data) || p.data[p.pos] != b {
		p.err = true
		return false
	}
	p.pos++
	return true
}

// tryExpect is like expect but doesn't set error on failure.
func (p *accessLogParser) tryExpect(b byte) bool {
	if p.pos >= len(p.data) || p.data[p.pos] != b {
		return false
	}
	p.pos++
	return true
}

// skipSpace returns the parser for chaining.
func (p *accessLogParser) skipSpace() *accessLogParser {
	p.skipSpaceInPlace()
	return p
}

func (p *accessLogParser) skipSpaceInPlace() {
	for p.pos < len(p.data) && (p.data[p.pos] == ' ' || p.data[p.pos] == '\t') {
		p.pos++
	}
}

// parseRequestLine splits "GET /path HTTP/1.1" into method, path, protocol.
func parseRequestLine(line string) (method, path, protocol string) {
	// Find method.
	i := 0
	for i < len(line) && line[i] != ' ' {
		i++
	}
	method = line[:i]
	if i >= len(line) {
		return "", "", ""
	}

	// Skip space.
	for i < len(line) && line[i] == ' ' {
		i++
	}

	// Find path.
	pathStart := i
	for i < len(line) && line[i] != ' ' {
		i++
	}
	path = line[pathStart:i]

	// Skip space.
	for i < len(line) && line[i] == ' ' {
		i++
	}

	// Remaining is protocol (may be empty).
	if i < len(line) {
		protocol = line[i:]
	}

	if method == "" || path == "" {
		return "", "", ""
	}

	return method, path, protocol
}

func isNumericString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
