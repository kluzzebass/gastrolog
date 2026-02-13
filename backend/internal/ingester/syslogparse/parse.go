// Package syslogparse provides shared syslog message parsing for RFC 3164 and RFC 5424.
// Used by the syslog and RELP ingesters.
package syslogparse

import (
	"strconv"
	"time"
)

// ParseMessage parses a syslog message and extracts attributes.
// Auto-detects RFC 3164 vs RFC 5424 format.
// Returns the extracted attributes and the source timestamp (zero if unparseable).
func ParseMessage(data []byte, remoteIP string) (attrs map[string]string, sourceTS time.Time) {
	attrs = make(map[string]string, 8)
	if remoteIP != "" {
		attrs["remote_ip"] = remoteIP
	}

	// Parse priority if present.
	if len(data) > 0 && data[0] == '<' {
		pri, rest, ok := ParsePriority(data)
		if ok {
			facility := pri / 8
			severity := pri % 8
			attrs["facility"] = strconv.Itoa(facility)
			attrs["severity"] = strconv.Itoa(severity)
			attrs["facility_name"] = FacilityName(facility)
			attrs["severity_name"] = SeverityName(severity)
			data = rest
		}
	}

	// Detect RFC 5424 vs RFC 3164 by looking for version number.
	if len(data) > 2 && data[0] >= '1' && data[0] <= '9' && data[1] == ' ' {
		// RFC 5424: version followed by space.
		sourceTS = ParseRFC5424(data, attrs)
	} else {
		// RFC 3164 (BSD) format.
		sourceTS = ParseRFC3164(data, attrs)
	}

	return attrs, sourceTS
}

// ParsePriority extracts the priority value from <PRI>.
func ParsePriority(data []byte) (int, []byte, bool) {
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

// ParseRFC3164 parses BSD syslog format.
// Format: MMM DD HH:MM:SS HOSTNAME TAG: MESSAGE
// Returns the parsed timestamp (zero if parsing fails).
// Note: RFC 3164 timestamps have no year, so we use the current year.
func ParseRFC3164(data []byte, attrs map[string]string) time.Time {
	var sourceTS time.Time

	// Try to parse timestamp: "Jan  2 15:04:05" or "Jan 02 15:04:05"
	if len(data) < 15 {
		return sourceTS
	}

	// Parse timestamp (first 15 characters).
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

// ParseRFC5424 parses IETF syslog format.
// Format: VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [SD] MESSAGE
// Returns the parsed timestamp (zero if parsing fails).
func ParseRFC5424(data []byte, attrs map[string]string) time.Time {
	var sourceTS time.Time

	fields := SplitFields(data, 7)
	if len(fields) < 1 {
		return sourceTS
	}

	// VERSION (already verified as digit)
	attrs["version"] = string(fields[0])

	// TIMESTAMP
	if len(fields) > 1 && string(fields[1]) != "-" {
		tsStr := string(fields[1])
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

	return sourceTS
}

// SplitFields splits data into up to n space-delimited fields.
func SplitFields(data []byte, n int) [][]byte {
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

// FacilityName returns the human-readable facility name.
func FacilityName(f int) string {
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

// SeverityName returns the human-readable severity name.
func SeverityName(s int) string {
	names := []string{
		"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
	}
	if s >= 0 && s < len(names) {
		return names[s]
	}
	return "unknown"
}
