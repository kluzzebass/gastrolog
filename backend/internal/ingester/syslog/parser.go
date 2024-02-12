package syslog

import (
	"log/slog"
	"time"

	"github.com/influxdata/go-syslog/v3"
	rfc3164 "github.com/influxdata/go-syslog/v3/rfc3164"
	rfc5424 "github.com/influxdata/go-syslog/v3/rfc5424"
)

type (
	// Parser struct
	parser struct {
		timeZone *time.Location
		p3164    syslog.Machine
		p5424    syslog.Machine
	}
)

func newParser(loc *time.Location) *parser {

	p := &parser{}

	p.p3164 = rfc3164.NewParser(
		rfc3164.WithBestEffort(),
		rfc3164.WithYear(rfc3164.CurrentYear{}),
		rfc3164.WithTimezone(loc),
		rfc3164.WithRFC3339(),
	)

	p.p5424 = rfc5424.NewParser(
		rfc5424.WithBestEffort(),
	)

	return p
}

func (p *parser) parseLine(line string) {

	var msg syslog.Message
	var err error

	// try parsing as rfc3164 first
	msg, err = p.p3164.Parse([]byte(line))
	if err != nil {
		// get rid of potential prepending message size number
		matches := lenStripRegex.FindStringSubmatch(line)
		if len(matches) > 1 {
			line = matches[1]
		}

		// try parsing as rfc5424 second
		msg, err = p.p5424.Parse([]byte(line))
		if err != nil {
			slog.Warn("failed to parse message", "err", err, "data", string(line))
			return
		}
	}

	slog.Info("parsed message", "msg", msg)
}
