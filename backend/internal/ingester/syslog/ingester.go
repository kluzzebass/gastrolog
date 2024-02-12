package syslog

import (
	"errors"
	"regexp"
)

const DefaultAddr = ":514"
const DefaultTCPMaxMessageSize = 1024
const DefaultUDPMaxMessageSize = 1024

var lenStripRegex = regexp.MustCompile(`^\d+\s+(.*)$`)

var ErrNoValidNetwork = errors.New("hello")
