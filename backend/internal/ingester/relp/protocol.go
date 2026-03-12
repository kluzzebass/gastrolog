// RELP (Reliable Event Logging Protocol) server implementation.
//
// RELP is a simple text-based protocol for reliable syslog transport over TCP.
// Each frame has the format:
//
//	TXNR SP COMMAND SP DATALEN [SP DATA] LF
//
// The session lifecycle is: open → N × syslog → close, with a "rsp" ACK
// after each command. This implementation handles the server side only.
//
// Spec: https://github.com/rsyslog/librelp/blob/master/doc/relp.html
package relp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Message represents a single RELP frame received from the client.
type Message struct {
	Txnr    int
	Command string
	Data    []byte
}

// Session manages the state of a single RELP connection. It handles the
// open/close handshake transparently; callers only see syslog messages.
type Session struct {
	r      *bufio.Reader
	w      io.Writer
	opened bool
}

// NewSession creates a RELP session over the given read/write streams.
// Typically fd is a net.Conn.
func NewSession(r io.Reader, w io.Writer) *Session {
	return &Session{
		r: bufio.NewReaderSize(r, 16384),
		w: w,
	}
}

// ReceiveLog returns the next syslog message from the RELP stream.
// On the first call it transparently handles the "open" handshake.
// Returns io.EOF on clean close or connection end.
func (s *Session) ReceiveLog() (*Message, error) {
	// Handle the open handshake on first call.
	if !s.opened {
		msg, err := s.readFrame()
		if err != nil {
			return nil, err
		}
		if msg.Command != "open" {
			return nil, fmt.Errorf("relp: expected open, got %q", msg.Command)
		}
		if err := s.answerOpen(msg); err != nil {
			return nil, err
		}
		s.opened = true
	}

	msg, err := s.readFrame()
	if err != nil {
		return nil, err
	}

	switch msg.Command {
	case "syslog":
		return msg, nil
	case "close":
		_ = s.AnswerOk(msg)
		return nil, io.EOF
	default:
		return nil, fmt.Errorf("relp: unexpected command %q", msg.Command)
	}
}

// AnswerOk sends a "200 Ok" response for the given message.
func (s *Session) AnswerOk(msg *Message) error {
	return s.sendResponse(msg.Txnr, "200 Ok")
}

// AnswerError sends a "500" error response for the given message.
func (s *Session) AnswerError(msg *Message, reason string) error {
	return s.sendResponse(msg.Txnr, "500 "+reason)
}

// readFrame reads a single RELP frame from the connection.
//
// Frame format: TXNR SP COMMAND SP DATALEN [SP DATA] LF
func (s *Session) readFrame() (*Message, error) {
	// Read TXNR.
	txnrStr, err := s.readToken()
	if err != nil {
		return nil, fmt.Errorf("relp: read txnr: %w", err)
	}
	txnr, err := strconv.Atoi(txnrStr)
	if err != nil {
		return nil, fmt.Errorf("relp: invalid txnr %q: %w", txnrStr, err)
	}

	// Read COMMAND.
	command, err := s.readToken()
	if err != nil {
		return nil, fmt.Errorf("relp: read command: %w", err)
	}

	// Read DATALEN. The delimiter is SP if datalen > 0, LF if datalen == 0.
	datalenStr, delim, err := s.readTokenOrLF()
	if err != nil {
		return nil, fmt.Errorf("relp: read datalen: %w", err)
	}
	datalen, err := strconv.Atoi(datalenStr)
	if err != nil {
		return nil, fmt.Errorf("relp: invalid datalen %q: %w", datalenStr, err)
	}

	// Read DATA (exactly datalen bytes) if present.
	var data []byte
	if datalen > 0 {
		if delim == '\n' {
			return nil, fmt.Errorf("relp: datalen %d but no data follows", datalen)
		}
		data = make([]byte, datalen)
		if _, err := io.ReadFull(s.r, data); err != nil {
			return nil, fmt.Errorf("relp: read data: %w", err)
		}
		// Consume trailing LF.
		if b, err := s.r.ReadByte(); err == nil && b != '\n' {
			_ = s.r.UnreadByte()
		}
	}
	// If datalen == 0, the LF was already consumed as the delimiter.

	return &Message{Txnr: txnr, Command: command, Data: data}, nil
}

// readToken reads bytes until a space delimiter and returns the token.
// The space is consumed but not included in the result.
func (s *Session) readToken() (string, error) {
	var buf []byte
	for {
		b, err := s.r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == ' ' {
			return string(buf), nil
		}
		buf = append(buf, b)
	}
}

// readTokenOrLF reads bytes until a space or LF delimiter.
// Returns the token and which delimiter was found.
func (s *Session) readTokenOrLF() (string, byte, error) {
	var buf []byte
	for {
		b, err := s.r.ReadByte()
		if err != nil {
			return "", 0, err
		}
		if b == ' ' || b == '\n' {
			return string(buf), b, nil
		}
		buf = append(buf, b)
	}
}

// answerOpen parses the client's offer and sends the server's response.
func (s *Session) answerOpen(msg *Message) error {
	offers := parseOffers(string(msg.Data))

	// Accept version 0 or 1.
	ver := offers["relp_version"]
	if ver != "" && ver != "0" && ver != "1" {
		_ = s.AnswerError(msg, "unsupported relp_version")
		return fmt.Errorf("relp: unsupported version %q", ver)
	}
	if ver == "" {
		ver = "0"
	}

	// Check that the client supports syslog.
	cmds := offers["commands"]
	if !strings.Contains(cmds, "syslog") {
		_ = s.AnswerError(msg, "syslog command required")
		return errors.New("relp: client does not support syslog command")
	}

	// Build accepted commands — we only support syslog.
	resp := fmt.Sprintf("200 ok\nrelp_version=%s\nrelp_software=gastrolog\ncommands=syslog", ver)
	return s.sendResponse(msg.Txnr, resp)
}

// parseOffers parses the open command's offer data.
// Format: key=value pairs separated by newlines.
func parseOffers(data string) map[string]string {
	out := make(map[string]string)
	for line := range strings.SplitSeq(data, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, _ := strings.Cut(line, "=")
		out[k] = v
	}
	return out
}

// sendResponse writes a RELP response frame.
// Format: TXNR rsp DATALEN SP DATA LF
func (s *Session) sendResponse(txnr int, data string) error {
	var frame []byte
	if len(data) == 0 {
		frame = fmt.Appendf(nil, "%d rsp 0\n", txnr)
	} else {
		frame = fmt.Appendf(nil, "%d rsp %d %s\n", txnr, len(data), data)
	}
	_, err := s.w.Write(frame)
	return err
}
