package syslog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/muesli/cancelreader"
)

type (
	SyslogTCPIngesterOptions struct {
		Addr           *string
		IPv4           *bool
		IPv6           *bool
		MaxMessageSize *int
		TimeZone       *time.Location
	}

	SyslogTCPIngester struct {
		wg             sync.WaitGroup
		cancel         context.CancelFunc
		cancelled      bool
		addr           string
		lc             net.ListenConfig
		ln             net.Listener
		ipv4           bool
		ipv6           bool
		maxMessageSize int
		timeZone       *time.Location
		network        string
		parser         *parser
	}
)

func NewSyslogTCPIngester(ctx context.Context, options *SyslogTCPIngesterOptions) (*SyslogTCPIngester, error) {

	// create cancel context
	ctx, cancel := context.WithCancel(ctx)

	i := &SyslogTCPIngester{
		cancel: cancel,
	}

	// parse options
	if options == nil {
		i.addr = DefaultAddr
		i.ipv4 = true
		i.ipv6 = true
	} else {
		if options.Addr == nil {
			i.addr = DefaultAddr
		} else {
			i.addr = *options.Addr
		}

		if options.IPv4 == nil {
			i.ipv4 = true
		} else {
			i.ipv4 = *options.IPv4
		}

		if options.IPv6 == nil {
			i.ipv6 = true
		} else {
			i.ipv6 = *options.IPv6
		}

		if i.ipv4 && i.ipv6 {
			i.network = "tcp"
		} else if i.ipv4 && !i.ipv6 {
			i.network = "tcp4"
		} else if !i.ipv4 && i.ipv6 {
			i.network = "tcp6"
		} else {
			return nil, ErrNoValidNetwork
		}

		if options.MaxMessageSize != nil {
			i.maxMessageSize = *options.MaxMessageSize
		} else {
			i.maxMessageSize = DefaultTCPMaxMessageSize
		}

		if options.TimeZone != nil {
			i.timeZone = options.TimeZone
		} else {
			i.timeZone = time.Local
		}
	}

	// create parser
	i.parser = newParser(i.timeZone)

	ln, err := i.lc.Listen(ctx, i.network, i.addr)
	if err != nil {
		return nil, err
	}

	i.ln = ln

	go i.ingest(ctx)

	return i, nil
}

func (i *SyslogTCPIngester) Cancel() {
	if i.cancelled {
		return
	}
	i.cancelled = true
	slog.Debug("cancelling ingester")
	i.cancel()
	slog.Debug("waiting for ingester to finish", "wg", &i.wg)
	i.wg.Wait()
	slog.Debug("ingester finished")
	return
}

func (i *SyslogTCPIngester) ingest(ctx context.Context) {
	if i.cancelled {
		return
	}
	defer slog.Debug("ingest finished")
	defer i.wg.Done()
	i.wg.Add(1)

	for {
		// check for cancellation
		select {
		case <-ctx.Done():
			slog.Debug("ingester cancelled")
			return
		default:
		}

		// accept a connection
		conn, err := i.ln.Accept()
		if err != nil {
			slog.Debug("failed to accept connection", "err", err)
			continue
		}

		go i.handleConn(ctx, conn)
	}
}

func (i *SyslogTCPIngester) handleConn(ctx context.Context, conn net.Conn) {
	if i.cancelled {
		return
	}
	defer slog.Debug("handleConn finished", "remote", conn.RemoteAddr())
	defer i.wg.Done()
	i.wg.Add(1)

	slog.Debug("handling connection", "remote", conn.RemoteAddr())

	lchan := make(chan string)
	qchan := make(chan struct{})

	go i.lineReader(ctx, conn, lchan, qchan)

	for {
		// check for cancellation
		select {
		case <-ctx.Done():
			slog.Debug("handleConn cancelled")
			return
		case line := <-lchan:
			slog.Debug("received line", "line", line)

			// process the data
			i.parser.parseLine(line)
		case <-qchan:
			slog.Debug("connection closed")
			return
		}
	}

}

func (i *SyslogTCPIngester) lineReader(ctx context.Context, conn net.Conn, lchan chan string, qchan chan struct{}) {
	if i.cancelled {
		return
	}

	defer slog.Debug("lineReader finished", "remote", conn.RemoteAddr())
	defer conn.Close()
	defer close(lchan)
	defer close(qchan)
	defer i.wg.Done()
	i.wg.Add(1)

	slog.Debug("reading lines", "remote", conn.RemoteAddr())

	buf := []byte{}

	reader, err := cancelreader.NewReader(conn)
	if err != nil {
		slog.Warn("failed to create cancel reader", "err", err)
		return
	}

	for {
		// check for cancellation
		select {
		case <-ctx.Done():
			slog.Debug("lineReader cancelled")
			reader.Cancel()
			return
		default:
		}

		// read a chunk
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		chunk := make([]byte, i.maxMessageSize)
		n, err := reader.Read(chunk)
		if err != nil {
			if errors.Is(err, cancelreader.ErrCanceled) {
				slog.Debug("canceled!")
				qchan <- struct{}{}
				return
			}
			if errors.Is(err, io.EOF) {
				slog.Debug("received EOF")
				qchan <- struct{}{}
				return
			}
			if errors.Is(err, io.ErrClosedPipe) {
				slog.Debug("received closed pipe")
				qchan <- struct{}{}
				return
			}
			if err, ok := err.(net.Error); ok && err.Timeout() {
				slog.Debug("Timeout", "err", err)
				continue
			}
			slog.Warn("failed to read line", "err", err)
			qchan <- struct{}{}
			return
		}

		if n == 0 {
			continue
		}

		// append the chunk to the buffer
		buf = append(buf, chunk[:n]...)

		// find the newline
		nl := -1
		for i, b := range buf {
			if b == '\n' {
				nl = i
				break
			}
		}

		// if no newline found, continue reading
		if nl == -1 {
			continue
		}

		// extract the line
		line := string(buf[:nl])

		// shift the buffer left
		buf = buf[nl+1:]

		// send the line
		lchan <- line
	}
}
