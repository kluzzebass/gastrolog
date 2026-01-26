package memory

import (
	"errors"
	"sync"
)

var ErrClosed = errors.New("memory appender closed")

type Appender struct {
	mu     sync.Mutex
	buf    []byte
	closed bool
}

func NewAppender() *Appender {
	return &Appender{}
}

func (a *Appender) Append(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, ErrClosed
	}
	a.buf = append(a.buf, p...)
	return len(p), nil
}

func (a *Appender) Size() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return int64(len(a.buf))
}

func (a *Appender) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	return nil
}

func (a *Appender) Bytes() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]byte, len(a.buf))
	copy(out, a.buf)
	return out
}
