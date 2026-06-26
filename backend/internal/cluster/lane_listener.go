package cluster

import (
	"errors"
	"io"
	"net"
	"sync"

	"gastrolog/internal/multiraft"
)

// sniDemuxListener accepts TCP connections on a shared cluster port and
// routes each to a lane-specific virtual listener based on the TLS ClientHello
// Server Name Indication. Service traffic uses gastrolog-cluster; each raft
// group uses multiraft.LaneSNI(groupID). Legacy gastrolog-raft SNI maps to
// the cluster config group.
type sniDemuxListener struct {
	base net.Listener

	service    *virtualListener
	raftLanes  *multiraft.InboundLaneRegistry

	closeOnce sync.Once
	closed    chan struct{}
}

type virtualListener struct {
	ch     chan net.Conn
	closed chan struct{}
	addr   net.Addr
}

func newVirtualListener(addr net.Addr) *virtualListener {
	return &virtualListener{
		ch:     make(chan net.Conn, 16),
		closed: make(chan struct{}),
		addr:   addr,
	}
}

func (l *virtualListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-l.ch:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *virtualListener) Close() error {
	select {
	case <-l.closed:
		return net.ErrClosed
	default:
		close(l.closed)
		return nil
	}
}

func (l *virtualListener) Addr() net.Addr { return l.addr }

func (l *virtualListener) deliver(conn net.Conn) {
	select {
	case l.ch <- conn:
	case <-l.closed:
		_ = conn.Close()
	}
}

// newSNIDemuxListener wraps base and starts accepting connections.
func newSNIDemuxListener(base net.Listener, raftLanes *multiraft.InboundLaneRegistry) *sniDemuxListener {
	addr := base.Addr()
	d := &sniDemuxListener{
		base:      base,
		service:   newVirtualListener(addr),
		raftLanes: raftLanes,
		closed:    make(chan struct{}),
	}
	go d.acceptLoop()
	return d
}

func (d *sniDemuxListener) ServiceListener() net.Listener { return d.service }

func (d *sniDemuxListener) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.closed)
		err = d.base.Close()
		_ = d.service.Close()
		if d.raftLanes != nil {
			d.raftLanes.Close()
		}
	})
	return err
}

func (d *sniDemuxListener) acceptLoop() {
	for {
		conn, err := d.base.Accept()
		if err != nil {
			select {
			case <-d.closed:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go d.route(conn)
	}
}

func (d *sniDemuxListener) route(raw net.Conn) {
	sni, prefix, err := peekClientHelloSNI(raw)
	if err != nil {
		_ = raw.Close()
		return
	}
	conn := &peekedConn{Conn: raw, prefix: prefix}
	if groupID, ok := multiraft.GroupIDFromLaneSNI(sni); ok {
		if d.raftLanes != nil && d.raftLanes.Deliver(groupID, conn) {
			return
		}
		_ = raw.Close()
		return
	}
	// Empty SNI, gastrolog-cluster, or legacy localhost dials → service lane.
	d.service.deliver(conn)
}

type peekedConn struct {
	net.Conn
	prefix []byte
}

func (c *peekedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// peekClientHelloSNI reads the first TLS record and extracts the SNI extension.
// Returns peeked bytes to replay on the connection for the TLS handshake.
func peekClientHelloSNI(conn net.Conn) (serverName string, prefix []byte, err error) {
	buf := make([]byte, 4096)
	n, readErr := conn.Read(buf)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return "", nil, readErr
	}
	if n == 0 {
		return "", nil, errors.New("empty TLS ClientHello")
	}
	prefix = append([]byte(nil), buf[:n]...)
	sni, _ := parseClientHelloSNI(prefix)
	return sni, prefix, nil
}

// parseClientHelloSNI extracts the SNI host_name from a TLS ClientHello record.
func parseClientHelloSNI(data []byte) (string, error) {
	hello, err := clientHelloFromRecord(data)
	if err != nil {
		return "", err
	}
	return sniFromHelloExtensions(hello)
}

func clientHelloFromRecord(data []byte) ([]byte, error) {
	if len(data) < 5 || data[0] != 0x16 {
		return nil, errors.New("not a TLS handshake record")
	}
	recordLen := int(data[3])<<8 | int(data[4])
	if len(data) < 5+recordLen {
		return nil, errors.New("incomplete TLS record")
	}
	body := data[5 : 5+recordLen]
	if len(body) < 4 || body[0] != 0x01 {
		return nil, errors.New("not a ClientHello")
	}
	helloLen := int(body[1])<<16 | int(body[2])<<8 | int(body[3])
	if len(body) < 4+helloLen {
		return nil, errors.New("incomplete ClientHello")
	}
	hello := body[4 : 4+helloLen]
	if len(hello) < 34 {
		return nil, errors.New("ClientHello too short")
	}
	return hello, nil
}

func sniFromHelloExtensions(hello []byte) (string, error) {
	pos := 34 // version(2) + random(32)
	if pos >= len(hello) {
		return "", errors.New("missing session id")
	}
	sidLen := int(hello[pos])
	pos += 1 + sidLen
	if pos+2 > len(hello) {
		return "", errors.New("missing cipher suites")
	}
	csLen := int(hello[pos])<<8 | int(hello[pos+1])
	pos += 2 + csLen
	if pos >= len(hello) {
		return "", errors.New("missing compression")
	}
	compLen := int(hello[pos])
	pos += 1 + compLen
	if pos+2 > len(hello) {
		return "", nil
	}
	extLen := int(hello[pos])<<8 | int(hello[pos+1])
	pos += 2
	extEnd := pos + extLen
	if extEnd > len(hello) {
		return "", errors.New("invalid extensions")
	}
	for pos+4 <= extEnd {
		extType := int(hello[pos])<<8 | int(hello[pos+1])
		extDataLen := int(hello[pos+2])<<8 | int(hello[pos+3])
		pos += 4
		if pos+extDataLen > extEnd {
			break
		}
		if extType == 0 {
			if sni, ok := sniFromServerNameExt(hello[pos : pos+extDataLen]); ok {
				return sni, nil
			}
		}
		pos += extDataLen
	}
	return "", nil
}

func sniFromServerNameExt(ext []byte) (string, bool) {
	if len(ext) < 3 {
		return "", false
	}
	listLen := int(ext[0])<<8 | int(ext[1])
	if listLen+2 > len(ext) {
		return "", false
	}
	p := 2
	for p+3 <= 2+listLen {
		nameType := ext[p]
		nameLen := int(ext[p+1])<<8 | int(ext[p+2])
		p += 3
		if nameType == 0 && p+nameLen <= len(ext) {
			return string(ext[p : p+nameLen]), true
		}
		p += nameLen
	}
	return "", false
}
