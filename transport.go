package gearman

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"bytes"

	"io"

	"github.com/pkg/errors"
)

var (
	null     = []byte("\x00")
	magicREQ = string(null) + "REQ"
)

// connection wrap a buffer read/writer
type bufConnection struct {
	conn net.Conn
	*bufio.ReadWriter
	closeCh chan struct{}
}

func newConnection(addr string) (*bufConnection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	var c = &bufConnection{
		conn:    conn,
		closeCh: make(chan struct{}),
		ReadWriter: bufio.NewReadWriter(
			bufio.NewReader(conn),
			bufio.NewWriter(conn),
		),
	}

	return c, nil
}

func (c *bufConnection) closed() chan struct{} {
	return c.closeCh
}

func (c *bufConnection) close() {
	if c.closeCh != nil {
		close(c.closeCh)
		c.conn.Close()
	}
}

func (c *bufConnection) localAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *bufConnection) remoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

var (
	ReconnectInterval = 1 * time.Second
)

type TransportPeer struct {
	Remote net.Addr

	// broadcast conn close
	flag FlagChan
}

func (p *TransportPeer) Closed() FlagChan {
	return p.flag
}

type Transport struct {
	in  chan *Request
	out chan *Response

	// buffered connection
	bc *bufConnection

	// address
	Peer *TransportPeer
}

func NewTransport(server string) *Transport {
	return new(Transport).Init(server)
}

func (t *Transport) Init(server string) *Transport {
	go func() {
		// auto reconnect
		for {
			// close the old peer
			if t.Peer != nil {
				close(t.Peer.flag)
			}

			conn, err := newConnection(server)
			if err != nil {
				log.Println("conn init fail " + err.Error())
				time.Sleep(ReconnectInterval)
				continue
			}

			log.Printf("connect %s suc", server)

			// make new Peer
			t.Peer = &TransportPeer{conn.remoteAddr(), make(FlagChan)}

			t.bc = conn

			var wg sync.WaitGroup
			go t.readLoop(&wg)
			go t.writeLoop(&wg)
			wg.Wait()
		}
	}()

	return t
}

func (t *Transport) Write(req *Request) error {
	return enqueRequestWithTimeout(t.in, req)
}

func (t *Transport) Read() (*Response, error) {
	p := <-t.out
	return p, nil
}

func (t *Transport) readLoop(wg *sync.WaitGroup) {
	wg.Add(1)
	defer func() {
		wg.Done()
		t.bc.close()
	}()

	for {
		var err error
		r := &protocolReader{r: t.bc}
		if err = r.read(); err != nil {
			log.Printf("packet read err %s", err.Error())
			continue
		}

		if IsConnErr(err) {
			return
		}

		var resp = &Response{
			Packet: Packet{
				peer: t.Peer,
				Type: r.Type(),
				args: r.Lines(),
			},
		}

		t.out <- resp
	}
}

func (t *Transport) writeLoop(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case req := <-t.in:
			var err error
			if err = (&protocolWriter{w: t.bc}).write(req); err != nil {
				log.Printf("packet write err, %s ", err.Error())
			}

			// write request result chan
			if IsConnErr(err) {
				// response the result
				if req.resCh != nil {
					go func() { req.resCh <- err }()
				}
				return
			}

			// update the peer
			req.peer = t.Peer
		case <-t.bc.closed():
			return
		}
	}
}

type protocolWriter struct {
	w *bufConnection
}

func (pw *protocolWriter) write(req *Request) error {
	if req.IsAdmin() {
		// plain text protocol
		// write command string
		if _, err := pw.w.WriteString(req.AdminCmdString()); err != nil {
			return err
		}

		// write args
		for _, arg := range req.args {
			// write space
			if _, err := pw.w.Write([]byte(" ")); err != nil {
				return err
			}

			// write arg line
			if _, err := pw.w.Write(arg); err != nil {
				return err
			}
		}

		// write "\n" to finish
		if _, err := pw.w.Write([]byte("\n")); err != nil {
			return err
		}
	} else {
		// binary protocol
		// write 4 byte magic code
		if _, err := pw.w.WriteString(magicREQ); err != nil {
			return err
		}

		// write 4 byte type, big-engian
		if err := binary.Write(pw.w, binary.BigEndian, req.Type); err != nil {
			return err
		}

		var args = req.args

		var size int
		if len(args) > 0 {
			size = len(args) - 1
		}
		for _, line := range args {
			size += len(line)
		}

		// write 4 byte size, big-endian
		if err := binary.Write(pw.w, binary.BigEndian, size); err != nil {
			return err
		}

		// write arg lines to conn
		for i, line := range args {
			if _, err := pw.w.Write(line); err != nil {
				return err
			}

			// write null terminate
			if i != len(args)-1 {
				if _, err := pw.w.Write(null); err != nil {
					return err
				}
			}
		}
	}

	return pw.w.Flush()
}

type protocolReader struct {
	binary bool
	r      *bufConnection
	header []byte
	lines  [][]byte
}

func (pr *protocolReader) read() error {
	first, err := pr.r.Peek(1)
	if err != nil {
		return err
	}

	pr.binary = bytes.Equal(first, null)
	if pr.binary {
		pr.header = make([]byte, HeaderSize)
		n, err := pr.r.Read(pr.header)
		if n < HeaderSize {
			err = errors.New(fmt.Sprintf("packet header size, %d expected but got %d", HeaderSize, n))
		}
		if err != nil {
			return err
		}

		args := make([]byte, PacketType(binary.BigEndian.Uint32(pr.header[8:])))
		n, err = pr.r.Read(args)
		if n < len(args) {
			err = errors.New(fmt.Sprintf("packet lines size, %d expected but got %d", len(args), n))
		}
		if err != nil {
			return err
		}

		pr.lines = bytes.Split(args, null)
	} else {
		for {
			line, _, err := pr.r.ReadLine()
			pr.lines = append(pr.lines, line)
			if bytes.Equal(line, []byte(".")) || err != nil {
				return err
			}
		}
	}

	return err
}

func (pr *protocolReader) Type() (pt PacketType) {
	if !pr.binary {
		pt = PtAdminResp
	} else {
		pt = PacketType(binary.BigEndian.Uint32(pr.header[4:8]))
	}
	return
}

func (pr *protocolReader) Lines() [][]byte {
	return pr.lines
}

func IsConnErr(err error) bool {
	_, ok := err.(*net.OpError)
	return ok || err == io.EOF
}
