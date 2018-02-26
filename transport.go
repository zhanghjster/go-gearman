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

var (
	DefaultReconnectInterval = 1 * time.Second
)

type TransportPeer struct {
	Remote string
	Local  string

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

func NewTransport(server string) (*Transport, error) {
	return new(Transport).Init(server)
}

func (t *Transport) Init(server string) (*Transport, error) {
	t.in = make(chan *Request)
	t.out = make(chan *Response)

	// do connect first
	if err := t.connect(server); err != nil {
		return nil, err
	}

	go func() {
		for {
			var wg sync.WaitGroup
			wg.Add(2)
			go t.readLoop(&wg)
			go t.writeLoop(&wg)
			wg.Wait()

			log.Printf("reconnect after %v\n", DefaultReconnectInterval)
			time.Sleep(DefaultReconnectInterval)

			// do reconnect
			t.connect(server)
		}
	}()

	return t, nil
}

// write request to remote server
func (t *Transport) Send(req *Request) error {
	return pushReqToChanWithTimeout(t.in, req)
}

// read response until there is one
func (t *Transport) Read() (*Response, error) {
	return <-t.out, nil
}

func (t *Transport) connect(server string) error {
	conn, err := newConnection(server)
	if err != nil {
		return err
	}

	t.bc = conn

	// close the old peer
	if t.Peer != nil {
		close(t.Peer.flag)
	}

	t.Peer = &TransportPeer{
		Remote: server,
		Local:  conn.localAddr().String(),
		flag:   make(FlagChan),
	}

	return nil
}

func (t *Transport) readLoop(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		t.bc.close()
	}()

	log.Printf("reader for %s => %s start\n", t.Peer.Local, t.Peer.Remote)

	r := &protocolReader{r: t.bc, hbf: make([]byte, HeaderSize)}
	for {
		pt, lines, err := r.read()
		if err != nil {
			log.Printf("packet read err %s", err.Error())
			continue
		}

		if IsConnErr(err) {
			log.Printf("transport conn err %s", err.Error())
			return
		}

		var resp = &Response{
			peer:   t.Peer,
			Packet: Packet{Type: pt, args: lines},
		}

		log.Printf("send response to transport out")

		t.out <- resp
	}
}

func (t *Transport) writeLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("writer for %s => %s start\n", t.Peer.Local, t.Peer.Remote)

	pw := &protocolWriter{w: t.bc}
	for {
		select {
		case req, ok := <-t.in:
			if !ok {
				log.Printf("fail to read from req chan")
				return
			}

			var err error
			if err = pw.write(req); err != nil {
				log.Printf("packet write err, %s ", err.Error())
			}

			// send back the result
			if req.retCh != nil {
				go func() {
					if err != nil {
						req.retCh <- err
					} else {
						req.retCh <- t.Peer
					}
				}()
			}

			if IsConnErr(err) {
				log.Printf("transport writer err %s", err.Error())
				return
			}
		case <-t.bc.closed():
			return
		}
	}
}

// connection wrap a buffer read/writer
type bufConnection struct {
	conn net.Conn
	*bufio.ReadWriter
	flag FlagChan
}

func newConnection(addr string) (*bufConnection, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	var c = &bufConnection{
		conn: conn,
		flag: make(FlagChan),
		ReadWriter: bufio.NewReadWriter(
			bufio.NewReader(conn),
			bufio.NewWriter(conn),
		),
	}

	return c, nil
}

func (c *bufConnection) closed() FlagChan {
	return c.flag
}

func (c *bufConnection) close() {
	c.conn.Close()
	close(c.flag)
}

func (c *bufConnection) remoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *bufConnection) localAddr() net.Addr {
	return c.conn.LocalAddr()
}

type protocolWriter struct {
	w *bufConnection
}

func (pw *protocolWriter) write(req *Request) error {
	if req.IsAdmin() {
		log.Printf("write adm command %s", req.AdminCmdString())
		// plain text protocol
		// write command string
		if _, err := pw.w.WriteString(req.AdminCmdString()); err != nil {
			return err
		}

		// write args
		for _, arg := range req.args {
			log.Printf("write adm command arg %s", string(arg))

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
		if err := binary.Write(pw.w, binary.BigEndian, uint32(size)); err != nil {
			return err
		}

		// write arg lines to conn
		for i, line := range args {
			log.Printf("write args line, size %d", len(line))

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
	r   *bufConnection
	hbf []byte // header buf
}

func (pr *protocolReader) read() (PacketType, [][]byte, error) {
	first, err := pr.r.Peek(1)
	if err != nil {
		return PtNull, nil, err
	}

	var pt PacketType
	var lines [][]byte

	if bytes.Equal(first, null) {
		log.Printf("read new binary packet")

		// read the header 12 bytes
		n, err := pr.r.Read(pr.hbf)
		if n < HeaderSize {
			err = errors.New(fmt.Sprintf("packet header size %d, but %d expected", n, HeaderSize))
		}
		if err != nil {
			return PtNull, nil, err
		}

		// parse the packet type
		pt = PacketType(binary.BigEndian.Uint32(pr.hbf[4:8]))

		// read the args
		args := make([]byte, binary.BigEndian.Uint32(pr.hbf[8:]))
		n, err = pr.r.Read(args)
		if n < len(args) {
			err = errors.New(fmt.Sprintf("packet lines size %d, but %d expected", n, len(args)))
		}
		if err != nil {
			return PtNull, nil, err
		}

		// split args by "\0"
		lines = bytes.Split(args, null)
	} else {
		log.Printf("read new adm response")

		pt = PtAdminResp
		for {
			line, _, err := pr.r.ReadLine()
			if err != nil {
				return pt, nil, err
			}

			log.Printf("read new line of adm resp, %s", string(line))

			// end with "."
			if bytes.Equal(line, []byte(".")) {
				break
			}

			lines = append(lines, line)

			if bytes.Equal(line[0:2], []byte("OK")) {
				break
			}
		}
	}

	return pt, lines, nil
}

func IsConnErr(err error) bool {
	_, ok := err.(*net.OpError)
	return ok || err == io.EOF
}
