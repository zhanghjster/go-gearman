package gearman

import (
	"log"
	"net"
	"sync"

	"fmt"
	"time"

	"github.com/pkg/errors"
)

type Dispatcher struct {
	reqCh        chan *Request
	transports   map[net.Addr]*Transport
	respHandlers map[PacketType]ResponseHandler
}

func NewDispatcher(server []string) *Dispatcher {
	var d = &Dispatcher{
		reqCh:        make(chan *Request),
		transports:   make(map[net.Addr]*Transport),
		respHandlers: make(map[PacketType]ResponseHandler),
	}

	for _, s := range server {
		ts, err := NewTransport(s)
		if err != nil {
			log.Printf("transport init for %s error\n", s)
			continue
		}

		d.transports[ts.Peer.Remote] = ts

		// loop for read and dispatch response
		go func(*Transport) {
			for {
				resp, err := ts.Read()
				if err != nil {
					log.Printf("read response, %s ", err.Error())
					continue
				}

				if h, ok := d.respHandlers[resp.Type]; ok {
					go h(resp)
				}
			}
		}(ts)

		// loop for send request by random server
		go func(*Transport) {
			for {
				if err := ts.Write(<-d.reqCh); err != nil {
					log.Printf("write req, %s", err.Error())
				}
			}
		}(ts)
	}

	log.Printf("dispatcher init done")

	return d
}

var DefaultSendTimeout = 10 * time.Second

func (d *Dispatcher) Send(req *Request) (err error) {
	if req.broadcast { // send to all server
		for _, ts := range d.transports {
			if e := ts.Write(req); e != nil {
				// merge the err string
				if err == nil {
					err = e
				} else {
					err = errors.New(fmt.Sprintf("%s\n%s", err.Error(), e.Error()))
				}
			}
		}
	} else if req.peer != nil { // send to picked server
		if ts, ok := d.transports[req.peer.Remote]; ok {
			err = ts.Write(req)
		} else {
			err = errors.New("server request send to not exist")
		}
	} else { // send Wait to random server
		err = enqueueRequestWithTimeout(d.reqCh, req)
	}

	return err
}

type ResponseTypeHandler struct {
	Types  []PacketType
	handle ResponseHandler
}

func (d *Dispatcher) RegisterResponseHandler(handlers ...ResponseTypeHandler) *Dispatcher {
	for _, handler := range handlers {
		for _, tp := range handler.Types {
			d.respHandlers[tp] = handler.handle
		}
	}
	return d
}

type Sender struct {
	mux sync.Mutex
	ds  *Dispatcher

	respCh chan *Response
}

func newSender(ds *Dispatcher) *Sender {
	return &Sender{ds: ds, respCh: make(chan *Response)}
}

func (s *Sender) sendAndWait(req *Request) (resp *Response, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if err = s.send(req); err != nil {
		return nil, err
	}

	log.Println("sender wait for response")

	// wait for response
	select {
	case resp = <-s.respCh:
	case <-req.peer.Closed():
		return nil, errors.New("conn closed")
	}

	return resp, nil
}

// TODO: sort out better way to handler peer/remote and send result
func (s *Sender) send(req *Request) error {
	// chan for response
	req.resCh = make(chan interface{})

	// send request
	if err := s.ds.Send(req); err != nil {
		return err
	}

	// 返回是哪个server发送的

	// wait for result
	res := <-req.resCh
	if err, ok := res.(error); ok {
		return err
	}

	return nil
}

func enqueueRequestWithTimeout(ch chan *Request, req *Request) error {
	if req.Timeout == nil {
		req.Timeout = time.After(DefaultSendTimeout)
	}

	select {
	case ch <- req:
	case <-req.Timeout:
		return errors.New("send timeout")
	}
	return nil
}
