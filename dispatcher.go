package gearman

import (
	"sync"

	"time"

	"github.com/pkg/errors"
)

type Dispatcher struct {
	reqCh        chan *Request
	transports   map[string]*Transport
	respHandlers map[PacketType]ResponseHandler
}

func NewDispatcher(server []string) *Dispatcher {
	var d = &Dispatcher{
		reqCh:        make(chan *Request),
		transports:   make(map[string]*Transport),
		respHandlers: make(map[PacketType]ResponseHandler),
	}

	for _, s := range server {
		ts, err := NewTransport(s)
		if err != nil {
			Log.Printf("transport init for %s error\n", s)
			continue
		}

		d.transports[s] = ts

		// loop for read and dispatch response
		go func(*Transport) {
			for {
				resp, err := ts.Read()
				if err != nil {
					Log.Printf("read response, %s ", err.Error())
					continue
				}

				if h, ok := d.respHandlers[resp.Type]; ok {
					go h(resp)
				}
			}
		}(ts)

		// loop for send request to random server
		go func(*Transport) {
			for {
				if err := ts.Send(<-d.reqCh); err != nil {
					Log.Printf("write req, %s", err.Error())
				}
			}
		}(ts)
	}

	Log.Printf("dispatcher init done")

	return d
}

var DefaultSendTimeout = 10 * time.Second

func (d *Dispatcher) Send(req *Request) error {
	if req.Server != "" {
		if ts, ok := d.transports[req.Server]; ok {
			return ts.Send(req)
		} else {
			return errors.New("server not exist")
		}
	}

	// inject to chan for transports custom
	return pushReqToChanWithTimeout(d.reqCh, req)
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

func (s *Sender) sendAndWaitResp(req *Request) (resp *Response, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	peer, err := s.send(req)
	if err != nil {
		return nil, err
	}

	Log.Println("miscSender wait for response")

	// wait for response
	select {
	case resp = <-s.respCh:
	case <-peer.Closed():
		return nil, errors.New("conn closed")
	}

	return resp, nil
}

func (s *Sender) send(req *Request) (peer *TransportPeer, err error) {
	// chan for return such as peer or error
	req.retCh = make(chan interface{})

	// send request
	if err = s.ds.Send(req); err != nil {
		return nil, err
	}

	// wait for result
	res := <-req.retCh
	switch res.(type) {
	case error:
		err = res.(error)
	case *TransportPeer:
		peer = res.(*TransportPeer)
	}

	return
}

func pushReqToChanWithTimeout(ch chan *Request, req *Request) error {
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
