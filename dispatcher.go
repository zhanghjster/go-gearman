package gearman

import (
	"log"
	"net"

	"github.com/pkg/errors"
)

type Dispatcher struct {
	reqCh       chan *Request
	transports  map[net.Addr]*Transport
	respHandler map[PacketType]ResponseHandler
}

func NewDispatcher(server []string) *Dispatcher {
	var d = &Dispatcher{
		reqCh:       make(chan *Request),
		transports:  make(map[net.Addr]*Transport),
		respHandler: make(map[PacketType]ResponseHandler),
	}

	for _, s := range server {
		ts := NewTransport(s)
		d.transports[ts.Peer.Remote] = ts

		// loop for read and dispatch response
		go func(*Transport) {
			for {
				resp, err := ts.Read()
				if err != nil {
					log.Printf("read response, %s ", err.Error())
					continue
				}

				if h, ok := d.respHandler[resp.Type]; ok {
					go h(resp)
				}
			}
		}(ts)

		// loop for asyncSend request by random server
		go func(*Transport) {
			for {
				if err := ts.Write(<-d.reqCh); err != nil {
					log.Printf("write req, %s", err.Error())
				}
			}
		}(ts)
	}

	return d
}

func (d *Dispatcher) Send(req *Request) error {
	if req.broadcast { // asyncSend to all server
		for _, ts := range d.transports {
			if err := ts.Write(req); err != nil {
				return err
			}
		}
	} else if req.peer != nil { // asyncSend to picked server
		if ts, ok := d.transports[req.peer.Remote]; ok {
			return ts.Write(req)
		}
	} else { // asyncSend to random server
		select {
		case d.reqCh <- req:
			return nil
		case <-req.Timeout:
			return errors.New("sending request timeout")
		}
	}

	return nil
}

type ResponseTypeHandler struct {
	Types  []PacketType
	handle ResponseHandler
}

func (d *Dispatcher) RegisterResponseHandler(handlers ...ResponseTypeHandler) *Dispatcher {
	for _, handler := range handlers {
		for _, tp := range handler.Types {
			d.respHandler[tp] = handler.handle
		}
	}
	return d
}
