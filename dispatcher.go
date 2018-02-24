package gearman

import (
	"log"
	"net"

	"fmt"
	"time"

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

		// loop for send request by random server
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
		}
	} else { // send Wait to random server
		err = enqueRequestWithTimeout(d.reqCh, req)
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
			d.respHandler[tp] = handler.handle
		}
	}
	return d
}
