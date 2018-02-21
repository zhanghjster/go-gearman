package gearman

import (
	"log"
)

type Dispatcher struct {
	reqCh       chan *Request
	transports  map[TransportPeer]*Transport
	respHandler map[PacketType]ResponseHandler
}

func NewDispatcher(server []string) *Dispatcher {
	var d = &Dispatcher{
		reqCh:       make(chan *Request),
		transports:  make(map[TransportPeer]*Transport),
		respHandler: make(map[PacketType]ResponseHandler),
	}

	for _, s := range server {
		ts := NewTransport(s)
		d.transports[ts.Peer] = ts

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

func (d *Dispatcher) Send(req *Request) error {
	if req.broadcast { // send to all server
		for _, ts := range d.transports {
			if err := ts.Write(req); err != nil {
				return err
			}
		}
	} else if req.peer != nil { // send to picked server
		if ts, ok := d.transports[*req.peer]; ok {
			return ts.Write(req)
		}
	} else { // send to random server
		d.reqCh <- req
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
	return ds
}
