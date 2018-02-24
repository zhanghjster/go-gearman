package gearman

import (
	"sync"

	"time"

	"github.com/pkg/errors"
)

type FlagChan chan struct{}

type sender struct {
	mux sync.Mutex
	ds  *Dispatcher

	respCh chan *Response
}

func (s *sender) sendAndWait(req *Request) (resp *Response, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if err = s.send(req); err != nil {
		return nil, err
	}

	// wait for response
	select {
	case resp = <-s.respCh:
	case <-req.peer.Closed():
		return nil, errors.New("conn closed")
	}

	return resp, nil
}

func (s *sender) send(req *Request) error {
	// chan for response
	req.resCh = make(chan interface{})

	// send request
	if err := s.ds.Send(req); err != nil {
		return err
	}

	// wait for result
	res := <-req.resCh
	if err, ok := res.(error); ok {
		return err
	}

	return nil
}

func enqueRequestWithTimeout(ch chan *Request, req *Request) error {
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
