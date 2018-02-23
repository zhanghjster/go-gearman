package gearman

import (
	"time"

	"sync"

	"github.com/pkg/errors"
)

type FlagChan chan struct{}

type Session struct {
	Peer  *TransportPeer
	after <-chan time.Time
}

func NewSession(peer *TransportPeer) *Session {
	return &Session{Peer: peer, after: make(chan time.Time)}
}

func (s *Session) Closed() FlagChan {
	return s.Peer.Closed()
}

func (s *Session) Timeout() <-chan time.Time {
	return s.after
}

func (s *Session) timeoutAfter(t time.Duration) *Session {
	s.after = time.After(t)
	return s
}

type sender struct {
	mux sync.Mutex
	ds  *Dispatcher

	respCh chan *Response
}

func (s *sender) asyncSend(req *Request) (resp *Response, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if err = s.send(req); err != nil {
		return nil, err
	}

	select {
	case resp = <-s.respCh:
	case <-req.peer.Closed():
		return nil, errors.New("conn closed")
	}

	return resp, nil
}

func (s *sender) send(req *Request) error {
	req.resCh = make(chan interface{})

	// asyncSend request
	if err := s.ds.Send(req); err != nil {
		return err
	}

	// wait for asyncSend result
	res := <-req.resCh
	if err, ok := res.(error); ok {
		return err
	}

	return nil
}
