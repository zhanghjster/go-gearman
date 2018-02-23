package gearman

import (
	"fmt"

	"github.com/pkg/errors"
)

type Admin struct {
	sender *sender
	ch     chan *Response
}

func NewAdmin(server []string) *Admin {
	return new(Admin).Init(server)
}

func (adm *Admin) Init(server []string) *Admin {
	adm.ch = make(chan *Response)

	ds := NewDispatcher(server)

	adm.sender = &sender{ds: ds, respCh: make(chan *Response)}

	handlers := []ResponseTypeHandler{
		{[]PacketType{PtAdminResp}, func(resp *Response) { adm.sender.respCh <- resp }},
	}

	ds.RegisterResponseHandler(handlers...)

	return adm
}

func (adm *Admin) Do(opt AdmOptFunc) (data [][]byte, err error) {
	if opt == nil {
		return nil, errors.New("admin command not set")
	}

	req := new(Request)

	var waitResp bool
	opt(req, &waitResp)

	adm.sender.asyncSend(req)

	if waitResp {
		resp, err := adm.sender.asyncSend(req)
		if err != nil {
			return nil, err
		} else {
			return resp.ArgsBytes(), nil
		}
	} else {
		return nil, adm.sender.send(req)
	}

	return
}

type AdmOptFunc func(req *Request, noWait *bool)

func AdmOptWorkers() AdmOptFunc {
	return func(req *Request, no *bool) { req.SetType(PtAdminWorkers) }
}
func AdmOptStatus() AdmOptFunc {
	return func(req *Request, no *bool) { req.SetType(PtAdminStatus) }
}
func AdmOptVersion() AdmOptFunc {
	return func(req *Request, no *bool) { req.SetType(PtAdminVersion) }
}
func AdmOptShutdown(graceful bool) AdmOptFunc {
	return func(req *Request, no *bool) {
		*no = true
		req.SetType(PtAdminShutdown)
		if graceful {
			req.SetGraceful()
		}
	}
}
func AdmOptMaxQueueAll(funcName string, all int) AdmOptFunc {
	return maxQueueOpt(funcName, fmt.Sprintf(" %d", all))
}
func AdmOptMaxQueueThreePriority(funcName string, high, normal, low int) AdmOptFunc {
	return maxQueueOpt(funcName, fmt.Sprintf(" %d %d %d", high, normal, low))
}
func maxQueueOpt(funcName, queue string) AdmOptFunc {
	return func(req *Request, no *bool) {
		*no = true
		req.SetType(PtAdminMaxQueue)
		req.SetFuncName(funcName)
		req.SetMaxQueue(queue)
	}
}
