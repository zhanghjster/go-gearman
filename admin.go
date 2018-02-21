package gearman

import (
	"fmt"

	"github.com/pkg/errors"
)

type Admin struct {
	ds *Dispatcher
	ch chan *Response
}

func NewAdmin(server []string) *Admin {
	return new(Admin).Init(server)
}

func (adm *Admin) Init(server []string) *Admin {
	adm.ch = make(chan *Response)

	adm.ds = NewDispatcher(server)

	handlers := []ResponseTypeHandler{
		{[]PacketType{PtAdminResp}, func(resp *Response) { adm.ch <- resp }},
	}

	adm.ds.RegisterResponseHandler(handlers...)

	return adm
}

func (adm *Admin) Do(opt AdmOptFunc) (data [][]byte, err error) {
	if opt == nil {
		return nil, errors.New("admin command not set")
	}

	req := &Request{broadcast: true}

	var waitResp bool
	opt(req, &waitResp)

	adm.ds.Send(req)

	if waitResp {
		resp := <-adm.ch
		data = resp.ArgsBytes()
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
