package gearman

import (
	"fmt"

	"github.com/pkg/errors"
)

type Admin struct {
	sender *Sender
}

func NewAdmin(server []string) *Admin {
	return new(Admin).Init(server)
}

func (adm *Admin) Init(server []string) *Admin {
	ds := NewDispatcher(server)

	adm.sender = &Sender{ds: ds, respCh: make(chan *Response)}

	handlers := []ResponseTypeHandler{
		{[]PacketType{PtAdminResp}, func(resp *Response) { adm.sender.respCh <- resp }},
	}

	ds.RegisterResponseHandler(handlers...)

	return adm
}

// send admin command, see AdmOptFuncs for command options
func (adm *Admin) Do(opt AdmOptFunc) (data [][]byte, err error) {
	if opt == nil {
		return nil, errors.New("admin command not set")
	}

	req := new(Request)

	var noWait bool
	opt(req, &noWait)

	if noWait {
		err = adm.sender.send(req)
	} else {
		var resp *Response
		resp, err = adm.sender.sendAndWait(req)
		if err == nil {
			data = resp.ArgsBytes()
		}
	}

	return
}

// option func for Do()
type AdmOptFunc func(*Request, *bool)

// adm option for show worker list
func AdmOptWorkers() AdmOptFunc {
	return func(req *Request, noWait *bool) { req.SetType(PtAdminWorkers) }
}

// amd option for show status
func AdmOptStatus() AdmOptFunc {
	return func(req *Request, noWait *bool) { req.SetType(PtAdminStatus) }
}

// adm option for show version
func AdmOptVersion() AdmOptFunc {
	return func(req *Request, noWat *bool) { req.SetType(PtAdminVersion) }
}

// adm option for shutdown
func AdmOptShutdown(graceful bool) AdmOptFunc {
	return func(req *Request, noWait *bool) {
		*noWait = true
		req.SetType(PtAdminShutdown)
		if graceful {
			req.SetGraceful()
		}
	}
}

// adm option for set max queue for all priority
func AdmOptMaxQueueAll(funcName string, all int) AdmOptFunc {
	return maxQueueOpt(funcName, fmt.Sprintf(" %d", all))
}

// adm option for set max queue for every priority
func AdmOptMaxQueueThreePriority(funcName string, high, normal, low int) AdmOptFunc {
	return maxQueueOpt(funcName, fmt.Sprintf(" %d %d %d", high, normal, low))
}

func maxQueueOpt(funcName, queue string) AdmOptFunc {
	return func(req *Request, noWait *bool) {
		*noWait = true
		req.SetType(PtAdminMaxQueue)
		req.SetFuncName(funcName)
		req.SetMaxQueue(queue)
	}
}
