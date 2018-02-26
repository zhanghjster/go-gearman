package gearman

import (
	"fmt"

	"log"

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

	adm.sender = newSender(ds)

	handlers := []ResponseTypeHandler{
		{[]PacketType{PtAdminResp}, func(resp *Response) { adm.sender.respCh <- resp }},
	}

	ds.RegisterResponseHandler(handlers...)

	return adm
}

// send admin command, see AdmOptFuncs for command options
func (adm *Admin) Do(server string, opt AdmOptFunc) (data [][]byte, err error) {
	if opt == nil {
		return nil, errors.New("admin cmd not set")
	}

	var waitResp bool

	var req = newRequestTo(server)
	opt(req, &waitResp)

	log.Printf("send admin cmd %s, wait %v", req.AdminCmdString(), waitResp)

	if waitResp {
		_, err = adm.sender.send(req)
	} else {
		var resp *Response
		resp, err = adm.sender.sendAndWaitResp(req)
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
	return func(req *Request, waitResp *bool) { reqWaitWithType(req, PtAdminWorkers, waitResp) }
}

// amd option for show status
func AdmOptStatus() AdmOptFunc {
	return func(req *Request, waitResp *bool) { reqWaitWithType(req, PtAdminStatus, waitResp) }
}

// adm option for show version
func AdmOptVersion() AdmOptFunc {
	return func(req *Request, waitResp *bool) { reqWaitWithType(req, PtAdminVersion, waitResp) }
}

func reqWaitWithType(req *Request, pt PacketType, wait *bool) {
	*wait = true
	req.Type = pt
}

// adm option for shutdown
func AdmOptShutdown(graceful bool) AdmOptFunc {
	return shutdown(false)
}

// adm option for shutdown graceful
func AdmOptShutdownGraceful() AdmOptFunc {
	return shutdown(true)
}

func shutdown(graceful bool) AdmOptFunc {
	return func(req *Request, waitResp *bool) {
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
	return func(req *Request, waitResp *bool) {
		req.SetType(PtAdminMaxQueue)
		req.SetFuncName(funcName)
		req.SetMaxQueue(queue)
	}
}
