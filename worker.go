package gearman

import (
	"time"
)

var DefaultGrabInterval = 10 * time.Millisecond

type Worker struct {
	ds *Dispatcher

	jobsFlag chan struct{}

	respCh chan *Response

	// interval for grab job command
	grabInterval time.Duration

	jobHandles map[string]JobHandle
}

func NewWorker(server []string) *Worker {
	return new(Worker).Init(server)
}

func (w *Worker) Init(server []string) *Worker {
	w.respCh = make(chan *Response)

	w.grabInterval = DefaultGrabInterval

	w.ds = NewDispatcher(server)

	// register handler
	var handlers = []ResponseTypeHandler{
		{Types: []PacketType{PtNoop}, handle: w.noopHandler},
		{Types: []PacketType{PtNoJob}, handle: w.noJobHandle},
		{Types: []PacketType{PtJobAssign, PtJobAssignUnique, PtJobAssignAll}, handle: w.jobHandler},
	}
	w.ds.RegisterResponseHandler(handlers...)

	return w
}

func (w *Worker) MaxParallelJobs(n int) *Worker {
	if n > 0 {
		w.jobsFlag = make(chan struct{}, n)
	}
	return w
}
func (w *Worker) GrabInterval(t time.Duration) *Worker {
	w.grabInterval = t
	return w
}

type WorkerOptFunc func(req *Request)

func WorkerOptCanDo() WorkerOptFunc  { return func(req *Request) { req.SetType(PtCanDo) } }
func WorkerOptCantDo() WorkerOptFunc { return func(req *Request) { req.SetType(PtCantDo) } }
func WorkerOptCanDoTimeout(t int) WorkerOptFunc {
	return func(req *Request) {
		req.SetType(PtCanDoTimeout)
		req.SetCanDoTimeout(t)
	}
}
func (w *Worker) RegisterFunction(funcName string, handle JobHandle, opt WorkerOptFunc) {
	w.jobHandles[funcName] = handle

	var req = &Request{broadcast: true}
	if opt == nil {
		opt = WorkerOptCanDo()
	}

	opt(req)

	w.sendRequest(req)
}

func (w *Worker) ResetAbilities() {
	req := &Request{broadcast: true}
	req.SetType(PtResetAbilities)
	w.sendRequest(req)
}

func (w *Worker) Work() {
	for {
		if w.jobsFlag != nil {
			w.jobsFlag <- struct{}{}
		}

		var req = new(Request)
		req.SetType(PtGrabJobAll)
		req.broadcast = true

		w.ds.Send(req)

		time.Sleep(w.grabInterval)
	}
}

func (w *Worker) sendRequest(req *Request) { w.ds.Send(req) }

func (w *Worker) jobHandler(resp *Response) {
	var job = &Job{w: w, Response: resp}

	funcName, _ := job.GetFuncName()
	handle, ok := w.jobHandles[funcName]
	if !ok {
		return
	}

	handle(job)

	if w.jobsFlag != nil {
		select {
		case <-w.jobsFlag:
		default:
		}
	}
}
func (w *Worker) noopHandler(resp *Response) {}
func (w *Worker) noJobHandle(resp *Response) {}

type WorkOptFunc func(req *Request)

func WorkOptStatus(n, d uint32) WorkOptFunc {
	return func(req *Request) {
		req.SetType(PtWorkStatus)
		req.SetPercent(n, d)
	}
}
func WorkOptFail() WorkOptFunc {
	return func(req *Request) { req.SetType(PtWorkFail) }
}
func WorkOptComplete(data []byte) WorkOptFunc {
	return workTypeDataOptFuncWrapper(PtWorkComplete, data)
}
func WorkOptWarning(data []byte) WorkOptFunc {
	return workTypeDataOptFuncWrapper(PtWorkWarning, data)
}
func WorkOptException(data []byte) WorkOptFunc {
	return workTypeDataOptFuncWrapper(PtWorkException, data)
}

func workTypeDataOptFuncWrapper(tp PacketType, data []byte) WorkOptFunc {
	return func(req *Request) {
		req.SetType(tp)
		req.SetData(data)
	}
}
