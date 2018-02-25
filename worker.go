package gearman

import (
	"log"
	"time"
)

var DefaultGrabInterval = 10 * time.Millisecond

type Worker struct {
	sender *Sender

	jobsFlag FlagChan

	// interval for grab job command
	grabInterval time.Duration

	jobHandles map[string]JobHandle
}

func NewWorker(server []string) *Worker {
	return new(Worker).Init(server)
}

func (w *Worker) Init(server []string) *Worker {
	var ds = NewDispatcher(server)

	w.grabInterval = DefaultGrabInterval

	w.sender = newSender(ds)

	// register handler
	var handlers = []ResponseTypeHandler{
		{Types: []PacketType{PtJobAssign, PtJobAssignUnique, PtJobAssignAll}, handle: w.jobHandler},
	}
	ds.RegisterResponseHandler(handlers...)

	return w
}

// set max parallel jobs work can handle
func (w *Worker) MaxParallelJobs(n int) *Worker {
	if n > 0 {
		w.jobsFlag = make(FlagChan, n)
	}
	return w
}

// set grab job command interval, default 10 millisecond
func (w *Worker) GrabInterval(t time.Duration) *Worker {
	w.grabInterval = t
	return w
}

type WorkerOptFunc func(req *Request)

// set worker can do
func WorkerOptCanDo() WorkerOptFunc { return func(req *Request) { req.SetType(PtCanDo) } }

// set worker cant do
func WorkerOptCantDo() WorkerOptFunc { return func(req *Request) { req.SetType(PtCantDo) } }

// set work can do with timeout
func WorkerOptCanDoTimeout(t int) WorkerOptFunc {
	return func(req *Request) {
		req.SetType(PtCanDoTimeout)
		req.SetCanDoTimeout(t)
	}
}

// register funcName and handle, see WorkOptFun for all use case
func (w *Worker) RegisterFunction(funcName string, handle JobHandle, opt WorkerOptFunc) error {
	if opt == nil {
		opt = WorkerOptCanDo()
	}

	w.jobHandles[funcName] = handle

	var req = newBroadcaseRequest()

	opt(req)

	return w.sendRequest(req)
}

func (w *Worker) ResetAbilities() {
	w.sendRequest(newRequestWithType(PtResetAbilities))
}

func (w *Worker) Work() error {
	for {
		if w.jobsFlag != nil {
			w.jobsFlag <- struct{}{}
		}

		var req = newBroadcastRequestWithType(PtGrabJobAll)
		if err := w.sender.send(req); err != nil {
			log.Printf("err: %s", err.Error())
		}

		time.Sleep(w.grabInterval)
	}
}

func (w *Worker) sendRequest(req *Request) error { return w.sender.send(req) }

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
