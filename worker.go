package gearman

import (
	"log"
	"sync"
	"time"
)

var DefaultRegisterInterval = 1 * time.Second

type Worker struct {
	sender *Sender

	server []string

	jobsFlag FlagChan

	jobHandles map[string]JobHandle

	// flag for registration loop
	regFlag map[string]FlagChan

	// flag for noop
	noopFlag map[string]FlagChan

	// interval for re-register
	registerInterval time.Duration
}

func NewWorker(server []string) *Worker {
	return new(Worker).Init(server)
}

func (w *Worker) Init(server []string) *Worker {
	var ds = NewDispatcher(server)

	w.server = server
	w.registerInterval = DefaultRegisterInterval

	w.jobHandles = make(map[string]JobHandle)
	w.regFlag = make(map[string]FlagChan)
	w.noopFlag = make(map[string]FlagChan)

	w.sender = newSender(ds)

	// register handler
	var handlers = []ResponseTypeHandler{
		{
			Types:  []PacketType{PtNoop},
			handle: func(resp *Response) { w.noopFlag[resp.peer.Remote] <- Flag },
		},
		{
			Types:  []PacketType{PtNoJob, PtJobAssign, PtJobAssignUnique, PtJobAssignAll},
			handle: func(resp *Response) { w.sender.respCh <- resp },
		},
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

type WorkerOptFunc func(*Request, *bool)

// set worker can do
func WorkerOptCanDo() WorkerOptFunc {
	return func(req *Request, regOnce *bool) {
		req.SetType(PtCanDo)
	}
}

// set worker cant do
func WorkerOptCantDo() WorkerOptFunc {
	return func(req *Request, regOnce *bool) {
		*regOnce = true
		req.SetType(PtCantDo)
	}
}

// set work can do with timeout
func WorkerOptCanDoTimeout(t int) WorkerOptFunc {
	return func(req *Request, regOnce *bool) {
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

	for _, s := range w.server {
		// stop the previous reg loop
		if flag, ok := w.regFlag[funcName]; ok {
			close(flag)
		}

		// flag for stop reg loop
		var done = make(FlagChan)
		w.regFlag[funcName] = done

		// run reg loop
		go func(server string) {
			var regOnce bool

			var req = newRequestTo(server)
			// set request type here by
			opt(req, &regOnce)

			err := req.SetFuncName(funcName)
			if err != nil {
				Log.Printf("set func name err %s", err.Error())
			}

			for {
				peer, err := w.sender.send(req)
				if err != nil {
					Log.Printf("register func '%s' err, %s", funcName, err.Error())

					// sleep before re-register
					time.Sleep(w.registerInterval)
					select {
					case <-done:
						return
					default:
						continue
					}
				}

				Log.Printf("register func '%s' to %s suc", funcName, server)

				// 'cannot' only send once
				if regOnce {
					break
				}

				select {
				case <-peer.Closed():
				case <-done:
					return
				}
			}
		}(s)
	}

	return nil
}

func (w *Worker) Work() {
	var wg sync.WaitGroup

	for _, s := range w.server {
		w.noopFlag[s] = make(FlagChan)

		wg.Add(1)
		go func(server string) {
			defer wg.Done()

			for {
				w.descJobs()

				// retrieve next job
				var req = newRequestToServerWithType(server, PtGrabJobAll)

				resp, err := w.sender.sendAndWaitResp(req)
				if err != nil {
					Log.Printf("send grab req to %s fail, %s", server, err.Error())
					return
				}

				Log.Printf("send grab request to %s suc", s)

				switch resp.Type {
				case PtNoJob:
					// send pre sleep then Wait for wake up signal
					var req = newRequestToServerWithType(server, PtPreSleep)
					peer, err := w.sender.send(req)
					if err != nil {
						Log.Printf("send pre sleep packet to %s fail, %s", server, err)
						return
					}

					Log.Printf("job retriever for %s Wait for weekup", server)

					select {
					case <-w.noopFlag[server]:
						Log.Printf("job retriever for %s weekup", server)
					case <-peer.Closed():
						Log.Printf("job retriever for %s err. ", err.Error())
					}
				case PtJobAssign, PtJobAssignUnique, PtJobAssignAll:
					go func(resp *Response) {
						defer w.descJobs()

						var job = &Job{w: w, resp: resp}

						funcName, _ := job.resp.GetFuncName()
						handle, ok := w.jobHandles[funcName]
						if !ok {
							Log.Printf("no worker handle found for job %s", funcName)
							return
						}

						handleId, err := job.resp.GetHandle()
						if err != nil {
							log.Printf("get job handle err, %s", err)
							return
						}

						// handle the job
						data, err := handle(job)

						var req = newRequestTo(server)
						// set req type
						if err != nil {
							req.SetType(PtWorkFail)
						} else {
							req.SetType(PtWorkComplete)
							req.SetData(data)
						}

						if err = req.SetHandle(handleId); err != nil {
							log.Printf("req set handle err, %s", err.Error())
							return
						}

						if _, err := w.sender.send(req); err != nil {
							Log.Printf("send job result request %d err, %s", req.Type, err.Error())
						}
					}(resp)
				}
			}
		}(s)
	}

	wg.Wait()
}

func (w *Worker) ResetAbilities(server string) {
	w.sendRequest(newRequestToServerWithType(server, PtResetAbilities))
}

func (w *Worker) incJobs() {
	if w.jobsFlag != nil {
		select {
		case <-w.jobsFlag:
		default:
		}
	}
}

func (w *Worker) descJobs() {
	if w.jobsFlag != nil {
		w.jobsFlag <- Flag
	}
}

func (w *Worker) sendRequest(req *Request) error {
	_, err := w.sender.send(req)
	return err
}
