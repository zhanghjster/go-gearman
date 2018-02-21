package gearman

import (
	"errors"
	"sync"
	"time"
)

var (
	defaultTaskPacketType = PtSubmitJob
)

type Task struct {
	Type     PacketType
	FuncName string

	Handle string // set by response

	OnFail      ResponseHandler
	OnException ResponseHandler
	OnWarning   ResponseHandler
	OnData      ResponseHandler
	OnComplete  ResponseHandler

	argsOpt []ArgsOptFunc

	// task response chan
	respCh chan *Response

	// for get task status
	peer *TransportPeer

	// for close
	done chan struct{}
}

type TaskOptFunc func(*Task)
type ArgsOptFunc func(*Request)
type TaskStatusOptFunc func(*Request)

func (t *Task) IsBackground() bool {
	return t.Type == PtSubmitJobBg ||
		t.Type == PtSubmitJobHighBg ||
		t.Type == PtSubmitJobLowBg
}

func (t *Task) wait() {
	var done bool
	for !done {
		resp := <-t.respCh

		var handler ResponseHandler
		switch resp.Type {
		case PtWorkComplete:
			handler = func(resp *Response) {
				defer t.Done()
				if t.OnData != nil {
					t.OnData(resp)
				}
			}
			done = true
		case PtWorkData:
			handler = t.OnData
		case PtWorkWarning:
			handler = t.OnWarning
		case PtWorkFail:
			handler = t.OnFail
		case PtWorkException:
			handler = t.OnException
		}

		if handler != nil {
			go handler(resp)
		}
	}
}

func (t *Task) Done() {
	close(t.done)
}

type TaskSet struct {
	tasks map[string]*Task

	ds *Dispatcher

	trMux sync.RWMutex // lock for task remove
	tcMux sync.Mutex   // lock for task creation

	tcCh chan *Response // task creation chan
	suCh chan *Response // status update chan
}

func NewTaskSet() *TaskSet {
	return &TaskSet{
		tasks: make(map[string]*Task),
		tcCh:  make(chan *Response),
		suCh:  make(chan *Response),
	}
}

func (t *TaskSet) AddTask(funcName string, data []byte, opts ...TaskOptFunc) (*Task, error) {
	var task = &Task{
		FuncName: funcName,
		Type:     defaultTaskPacketType,
		done:     make(chan struct{}),
	}

	// set options
	for _, opt := range opts {
		opt(task)
	}

	var req = new(Request)

	// set request args
	req.SetType(task.Type)
	req.SetFuncName(task.FuncName)
	req.SetData(data)

	// args option
	for _, opt := range task.argsOpt {
		opt(req)
	}

	t.tcMux.Lock()
	defer t.tcMux.Unlock()

	if err := t.ds.Send(req); err != nil {
		return nil, err
	}

	// wait for response
	resp := <-t.tcCh

	// set handle
	handle, err := resp.GetHandle()
	if err != nil {
		return nil, err
	}

	task.peer = resp.peer
	task.Handle = handle

	// save background task
	if task.IsBackground() {
		t.tasks[task.Handle] = task
		task.wait()

		// handle task close signal
		go func() {
			<-task.done
			t.trMux.Lock()
			delete(t.tasks, task.Handle)
			t.trMux.Unlock()
		}()
	}

	return task, nil
}

func (t *TaskSet) TaskStatus(handle string, opts ...TaskStatusOptFunc) (ts TaskStatus, err error) {
	// get the handle
	t.trMux.RLock()
	task, ok := t.tasks[handle]
	t.trMux.RUnlock()
	if !ok {
		err = errors.New("task not exist")
		return
	}

	var req = new(Request)
	req.SetPeer(task.peer)

	for _, opt := range opts {
		opt(req)
	}

	if err = t.ds.Send(req); err != nil {
		return
	}

	resp := <-t.suCh

	// TODO: debug int parse
	ts.Known, _ = resp.GetStatusKnow()
	ts.Running, _ = resp.GetStatusRunning()
	ts.Numerator, _ = resp.GetPercentNumerator()
	ts.Denominator, _ = resp.GetPercentDenominator()
	ts.WaitingClient, _ = resp.GetWaitingClientNum()

	return ts, nil
}

func (t *TaskSet) RegisterResponseHandle(ds *Dispatcher) *TaskSet {
	var handlers = []ResponseTypeHandler{
		{
			[]PacketType{PtJobCreated}, t.jobCreationResponseHandle,
		},
		{
			[]PacketType{PtStatusRes, PtStatusResUnique}, t.statusResponseHandle,
		},
		{
			[]PacketType{PtWorkData, PtWorkStatus, PtWorkComplete, PtWorkWarning, PtWorkFail, PtWorkException},
			t.workResponseHandle,
		},
	}

	t.ds = ds.RegisterResponseHandler(handlers...)
	return t
}

func (t *TaskSet) jobCreationResponseHandle(resp *Response) { t.tcCh <- resp }
func (t *TaskSet) statusResponseHandle(resp *Response)      { t.suCh <- resp }
func (t *TaskSet) workResponseHandle(resp *Response) {
	handle, _ := resp.GetHandle()

	t.trMux.RLock()
	if task, ok := t.tasks[handle]; ok {
		go func() { task.respCh <- resp }()
	}
	t.trMux.RUnlock()
}

type TaskStatus struct {
	Known         bool   // know status
	Running       bool   // running status
	Numerator     uint32 // numerator of percent
	Denominator   uint32 // denominator of percent
	WaitingClient uint32 // count of waiting clients
}

// option func for task type
func taskTypeOpt(tp PacketType) TaskOptFunc {
	return func(task *Task) { task.Type = tp }
}
func TaskOptNormal() TaskOptFunc           { return taskTypeOpt(PtSubmitJob) }
func TaskOptNormalBackground() TaskOptFunc { return taskTypeOpt(PtSubmitJobBg) }
func TaskOptHigh() TaskOptFunc             { return taskTypeOpt(PtSubmitJobHigh) }
func TaskOptHighBackground() TaskOptFunc   { return taskTypeOpt(PtSubmitJobHighBg) }
func TaskOptLow() TaskOptFunc              { return taskTypeOpt(PtSubmitJobLow) }
func TaskOptLowBackground() TaskOptFunc    { return taskTypeOpt(PtSubmitJobLowBg) }

// option func for task callbacks
func TaskOptOnComplete(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnComplete = handler }
}
func TaskOptOnData(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnData = handler }
}
func TaskOptOnFail(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnFail = handler }
}
func TaskOptOnWarning(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnWarning = handler }
}
func TaskOptOnException(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnException = handler }
}

// option func for args
func TaskOptUniqueId(id string) TaskOptFunc {
	return func(t *Task) {
		t.argsOpt = append(t.argsOpt, func(req *Request) { req.SetUniqueId(id) })
	}
}
func TaskOptReducer(name string) TaskOptFunc {
	return func(t *Task) {
		t.argsOpt = append(t.argsOpt, func(req *Request) { req.SetReducer(name) })
	}
}
func TaskOptSchedule(sched time.Time) TaskOptFunc {
	return func(t *Task) {
		t.argsOpt = append(t.argsOpt, func(req *Request) { req.SetSchedule(sched) })
	}
}
func TaskOptEpoch(epoch int64) TaskOptFunc {
	return func(t *Task) {
		t.argsOpt = append(t.argsOpt, func(req *Request) { req.SetEpoch(epoch) })
	}
}
func TaskOptConnOption(name string) TaskOptFunc {
	return func(t *Task) {
		t.argsOpt = append(t.argsOpt, func(req *Request) { req.SetConnOption(name) })
	}
}

func TaskOptStatusUniqueId(id string) TaskStatusOptFunc {
	return func(req *Request) {
		req.SetType(PtGetStatusUnique)
		req.SetUniqueId(id)
	}
}
func TaskOptStatusHandle(handle string) TaskStatusOptFunc {
	return func(req *Request) {
		req.SetType(PtGetStatus)
		req.SetHandle(handle)
	}
}
