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

	reqOpt []ReqOptFunc

	// task response chan
	respCh chan *Response

	peer *TransportPeer

	// for close
	done chan struct{}
}

type TaskOptFunc func(*Task)
type ReqOptFunc func(*Request)
type TaskStatusOptFunc func(*Request)

func (t *Task) IsBackground() bool {
	return t.Type == PtSubmitJobBg ||
		t.Type == PtSubmitJobHighBg ||
		t.Type == PtSubmitJobLowBg
}

func (t *Task) wait() error {
	var done bool
	for !done {
		select {
		case resp := <-t.respCh:
			var handler ResponseHandler
			switch resp.Type {
			case PtWorkComplete:
				handler = func(resp *Response) {
					defer t.Done()
					if t.OnComplete != nil {
						t.OnComplete(resp)
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
		case <-t.peer.Closed():
			return errors.New("conn closed")
		}
	}

	return nil
}

func (t *Task) Done() {
	close(t.done)
}

type TaskSet struct {
	tasks map[string]*Task

	trMux sync.RWMutex // lock for task remove

	tcSender *sender
	tsSender *sender
}

func NewTaskSet() *TaskSet {
	return &TaskSet{
		tasks: make(map[string]*Task),
	}
}

// add task, see TaskOptFunc for all use case
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
	for _, opt := range task.reqOpt {
		opt(req)
	}

	resp, err := t.tcSender.sendAndWait(req)
	if err != nil {
		return nil, err
	}

	// set handle
	handle, err := resp.GetHandle()
	if err != nil {
		return nil, err
	}

	task.peer = resp.peer

	task.Handle = handle

	// save background task
	if !task.IsBackground() {
		// save to task map
		t.setTask(handle, task)

		// wait for task finished
		err = task.wait()

		// remote from task map
		t.removeTask(task.Handle)
	}

	return task, err
}

// check the status of task, see TaskStatusOptFuncs for all use case
func (t *TaskSet) TaskStatus(task *Task, opts ...TaskStatusOptFunc) (ts TaskStatus, err error) {
	var req = new(Request)
	req.SetPeer(task.peer)

	for _, opt := range opts {
		opt(req)
	}

	resp, err := t.tsSender.sendAndWait(req)
	if err != nil {
		return ts, err
	}

	// TODO: debug int parse
	ts.Known, _ = resp.GetStatusKnow()
	ts.Running, _ = resp.GetStatusRunning()
	ts.Numerator, _ = resp.GetPercentNumerator()
	ts.Denominator, _ = resp.GetPercentDenominator()
	ts.WaitingClient, _ = resp.GetWaitingClientNum()

	return ts, nil
}

func (t *TaskSet) registerResponseHandle(ds *Dispatcher) *TaskSet {
	t.tcSender = &sender{ds: ds, respCh: make(chan *Response)}
	t.tsSender = &sender{ds: ds, respCh: make(chan *Response)}

	var handlers = []ResponseTypeHandler{
		{
			[]PacketType{PtJobCreated}, func(resp *Response) { t.tcSender.respCh <- resp },
		},
		{
			[]PacketType{PtStatusRes, PtStatusResUnique}, func(resp *Response) { t.tsSender.respCh <- resp },
		},
		{
			[]PacketType{PtWorkData, PtWorkStatus, PtWorkComplete, PtWorkWarning, PtWorkFail, PtWorkException},
			t.workResponseHandle,
		},
	}

	ds.RegisterResponseHandler(handlers...)

	return t
}

func (t *TaskSet) workResponseHandle(resp *Response) {
	handle, _ := resp.GetHandle()
	if task, ok := t.getTask(handle); ok {
		go func() { task.respCh <- resp }()
	}
}

func (t *TaskSet) setTask(handle string, task *Task) {
	t.trMux.Lock()
	defer t.trMux.Unlock()
	t.tasks[handle] = task
}

func (t *TaskSet) getTask(handle string) (*Task, bool) {
	t.trMux.RLock()
	defer t.trMux.RUnlock()
	task, ok := t.tasks[handle]
	return task, ok
}

func (t *TaskSet) removeTask(handle string) {
	t.trMux.Lock()
	defer t.trMux.Unlock()
	delete(t.tasks, handle)
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

// set normal priority async task
func TaskOptNormal() TaskOptFunc { return taskTypeOpt(PtSubmitJob) }

// set normal priority background task
func TaskOptNormalBackground() TaskOptFunc { return taskTypeOpt(PtSubmitJobBg) }

// set high priority async task
func TaskOptHigh() TaskOptFunc { return taskTypeOpt(PtSubmitJobHigh) }

// set high priority background task
func TaskOptHighBackground() TaskOptFunc { return taskTypeOpt(PtSubmitJobHighBg) }

// set low priority async task
func TaskOptLow() TaskOptFunc { return taskTypeOpt(PtSubmitJobLow) }

// set low priority background task
func TaskOptLowBackground() TaskOptFunc { return taskTypeOpt(PtSubmitJobLowBg) }

// set async task complete callback
func TaskOptOnComplete(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnComplete = handler }
}

// set async task data update callback
func TaskOptOnData(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnData = handler }
}

// set async task fail callback
func TaskOptOnFail(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnFail = handler }
}

// set async task warning callback
func TaskOptOnWarning(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnWarning = handler }
}

// set async task exception callback
func TaskOptOnException(handler ResponseHandler) TaskOptFunc {
	return func(t *Task) { t.OnException = handler }
}

// set task unique id
func TaskOptUniqueId(id string) TaskOptFunc {
	return func(t *Task) {
		t.reqOpt = append(t.reqOpt, func(req *Request) { req.SetUniqueId(id) })
	}
}

// set task reducer
func TaskOptReducer(name string) TaskOptFunc {
	return func(t *Task) {
		t.reqOpt = append(t.reqOpt, func(req *Request) { req.SetReducer(name) })
	}
}

// set task schedule
func TaskOptSchedule(sched time.Time) TaskOptFunc {
	return func(t *Task) {
		t.reqOpt = append(t.reqOpt, func(req *Request) { req.SetSchedule(sched) })
	}
}

// set task epoch time
func TaskOptEpoch(epoch int64) TaskOptFunc {
	return func(t *Task) {
		t.reqOpt = append(t.reqOpt, func(req *Request) { req.SetEpoch(epoch) })
	}
}

// set connection option
func TaskOptConnOption(name string) TaskOptFunc {
	return func(t *Task) {
		t.reqOpt = append(t.reqOpt, func(req *Request) { req.SetConnOption(name) })
	}
}

// set timeout of task creation
func TaskOptCreationTimeout(d time.Duration) TaskOptFunc {
	return func(t *Task) {
		t.reqOpt = append(t.reqOpt, func(req *Request) { req.Timeout = time.After(d) })
	}
}

// set unique id of task for task status retrieve
func TaskOptStatusUniqueId(id string) TaskStatusOptFunc {
	return func(req *Request) {
		req.SetType(PtGetStatusUnique)
		req.SetUniqueId(id)
	}
}

// set task handle for task status retrieve
func TaskOptStatusHandle(handle string) TaskStatusOptFunc {
	return func(req *Request) {
		req.SetType(PtGetStatus)
		req.SetHandle(handle)
	}
}
