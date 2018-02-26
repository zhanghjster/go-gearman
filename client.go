package gearman

type Client struct {
	ts *TaskSet
	cm *ClientMisc
}

func NewClient(server []string) *Client {
	return new(Client).Init(server)
}

func (c *Client) Init(server []string) *Client {
	var ds = NewDispatcher(server)

	// set up modules
	c.ts = NewTaskSet().registerResponseHandle(ds)
	c.cm = NewClientMisc().registerResponseHandler(ds)

	return c
}

// add task, see TaskOptFuncs for all use case
func (c *Client) AddTask(funcName string, data []byte, opt ...TaskOptFunc) (*Task, error) {
	return c.ts.AddTask(funcName, data, opt...)
}

// get task status of handle default, see TaskStatusFuncs for all use case
func (c *Client) TaskStatus(task *Task, opts ...TaskStatusOptFunc) (TaskStatus, error) {
	return c.ts.TaskStatus(task, opts...)
}

func (c *Client) Echo(data []byte) ([]byte, error) {
	return c.cm.Echo(data)
}

func (c *Client) SetConnOption(name string) (string, error) {
	return c.cm.SetConnOption(name)
}

type ClientMisc struct {
	sender *Sender
}

func NewClientMisc() *ClientMisc {
	return new(ClientMisc)
}

func (cm *ClientMisc) SetConnOption(name string) (string, error) {
	var req = newRequestWithType(PtOptionReq)
	req.SetConnOption(name)

	resp, err := cm.sender.sendAndWaitResp(req)
	if err != nil {
		return "", err
	}

	return resp.GetConnOption()
}

func (cm *ClientMisc) Echo(data []byte) ([]byte, error) {
	var req = newRequestWithType(PtEchoReq)

	resp, err := cm.sender.sendAndWaitResp(req)
	if err != nil {
		return nil, err
	}

	return resp.GetData()
}

func (cm *ClientMisc) registerResponseHandler(ds *Dispatcher) *ClientMisc {
	cm.sender = newSender(ds)

	var handlers = []ResponseTypeHandler{
		{[]PacketType{PtOptionRes}, func(resp *Response) { cm.sender.respCh <- resp }},
		{[]PacketType{PtEchoRes}, func(resp *Response) { cm.sender.respCh <- resp }},
	}

	ds.RegisterResponseHandler(handlers...)

	return cm
}
