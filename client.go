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
	c.ts = NewTaskSet().RegisterResponseHandle(ds)
	c.cm = NewClientMisc().RegisterResponseHandler(ds)

	return c
}

func (c *Client) AddTask(funcName string, data []byte, opt ...TaskOptFunc) (*Task, error) {
	return c.ts.AddTask(funcName, data, opt...)
}

// get task status of handle default
// set TaskStatusUniqueId(id) opt for task status unique
func (c *Client) TaskStatus(handle string, opts ...TaskStatusOptFunc) (TaskStatus, error) {
	return c.ts.TaskStatus(handle, opts...)
}

func (c *Client) Echo(data []byte) []byte {
	return c.cm.Echo(data)
}

func (c *Client) SetConnOption(name string) string {
	return c.cm.SetConnOption(name)
}

type ClientMisc struct {
	ds *Dispatcher

	orCh chan *Response
	erCh chan *Response
}

func NewClientMisc() *ClientMisc {
	return &ClientMisc{
		orCh: make(chan *Response),
		erCh: make(chan *Response),
	}
}

func (cm *ClientMisc) SetConnOption(name string) string {
	var req = new(Request)
	req.Type = PtOptionReq
	req.SetConnOption(name)

	resp := <-cm.orCh

	name, _ = resp.GetConnOption()
	return name
}

func (cm *ClientMisc) Echo(data []byte) []byte {
	var req = new(Request)
	req.Type = PtEchoReq

	resp := <-cm.orCh

	data, _ = resp.GetData()
	return data
}

func (cm *ClientMisc) RegisterResponseHandler(ds *Dispatcher) *ClientMisc {
	var handlers = []ResponseTypeHandler{
		{[]PacketType{PtOptionRes}, func(resp *Response) { cm.orCh <- resp }},
		{[]PacketType{PtEchoRes}, func(resp *Response) { cm.erCh <- resp }},
	}

	cm.ds = ds.RegisterResponseHandler(handlers...)

	return cm
}
