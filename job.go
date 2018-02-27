package gearman

import "github.com/pkg/errors"

// job processor
// the return value 'err' means fail,
// 'data' is the final data send to the client
type JobHandle func(job *Job) (data []byte, err error)

type Job struct {
	resp *Response
	w    *Worker
}

// update job status, see JobOptFuncs for details
func (j *Job) Update(opt JobOptFunc) error {
	if opt == nil {
		return errors.New("job update opt nil")
	}

	var req = newRequestTo(j.resp.peer.Remote)
	opt(req)

	handle, _ := j.resp.GetHandle()
	if err := req.SetHandle(handle); err != nil {
		return err
	}

	Log.Printf("send job update, handle %s, server %s", handle, j.resp.peer.Remote)

	return j.w.sendRequest(req)
}

// get job data
func (j *Job) Data() []byte {
	data, _ := j.resp.GetData()
	return data
}

type JobOptFunc func(req *Request)

// update job percent numerator and denominator
func JobOptStatus(n, d uint32) JobOptFunc {
	return func(req *Request) {
		req.SetType(PtWorkStatus)
		req.SetPercent(n, d)
	}
}

// update job data
func JobOptData(data []byte) JobOptFunc {
	return func(req *Request) {
		req.SetType(PtWorkData)
		req.SetData(data)
	}
}
