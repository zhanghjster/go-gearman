package gearman

import "errors"

type JobHandle func(job *Job)

type Job struct {
	*Response
	w *Worker
}

func (j *Job) Update(opt WorkOptFunc) error {
	if opt == nil {
		return errors.New("opt nil")
	}

	var req = new(Request)

	handle, err := j.GetHandle()
	if err != nil {
		return err
	}

	req.SetHandle(handle)

	req.peer = j.peer

	opt(req)

	return j.w.sendRequest(req)
}
