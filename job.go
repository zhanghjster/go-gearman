package gearman

import "errors"

type JobHandle func(job *Job) ([]byte, error)

type Job struct {
	*Response
	w *Worker
}

func (j *Job) Update(opt WorkOptFunc) error {
	if opt == nil {
		return errors.New("opt nil")
	}

	var req = newRequestTo(j.peer.Remote)

	handle, err := j.GetHandle()
	if err != nil {
		return err
	}

	req.SetHandle(handle)

	opt(req)

	return j.w.sendRequest(req)
}
