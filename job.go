package gearman

type JobHandle func(job *Job)

type Job struct {
	*Response
	w *Worker
}

func (j *Job) Update(opt WorkOptFunc) {
	if opt == nil {
		return
	}

	var req = new(Request)

	handle, _ := j.GetHandle()
	req.SetHandle(handle)

	req.peer = j.peer

	opt(req)

	j.w.sendRequest(req)
}
