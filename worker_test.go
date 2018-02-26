package gearman

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorker_RegisterFunction(t *testing.T) {
	var server = "localhost:4730"
	var worker = NewWorker([]string{server})

	var funcName = "test"
	var handle = func(job *Job) {}

	err := worker.RegisterFunction(funcName, handle, WorkerOptCanDo())
	assert.Nil(t, err, "register func fail")
	worker.Work()

}
