package main

import (
	"log"

	"github.com/zhanghjster/go-gearman"
)

func main() {
	var server = "localhost:4730"

	var worker = gearman.NewWorker([]string{server})

	var funcName = "test"
	var handle = func(job *gearman.Job) {}

	err := worker.RegisterFunction(funcName, handle, gearman.WorkerOptCanDo())
	if err != nil {
		log.Fatal(err)
	}

	worker.Work()
}
