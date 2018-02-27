package main

import (
	"log"

	"github.com/zhanghjster/go-gearman"
)

func main() {
	var server = "localhost:4730"

	var worker = gearman.NewWorker([]string{server})

	var funcName = "test"
	var handle = func(job *gearman.Job) ([]byte, error) {
		// get the data
		data, _ := job.GetData()
		if data != nil {
			log.Println(string(data))
		}

		return nil, nil
	}

	err := worker.RegisterFunction(funcName, handle, gearman.WorkerOptCanDo())
	if err != nil {
		log.Fatal(err)
	}

	worker.Work()
}
