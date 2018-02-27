package main

import (
	"log"

	"time"

	"github.com/zhanghjster/go-gearman"
)

func main() {
	var server = "localhost:4730"

	var worker = gearman.NewWorker([]string{server})

	var funcName = "test"

	// job handler,
	// return []byte as data to client,
	// err means job fail
	var handle = func(job *gearman.Job) ([]byte, error) {
		// get the data
		data := job.Data()

		log.Printf("data string %s", string(data))

		if string(data) == "background" {
			log.Printf("process backgroup job, data %s", string(data))
		}

		var retData []byte
		if string(data) == "non-background" {
			log.Printf("process non-backgroud job, data %s", string(data))

			// send hello to the client
			err := job.Update(gearman.JobOptData([]byte("data for job update")))
			if err != nil {
				log.Printf("job data update err, %s", err.Error())
			}

			time.Sleep(3 * time.Second)

			// data fro task complete
			retData = []byte("data for job complete")
		}

		return retData, nil
	}

	err := worker.RegisterFunction(
		// function name worker can handle
		funcName,
		// handler for job process
		handle,
		// gearman.WorkerOptCanDo() for register
		// gearman.WorkerOptCantDo() for un-register
		// gearman.WorkerOptCanDoTimeout(10 * time.Second) for register can do with timeout
		gearman.WorkerOptCanDo(),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker.Work()
}
