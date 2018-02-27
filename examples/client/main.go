package main

import (
	"log"

	"github.com/zhanghjster/go-gearman"
)

func main() {
	var server = "localhost:4730"

	var client = gearman.NewClient([]string{server})

	// echo for test
	ret, err := client.Echo(server, []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("echo return ", string(ret))

	var funcName = "test"

	// do background task
	client.AddTask(
		// funcion name
		funcName,
		// data sent to worker
		[]byte("background"),
		// gearman.TaskOptHigh(), 				// set task priority high
		// gearman.TaskOptHighBackground(), 	// set background task high priority
		// gearman.TaskOptLow(),				// set task priority low
		// gearman.TaskOptLowBackground(),		// set background task low priority
		// gearman.TaskOptNormal(), 			// set task priority normal
		// gearman.TaskOptNormalBackground(),	// set background task normal priority
		gearman.TaskOptNormalBackground(),
	)

	// do non-background task, block until task complete
	client.AddTask(
		funcName,
		[]byte("non-background"),
		// handler for task complete
		gearman.TaskOptOnComplete(func(resp *gearman.Response) {
			data, _ := resp.GetData()
			log.Printf("task complete, data '%s'", string(data))
		}),
		// handler for task data update
		gearman.TaskOptOnData(func(resp *gearman.Response) {
			data, _ := resp.GetData()
			log.Printf("task update '%s'", string(data))
		}),
		// handler for task fail
		gearman.TaskOptOnFail(func(resp *gearman.Response) {
			log.Printf("task failed")
		}),
	)
}
