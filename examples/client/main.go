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
		funcName,
		[]byte("background"),
		gearman.TaskOptNormalBackground(),
	)

	// do non-background task, block until task complete
	client.AddTask(
		funcName,
		[]byte("non-background"),
		gearman.TaskOptOnComplete(func(resp *gearman.Response) {
			data, _ := resp.GetData()
			log.Printf("task complete, data '%s'", string(data))
		}),
		gearman.TaskOptOnData(func(resp *gearman.Response) {
			data, _ := resp.GetData()
			log.Printf("task update '%s'", string(data))
		}),
		gearman.TaskOptOnFail(func(resp *gearman.Response) {
			log.Printf("task failed")
		}),
	)
}
