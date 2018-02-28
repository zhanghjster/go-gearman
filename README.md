## Install

~~~go
go get github.com/zhanghjster/go-gearman
~~~

## Client

```go
var server = []string{"localhost:4730", "localhost:4731"}

var client = gearman.NewClient(server)

// echo for test
for _, s := range server {
    ret, err := client.Echo(s, []byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("echo to server %s, returned %s", s, string(ret))
}

var funcName = "test"

// do background task
client.AddTask(
    // function name
    funcName,
    // data sent to worker
    []byte("background"),
    // set task background with normal priority
    gearman.TaskOptNormalBackground(),
)

for i := 0; i < 10; i++ {
    log.Printf("run non-background task %d", i)

    // do non-background task, block until task complete
    task, err := client.AddTask(
        funcName,
        []byte("non-background"),
        // handler for task data forwarded from worker
        gearman.TaskOptOnData(func(resp *gearman.Response) {
            data, _ := resp.GetData()
            log.Printf("task update, data sent from worker is '%s'", string(data))
        }),
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("gearman server task sent send to %s", task.Remote())

    // wait for complete
    data, err := task.Wait()
    log.Printf("task finished, data sent from worker is '%s'", string(data))
}
```

### func(*Client) AddTask()

~~~go
func (*Client) AddTask(funcName string, data []byte, opt ...TaskOptFunc) (*Task, error)
~~~

create new task, 'funcName' is the function name of worker registered, 'data' is the opaque data worker should handle

##  Server

~~~go
var server = []string{"localhost:4730", "localhost:4731"}

var worker = gearman.NewWorker(server)

var funcName = "test"

// job handle
// return []byte as data to client for job compelte
// err means job fail
var handle = func(job *gearman.Job) ([]byte, error) {
    // get the data client sent
    data := job.Data()

    log.Printf("get job, data is '%s'", string(data))

    if string(data) == "background" {
        log.Printf("process backgroup job, data '%s'", string(data))
    }

    var retData []byte
    if string(data) == "non-background" {
        log.Printf("process non-backgroud job, data '%s'", string(data))

      	// sendback data to client
        err := job.Update(gearman.JobOptData([]byte("data for job update")))
        if err != nil {
            log.Printf("job data update err, %s", err.Error())
        }

        // simulator task processing
        time.Sleep(3 * time.Second)

        // set the data returned to client for compelte
        retData = []byte("data for job complete")
    }

    return retData, nil
}

err := worker.RegisterFunction(
    // function name worker can handle
    funcName,
    // handler for job process
    handle,
    // set worker can do funcName
    gearman.WorkerOptCanDo(),
)
if err != nil {
    log.Fatal(err)
}

// work loop, block
worker.Work()
~~~

### Worker API

#### func(*Worker) MaxParallelJobs()

~~~go
func (*Worker)MaxParallelJobs(n int)
~~~

set max parallel jobs worker can handle, not this limitation default

#### func (*Worker)RegisterFunction()

~~~go
func (*Worker) RegisterFunction(funcName string, handle JobHandle, opt WorkerOptFunc) error
~~~

Do function register and unregister, 'funcName' is the function worker will handle, 'handle' is the processor of job. 'opt' set other options, see the list blow

'WorkerOptCanDo()', register the handle of 'funcName'

'WorkerOptCanDoTimeout(t)', same as WorkerOptCanDo() but with a timeout value, after the timeout server will set the job failed and notify to the client

'WorkerOptCanotDo()', unregister the handle of 'funcName'

#### func (*Worker) Work()

~~~go
func (*Worker) Work() 
~~~

start grab the jobs from server and process

#### type JobHandle 

~~~go
type JobHandle func(job *Job) (data []byte, err error)
~~~

handler of job worker set when register the function for processing the job client submitted to server. send backed the 'data' to client if there is, 'err' indicate job failed

### Job API

#### func (*Job) Update()

~~~go
func (*Job) Update(opt JobOptFunc) error 
~~~

upate job status or send data to client during job running. 'opt' set the options, see the list below

'JobOptStatus(n, d uint32)', send job complete precent numerator and denominator to client

'JobOptData(data []byte)', send data to client during runnning

#### func (*Job) Data() 

~~~go
func (*Job) Data() []byte
~~~

return the opaque data client send to worker

## Admin

~~~go
var server = []string{"127.0.0.1:4730", "localhost:4731"}

var admin = gearman.NewAdmin(server)

for _, s := range server {
    log.Printf("send admin commands to %s", s)

    // show version
    lines, err := admin.Do(s, gearman.AdmOptVersion())
    if err != nil {
        log.Fatal(err)
    }
    log.Println("version:")
    gearman.PrintLines(lines)

    // show workers
    lines, err = admin.Do(s, gearman.AdmOptWorkers())
    if err != nil {
        log.Fatal(err)
    }
    log.Println("workers:")
    gearman.PrintLines(lines)

    // show status
    lines, err = admin.Do(s, gearman.AdmOptStatus())
    if err != nil {
        log.Fatal(err)
    }
    log.Println("status:")
    gearman.PrintLines(lines)
}
~~~

### func (*Admin) Do()

~~~go
func (*Admin) Do(server string, opt AdmOptFunc) ([]string, error)
~~~

send admin command to 'server'. plain text line returned

command 'version' 'workers' 'status' 'shutdown' 'shutdown' 'maxqueue' supported, set by 'opt' functions, see the list blow

'AdmOptVersion()',  show the version of server

'AdmOptWorkers()', show the worker list of server, returned text line formatter is 

~~~
FD IP-ADDRESS CLIENT-ID : FUNCTION ...
~~~

'AdmOptStatus()', show the registered functions of server, returned text line formateter is

~~~
FUNCTION\tTOTAL\tRUNNING\tAVAILABLE_WORKERS
~~~

'AdmOptMaxQueueAll(funcName, n)', set command 'maxqueue', set  max queue size for a function for all priority

'AdmOptMaxQueueThreePriority(funcName, high, normal, low)', set max queue size for a function for three priority

'AdmOptShutdown()', shutdown the server

'AdmOptShutdownGraceful()', shutdown the server graceful

## License

MIT



