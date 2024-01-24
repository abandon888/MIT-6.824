package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mutex     sync.Mutex
	nMap      int
	nReduce   int
	tasks     map[string]ApplyTaskAgr
	initTasks chan ApplyTaskAgr

	doneCh chan ApplyTaskAgr
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//遍历task任务，如果有未完成的任务，返回false
	if len(c.initTasks) != 0 {
		return false
	}
	for _, task := range c.tasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:   nReduce,
		tasks:     make(map[string]ApplyTaskAgr),
		nMap:      len(files),
		initTasks: make(chan ApplyTaskAgr),
	}
	//初始化map任务
	for _, file := range files {
		initTask := ApplyTaskAgr{
			fileName: file,
		}
		c.initTasks <- initTask
	}
	// start the coordinator server
	c.server()
	return &c
}
func (c *Coordinator) ApplyTask(args *ApplyTaskAgr, reply *ApplyTaskReply) error {
	c.mutex.Lock()
	c.tasks[args.Id] = *args
	c.mutex.Unlock()
	switch args.Type {
	case mapStatus:
		initTask := <-c.initTasks
		args.fileName = initTask.fileName
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
	case reduceStatus:
	case doneStatus:
		c.mutex.Lock()
		c.doneCh <- *args
		c.mutex.Unlock()
	default:
	}
	return nil
}
