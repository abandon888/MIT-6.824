package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	wg          sync.WaitGroup
	mutex       sync.Mutex
	nMap        int
	nReduce     int
	tasks       map[string]ApplyTaskAgr
	mapTasks    chan ApplyTaskReply
	reduceTasks chan ApplyTaskReply

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
	err := rpc.Register(c)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//log.Println("start server")
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Println(err)
		}
	}()
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//遍历task任务，如果有未完成的任务，返回false
	if c.mapTasks != nil || c.reduceTasks != nil {
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
		nReduce:     nReduce,
		tasks:       make(map[string]ApplyTaskAgr),
		nMap:        len(files),
		mapTasks:    make(chan ApplyTaskReply, int(math.Max(float64(len(files)), float64(nReduce)))), //管道阻塞的问题
		reduceTasks: make(chan ApplyTaskReply, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	//初始化map任务
	for i, file := range files {
		initTask := ApplyTaskReply{
			Id:       i,
			NReduce:  nReduce,
			NMap:     len(files),
			Type:     mapStatus,
			FileName: file,
		}
		c.mapTasks <- initTask
	}

	//初始化reduce任务
	for i := 0; i < nReduce; i++ {
		initTask := ApplyTaskReply{
			Id:      i,
			NReduce: nReduce,
			NMap:    len(files),
			Type:    reduceStatus,
		}
		//log.Println("init reduce task", initTask)
		c.reduceTasks <- initTask
	}
	// start the coordinator server
	c.server()
	return &c
}

func (c *Coordinator) ApplyTask(args *ApplyTaskAgr, reply *ApplyTaskReply) error {
	//wc := sync.WaitGroup{}
	c.mutex.Lock()
	c.tasks[args.Id] = *args
	c.mutex.Unlock()
	//如果map任务还没完成，就分配map任务
	if len(c.mapTasks) > 0 {
		//log.Println("map task apply", args)
		initTask := <-c.mapTasks
		////检查管道，及时关闭
		//if len(c.mapTasks) == 0 {
		//	close(c.mapTasks)
		//}
		reply.Id = initTask.Id
		reply.FileName = initTask.FileName
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Type = mapStatus
		return nil
	} else if len(c.reduceTasks) > 0 {
		//如果map任务完成，就分配reduce任务
		reduceTask := <-c.reduceTasks
		reply.Id = reduceTask.Id
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Type = reduceStatus
		return nil
	} else {
		//如果reduce任务完成，就返回done
		reply.Type = doneStatus
		return nil
	}
}

func (c *Coordinator) MapTaskDone(args *ApplyTaskAgr, reply *ApplyTaskReply) error {
	c.mutex.Lock()
	c.tasks[args.Id] = *args
	c.mutex.Unlock()
	//if args.Status == TaskStatusCompleted {
	//	log.Println("map task completed", args.Id)
	//
	//	return nil
	//}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ApplyTaskAgr, reply *ApplyTaskReply) error {
	c.mutex.Lock()
	c.tasks[args.Id] = *args
	c.mutex.Unlock()
	//if args.Status == TaskStatusCompleted {
	//	log.Println("reduce task completed", args)
	//	<-c.reduceTasks
	//	return nil
	//}
	return nil
}
