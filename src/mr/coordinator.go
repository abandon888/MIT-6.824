package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask
	reduceTasks []ReduceTask
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
	//遍历map任务，如果有未完成的任务，返回false
	for _, task := range c.mapTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	//遍历reduce任务，如果有未完成的任务，返回false
	for _, task := range c.reduceTasks {
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
	c := Coordinator{}
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)

	//初始化map任务
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			FileName: file,
			Index:    i,
			NReduce:  nReduce,
			Status:   TaskStatusNotStarted,
		}
	}
	//初始化reduce任务
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			Index:                i,
			NMap:                 len(files),
			IntermediateLocation: "",
			Status:               TaskStatusNotStarted,
		}
	}
	// start the coordinator server
	c.server()
	return &c
}

// GetMapTask
// worker获取map任务
func (c *Coordinator) GetMapTask(args *NArgs, reply *MapReply) error {
	for i, task := range c.mapTasks {
		if task.Status == TaskStatusNotStarted {
			reply.FileName = task.FileName
			reply.Index = task.Index
			reply.NReduce = task.NReduce
			reply.Content = ""
			c.mapTasks[i].Status = TaskStatusInProgress
			return nil
		}
	}
	reply.Index = -1
	return nil
}

// PutIntermediate
// worker将map任务的结果发送给coordinator
func (c *Coordinator) PutIntermediate(args *ReduceTask, reply *ReduceReply) error {
	c.reduceTasks[args.Index].IntermediateLocation = args.IntermediateLocation
	return nil
}

// completeMapTask
// worker通知coordinator完成map任务
func (c *Coordinator) completeMapTask(args *MapTask, reply *MapReply) error {
	c.mapTasks[args.Index].Status = TaskStatusCompleted
	return nil
}

// GetReduceTask
// worker获取reduce任务
func (c *Coordinator) GetReduceTask(args *NArgs, reply *ReduceReply) error {
	for i, task := range c.reduceTasks {
		if task.Status == TaskStatusNotStarted {
			reply.Index = task.Index
			reply.NMap = task.NMap
			reply.OutputFile = ""
			c.reduceTasks[i].Status = TaskStatusInProgress
			return nil
		}
	}
	reply.Index = -1
	return nil
}

// completeReduceTask
// worker通知coordinator完成reduce任务
func (c *Coordinator) completeReduceTask(args *ReduceTask, reply *ReduceReply) error {
	c.reduceTasks[args.Index].Status = TaskStatusCompleted
	return nil
}
