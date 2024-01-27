package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mutex   sync.Mutex
	nMap    int
	nReduce int

	tasks       map[string]ApplyTaskAgr //任务列表
	mapTasks    chan ApplyTaskReply
	reduceTasks chan ApplyTaskReply

	mapDone    chan ApplyTaskAgr
	reduceDone chan ApplyTaskAgr
}

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

		mapDone:    make(chan ApplyTaskAgr, int(math.Max(float64(len(files)), float64(nReduce)))),
		reduceDone: make(chan ApplyTaskAgr, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	//初始化map任务
	for i, file := range files {
		initTask := ApplyTaskReply{
			Id:       i,
			NReduce:  nReduce,
			NMap:     len(files),
			Type:     mapStatus,
			FileName: file,
			TaskId:   generateTaskId(mapStatus, i),
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
			TaskId:  generateTaskId(reduceStatus, i),
		}
		//log.Println("init reduce task", initTask)
		c.reduceTasks <- initTask
	}
	// start the coordinator server
	c.server()
	return &c
}

func (c *Coordinator) ApplyTask(args *ApplyTaskAgr, reply *ApplyTaskReply) error {
	c.mutex.Lock()
	//检查是否有超时的任务和未完成的任务
	for _, task := range c.tasks {
		if task.Status == TaskStatusInProgress && time.Now().Sub(task.StartTime) > 10*time.Second {
			log.Println("task timeout", task)
			if task.Type == mapStatus {
				//log.Println("map task timeout", task)
				c.mapTasks <- ApplyTaskReply{
					Id:       task.WorkId,
					NReduce:  c.nReduce,
					NMap:     c.nMap,
					Type:     mapStatus,
					FileName: task.FileName,
					TaskId:   task.TaskId,
				}
				delete(c.tasks, task.TaskId)
			} else if task.Type == reduceStatus {
				//log.Println("reduce task timeout", task)
				c.reduceTasks <- ApplyTaskReply{
					Id:      task.WorkId,
					NReduce: c.nReduce,
					NMap:    c.nMap,
					Type:    reduceStatus,
					TaskId:  task.TaskId,
				}
				delete(c.tasks, task.TaskId)
			}
		}
	}
	c.mutex.Unlock()
	//如果map任务还没完成，就分配map任务
	if len(c.mapTasks) > 0 {
		initTask := <-c.mapTasks
		reply.Id = initTask.Id
		reply.FileName = initTask.FileName
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Type = mapStatus
		reply.TaskId = initTask.TaskId
		args.Type = mapStatus
		args.Status = TaskStatusInProgress
		args.WorkId = initTask.Id
		args.FileName = initTask.FileName
		args.TaskId = initTask.TaskId
		log.Println("map task apply", args, "remain map task", len(c.mapTasks))
		c.updateTaskStatus(args)
		return nil
	} else if len(c.reduceTasks) > 0 && len(c.mapDone) == c.nMap {
		//如果map任务完成，就分配reduce任务
		reduceTask := <-c.reduceTasks
		reply.Id = reduceTask.Id
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Type = reduceStatus
		reply.TaskId = reduceTask.TaskId
		args.Type = reduceStatus
		args.Status = TaskStatusInProgress
		args.WorkId = reduceTask.Id
		args.TaskId = reduceTask.TaskId
		c.updateTaskStatus(args)
		return nil
	} else if len(c.reduceTasks) == 0 && len(c.reduceDone) == c.nReduce {
		//如果reduce任务完成，就返回done
		reply.Type = doneStatus
		//args.Type = doneStatus
		//args.Status = TaskStatusCompleted
		//c.updateTaskStatus(args)
		return nil
	} else {
		log.Println("no task to apply")
		reply.Type = waitStatus
		return nil
	}
}

func (c *Coordinator) MapTaskDone(args *ApplyTaskAgr, reply *ApplyTaskReply) error {

	//检查任务是否超时
	if time.Now().Sub(args.StartTime) < 10*time.Second {
		log.Println("map task done", args)
		defer func() {
			if recover() != nil {
				// 如果这里的代码被执行，那么说明向管道写入数据时发生了 panic
				// 这里将 panic 恢复，防止程序崩溃
				log.Println("map task done panic")
				close(c.mapDone)
			}
		}()
		c.mapDone <- *args
		c.updateTaskStatus(args)
		return nil
	}

	log.Println("Done: map task timeout", args)
	return nil

}

func (c *Coordinator) ReduceTaskDone(args *ApplyTaskAgr, reply *ApplyTaskReply) error {

	//检查任务是否超时
	if time.Now().Sub(args.StartTime) < 10*time.Second {
		log.Println("reduce task done", args)
		c.reduceDone <- *args
		c.updateTaskStatus(args)
		return nil
	}
	return nil
}

// 修改tasks的状态
func (c *Coordinator) updateTaskStatus(args *ApplyTaskAgr) {
	c.mutex.Lock()
	c.tasks[args.TaskId] = *args
	c.mutex.Unlock()
}
