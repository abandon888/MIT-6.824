package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskStatus int

const (
	TaskStatusNotStarted TaskStatus = iota
	TaskStatusInProgress
	TaskStatusCompleted
)

type WorkType int

const (
	mapStatus WorkType = iota
	reduceStatus
	doneStatus
)

// 注意全要大写，不然无法导出
type ApplyTaskReply struct {
	Id       int
	NReduce  int
	NMap     int
	Type     WorkType
	FileName string //map任务的文件名
}

type ApplyTaskAgr struct {
	Id                   string
	Status               TaskStatus
	StartTime            time.Time
	IntermediateLocation string //中间文件地址
	OutPutFile           string //输出文件地址
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// 生成id
func generateId() string {
	return strconv.Itoa(os.Getuid()) + strconv.FormatInt(time.Now().UnixNano(), 10)
}
