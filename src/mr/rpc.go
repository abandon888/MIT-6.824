package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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

type ApplyTaskReply struct {
	Id                   string
	NReduce              int
	NMap                 int
	IntermediateLocation string //中间文件地址
	outPutFile           string //输出文件地址
}

type ApplyTaskAgr struct {
	Id        string
	Type      WorkType
	Status    TaskStatus
	startTime time.Time
	fileName  string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
