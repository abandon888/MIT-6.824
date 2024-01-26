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
	TaskId   string //任务id
}

type ApplyTaskAgr struct {
	TaskId    string
	Status    TaskStatus
	StartTime time.Time
	Type      WorkType
	WorkId    int
	FileName  string
}

type RenameFileAgr struct {
	OldName string
	NewName string
}

type RenameFileReply struct {
	NewName string
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

// 生成TaskId,格式为：工作类型+工作id
func generateTaskId(workType WorkType, id int) string {
	return strconv.Itoa(int(workType)) + "-" + strconv.Itoa(id)
}

// 解析TaskId
func parseTaskId(taskId string) (WorkType, int) {
	workType, _ := strconv.Atoi(string(taskId[0]))
	id, _ := strconv.Atoi(taskId[2:])
	return WorkType(workType), id
}
