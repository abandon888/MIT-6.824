package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type NArgs struct {
	NReduce int
	NMap    int
}

type MapTask struct {
	FileName string
	Index    int
	NReduce  int
}

type MapReply struct {
	FileName string
	Index    int
	NReduce  int
	Content  string
}

type ReduceTask struct {
	Index                int
	NMap                 int
	IntermediateLocation string
}

type ReduceReply struct {
	Index      int
	NMap       int
	OutputFile string
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
