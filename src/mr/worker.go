package mr

import (
	"fmt"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	//定义map任务
	mapArgs := MapTask{}
	mapReply := MapReply{}
	//与coordinator通信
	call("Coordinator.GetMapTask", &mapArgs, &mapReply)
	//循环执行map任务
	for mapReply.Index != -1 {
		fmt.Printf("mapReply.Index: %d\n", mapReply.Index)
		kva := mapf(mapReply.FileName, mapReply.Content)
		//将map的结果放入intermediate中
		intermediate = append(intermediate, kva...)
		sort.Sort(ByKey(intermediate))
		reduceArgs := ReduceTask{}
		reduceReply := ReduceReply{}
		//将map的结果发送给coordinator
		for i := 0; i < mapReply.NReduce; i++ {
			reduceArgs.Index = i
			reduceArgs.Intermediate = intermediate
			call("Coordinator.PutIntermediate", &reduceArgs, &reduceReply)
		}
		//通知coordinator完成map任务
		call("Coordinator.GetMapTask", &mapArgs, &mapReply)
	}
	//定义reduce任务
	reduceArgs := ReduceTask{}
	reduceReply := ReduceReply{}
	//与coordinator通信
	call("Coordinator.GetReduceTask", &reduceArgs, &reduceReply)
	//循环执行reduce任务
	for reduceReply.Index != -1 {
		fmt.Printf("reduceReply.Index: %d\n", reduceReply.Index)
		//获取reduce任务的结果
		reduceArgs.Value = reducef(reduceArgs.Key, reduceArgs.Value)
		//将reduce任务的结果发送给coordinator
		call("Coordinator.PutResult", &reduceArgs, &reduceReply)
		//通知coordinator完成reduce任务
		call("Coordinator.GetReduceTask", &reduceArgs, &reduceReply)
	}

}

func getReduceValue() output {
	i := 0
	for i < len(intermediate) {
		//j为重复元素的个数
		j := i + 1
		//遇到重复的key，j就加1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		//其实j已经实现了统计，这里由于value都是相同，所以可以对value进行累加
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		i = j
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
