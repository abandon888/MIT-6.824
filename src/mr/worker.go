package mr

import (
	"encoding/json"
	"fmt"
	"os"
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

// serializeKeyValuePairs: 将key/value对序列化到文件中
func serializeKeyValuePairs(keyValues []KeyValue, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(keyValues); err != nil {
		return err
	}

	return nil
}

// deserializeKeyValuePairs: 从文件中反序列化key/value对
func deserializeKeyValuePairs(filename string) ([]KeyValue, error) {
	var keyValues []KeyValue

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&keyValues); err != nil {
		return nil, err
	}

	return keyValues, nil
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	//定义map任务
	mapArgs := MapTask{}
	mapReply := MapReply{}
	//获取map任务
	call("Coordinator.GetMapTask", &mapArgs, &mapReply)
	//循环执行map任务
	for mapReply.Index != -1 {
		fmt.Printf("mapReply.Index: %d\n", mapReply.Index)
		kva := mapf(mapReply.FileName, mapReply.Content)
		//定义reduce任务
		reduceArgs := ReduceTask{}
		reduceReply := ReduceReply{}
		//使用ihash函数对key进行hash，将相同的key放在一起
		for _, kv := range kva {
			intermediate = append(intermediate, kv)
			sort.Sort(ByKey(intermediate))
			reduceArgs.Index = ihash(kv.Key) % mapReply.NReduce
			oname := fmt.Sprintf("mr-%d-%d", mapReply.Index, reduceArgs.Index)
			//将map的结果放入intermediate中
			serializeKeyValuePairs(intermediate, oname)
			reduceArgs.IntermediateLocation = oname
			//将map的结果发送给coordinator
			call("Coordinator.PutIntermediate", &reduceArgs, &reduceReply)
		}
		//通知coordinator完成map任务
		call("Coordinator.completeMapTask", &mapArgs, &mapReply)
	}
	//定义reduce任务
	reduceArgs := ReduceTask{}
	reduceReply := ReduceReply{}
	//获取reduce任务
	call("Coordinator.GetReduceTask", &reduceArgs, &reduceReply)
	//循环执行reduce任务
	for reduceReply.Index != -1 {
		fmt.Printf("reduceReply.Index: %d\n", reduceReply.Index)
		oname := fmt.Sprintf("mr-out-%d", reduceReply.Index)
		ofile, _ := os.Create(oname)
		//获取reduce任务的结果
		getReduceValue(reduceArgs, reducef, ofile)
		ofile.Close()
		//将reduce任务的结果发送给coordinator
		reduceReply.OutputFile = oname
		call("Coordinator.completeReduceTask", &reduceArgs, &reduceReply)
	}
}

func getReduceValue(task ReduceTask, reducef func(string, []string) string, ofile *os.File) {
	//读取中间文件
	intermediate, err := deserializeKeyValuePairs(task.IntermediateLocation)
	if err != nil {
		log.Fatalf("cannot read %v", task.IntermediateLocation)
	}
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
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
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
