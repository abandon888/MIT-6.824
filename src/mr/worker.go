package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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

func getFileContent(filename string) string {

	//对传入的文件进行遍历操作
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)

}

// MapTask
func doMapTask(mapf func(string, string) []KeyValue, reply *ApplyTaskReply, arg *ApplyTaskAgr) {

	intermediate := []KeyValue{}
	content := getFileContent(arg.fileName)
	arg.startTime = time.Now()
	arg.Status = TaskStatusInProgress

	kva := mapf(arg.fileName, content)
	//使用ihash函数对key进行hash，将相同的key放在一起
	for _, kv := range kva {
		intermediate = append(intermediate, kv)
		//reply.Id = ihash(kv.Key) % reply.NReduce //todo:哈希如何处理？
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-%d-%d", reply.Id, reply.Id) //todo:中间文件应如何命名？
	//将map的结果放入intermediate中
	serializeKeyValuePairs(intermediate, oname)
	reply.IntermediateLocation = oname
	arg.Status = TaskStatusCompleted
}

func doReduceTask(reducef func(string, []string) string, reply *ApplyTaskReply, arg *ApplyTaskAgr) {
	oname := fmt.Sprintf("mr-out-%d", reply.Id) //todo:文件命名怎么搞
	ofile, _ := os.Create(oname)
	//读取中间文件
	intermediate, err := deserializeKeyValuePairs(reply.IntermediateLocation)
	if err != nil {
		log.Fatalf("cannot read %v", reply.IntermediateLocation)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	//将reduce任务的结果发送给coordinator
	reply.outPutFile = oname
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//定义任务
	applyAgr := ApplyTaskAgr{
		Id:        time.Now().String(),
		Status:    TaskStatusNotStarted,
		startTime: time.Now(),
		Type:      mapStatus,
	}
	applyReply := ApplyTaskReply{
		Id: applyAgr.Id,
	}
	call("Coordinator.ApplyTask", &applyAgr, &applyReply)
	for {
		switch applyAgr.Type {
		case mapStatus:
			doMapTask(mapf, &applyReply, &applyAgr)
			call("Coordinator.ApplyTask", &applyAgr, &applyReply)
		case reduceStatus:
			doReduceTask(reducef, &applyReply, &applyAgr)
			call("Coordinator.ApplyTask", &applyAgr, &applyReply)
		case doneStatus:
			call("Coordinator.ApplyTask", &applyAgr, &applyReply)
		default:
			log.Println("undefined workType")
		}
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
