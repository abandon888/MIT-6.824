package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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
	//取随机数作为临时文件名
	tempFil, _ := ioutil.TempFile("", fileName)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("close fail", err)
		}
	}(tempFil)

	encoder := json.NewEncoder(tempFil)
	for _, kv := range keyValues {
		err := encoder.Encode(&kv)
		if err != nil {
			log.Println("encode fail", err)
			return err
		}
	}
	err := os.Rename(tempFil.Name(), fileName)
	if err != nil {
		log.Println("rename fail", err)
		return err
	}

	//log.Println("rename", fileName, "to", tempFil)

	return nil
}

// deserializeKeyValuePairs: 从文件中反序列化key/value对
func deserializeKeyValuePairs(filename string) ([]KeyValue, error) {
	var keyValues []KeyValue

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("close fail", err)
		}
	}(file)

	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err != nil {
			//log.Println("decode fail", err)
			break
		}
		keyValues = append(keyValues, kv)
	}

	return keyValues, nil
}

func getFileContent(filename string) string {

	//对传入的文件进行遍历操作
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("doMap:cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		log.Println("close fail", err)
	}
	return string(content)

}

// MapTask
func doMapTask(mapf func(string, string) []KeyValue, reply *ApplyTaskReply, arg *ApplyTaskAgr) {
	//log.Println("map task start", reply.FileName)
	//intermediate := []KeyValue{}
	content := getFileContent(reply.FileName)
	arg.StartTime = time.Now()
	arg.Status = TaskStatusInProgress

	kva := mapf(reply.FileName, content)
	buckets := make([][]KeyValue, reply.NReduce)
	//使用ihash函数对key进行hash，将相同的key放在一起
	for _, kv := range kva {
		buckets[ihash(kv.Key)%reply.NReduce] = append(buckets[ihash(kv.Key)%reply.NReduce], kv)
	}
	var oname string
	//将中间结果放入intermediate中
	for i, bucket := range buckets {
		oname = fmt.Sprintf("mr-%d-%d", reply.Id, i)
		//log.Println("oname", oname)
		err := serializeKeyValuePairs(bucket, oname)
		if err != nil {
			return
		}
	}
	arg.Status = TaskStatusCompleted
	call("Coordinator.MapTaskDone", &arg, &reply)
}

func doReduceTask(reducef func(string, []string) string, reply *ApplyTaskReply, arg *ApplyTaskAgr) {

	arg.StartTime = time.Now()
	arg.Status = TaskStatusInProgress

	oname := fmt.Sprintf("mr-out-%d", reply.Id)
	ofile, _ := ioutil.TempFile("", oname)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("close fail", err)
		}
	}(ofile)
	//读取中间文件
	var intermediate []KeyValue
	for i := 0; i < reply.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.Id)
		kva, err := deserializeKeyValuePairs(fileName)
		if err != nil {
			log.Fatalf("Do Reduce:cannot read %v", fileName)
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		//j为重复元素的个数
		j := i + 1
		//遇到重复的key，j就加1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		//其实j已经实现了统计，这里由于value都是相同，所以可以对value进行累加
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return
		}
		i = j
	}
	err := os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Println("rename fail", err)
		return
	}
	//将reduce任务的结果发送给coordinator
	arg.Status = TaskStatusCompleted
	call("Coordinator.ReduceTaskDone", &arg, &reply)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		//定义任务
		applyAgr := ApplyTaskAgr{
			Status:    TaskStatusNotStarted,
			StartTime: time.Now(),
		}
		applyReply := ApplyTaskReply{}
		call("Coordinator.ApplyTask", &applyAgr, &applyReply)
		applyAgr.TaskId = applyReply.TaskId
		applyAgr.WorkId = applyReply.Id
		applyAgr.Type = applyReply.Type
		switch applyReply.Type {
		case mapStatus:
			doMapTask(mapf, &applyReply, &applyAgr)
		case reduceStatus:
			doReduceTask(reducef, &applyReply, &applyAgr)
		case doneStatus:
			//time.Sleep(2000 * time.Millisecond)
			log.Println("all task done")
			return
		case waitStatus:
			log.Println("no task to do")
			time.Sleep(3 * time.Second)
		}
		time.Sleep(200 * time.Millisecond) //防止频繁请求
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
