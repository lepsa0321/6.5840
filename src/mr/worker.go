package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		fmt.Printf("worker waiting\n")
		task := GetTask(GetTaskArgs{State: Waiting})
		switch task.TaskType {
		case Map:
			// 更新任务状态
			task.TaskState = TaskRunning
			ReportTask(ReportTaskArgs{Task: task})

			// 读取一个文件,
			filename := task.File
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// 执行map函数,返回kva
			kva := mapf(filename, string(content))

			// kva按照nReduce分桶,保存到mr-<TaskID>-<reduceNum>文件中
			for _, kv := range kva {
				reduceNum := ihash(kv.Key) % task.NReduce
				oname := fmt.Sprintf("mr-%d-%d", task.TaskID, reduceNum)
				ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				ofile.Close()
			}

			// 更新任务状态
			task.TaskState = TaskFinish
			ReportTask(ReportTaskArgs{Task: task})

		case Reduce:
			reducef(task.File, task.NInput)
		case Exit:
			fmt.Println("worker exit")
			return
		case Wait:
			time.Sleep(100 * time.Millisecond)
		}
	}

}

func GetTask(args GetTaskArgs) (task Task) {
	getTaskReply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &getTaskReply)
	if ok {
		task = getTaskReply.Task
		//fmt.Printf("Task type: %v, Task State: %v, Task num: %v, nreduce: %v, nmap: %v, file: %v, noutput: %v, ninput: %v\n", Task.TaskType, Task.TaskState, Task.TaskNum, Task.NReduce, Task.NMap, Task.File, Task.NOutput, Task.NInput)
		fmt.Printf("get Task type: %v, get TaskID: %v \n", task.TaskType, task.TaskID)
	} else {
		fmt.Println("get Task failed")
	}
	return
}

func ReportTask(args ReportTaskArgs) {
	reportTaskReply := ReportTaskReply{}
	fmt.Printf("report Task type: %v, TaskID: %v \n", args.Task.TaskType, args.Task.TaskID)
	ok := call("Coordinator.ReportTask", &args, &reportTaskReply)
	if !ok {
		fmt.Println("report Task failed")
	} else {
		fmt.Println("report Task success")
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
