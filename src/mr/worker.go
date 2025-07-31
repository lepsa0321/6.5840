package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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

			// 创建临时文件
			tmpFiles := make([]*os.File, task.NReduce)
			for reduceNum := 0; reduceNum < task.NReduce; reduceNum++ {
				tmpFile, err := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d-", task.TaskID, reduceNum))
				if err != nil {
					log.Fatalf("cannot create temporary file: %v", err)
				}
				tmpFiles[reduceNum] = tmpFile
			}

			// 写入数据到临时文件
			for _, kv := range kva {
				reduceNum := ihash(kv.Key) % task.NReduce
				fmt.Fprintf(tmpFiles[reduceNum], "%v %v\n", kv.Key, kv.Value)
			}

			// 关闭所有临时文件
			for _, tmpFile := range tmpFiles {
				tmpFile.Close()
			}

			for reduceNum, tmpFile := range tmpFiles {
				tmpName := tmpFile.Name()
				finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, reduceNum)

				// 复制文件内容
				tmpContent, err := ioutil.ReadFile(tmpName)
				if err != nil {
					log.Fatalf("cannot read temporary file %v: %v", tmpName, err)
				}

				if err := ioutil.WriteFile(finalName, tmpContent, 0644); err != nil {
					log.Fatalf("cannot write final file %v: %v", finalName, err)
				}

				// 删除临时文件
				if err := os.Remove(tmpName); err != nil {
					log.Printf("warning: cannot remove temporary file %v: %v", tmpName, err)
				}
			}

			// 更新任务状态
			task.TaskState = TaskMapFinish
			ReportTask(ReportTaskArgs{Task: task})

		case Reduce:
			// 更新任务状态
			task.TaskState = TaskRunning
			ReportTask(ReportTaskArgs{Task: task})

			// 收集所有中间文件内容
			intermediate := make(map[string][]string)
			for mapID := 0; mapID < task.NInput; mapID++ {
				filename := fmt.Sprintf("mr-%d-%d", mapID, task.ReduceID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()

				// 解析中间文件内容
				lines := strings.Split(string(content), "\n")
				for _, line := range lines {
					if line == "" {
						continue
					}
					parts := strings.Split(line, " ")
					if len(parts) != 2 {
						continue
					}
					key := parts[0]
					value := parts[1]
					intermediate[key] = append(intermediate[key], value)
				}
			}

			// 创建输出文件
			outputFile := fmt.Sprintf("mr-out-%d", task.ReduceID)
			ofile, err := os.Create(outputFile)
			if err != nil {
				log.Fatalf("cannot create %v", outputFile)
			}

			// 对每个key调用reduce函数
			for key, values := range intermediate {
				output := reducef(key, values)
				fmt.Fprintf(ofile, "%v %v\n", key, output)
			}
			ofile.Close()

			// 报告任务完成
			task.TaskState = TaskFinish
			ReportTask(ReportTaskArgs{Task: task})

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
