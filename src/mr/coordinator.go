package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	pendingTasks    []Task
	inProgressTasks map[int]Task
	completedTasks  map[int]Task
	mReduce         int
	nMap            int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	defer c.mu.Unlock()

	// Your code here.
	if len(c.pendingTasks) == 0 && len(c.inProgressTasks) == 0 {
		ret = true
	}

	fmt.Printf("Done: %v, len(c.pendingTasks) = %v len(c.inProgressTasks) = %v\n",
		ret, len(c.pendingTasks), len(c.inProgressTasks))

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		pendingTasks:    make([]Task, 0),
		inProgressTasks: make(map[int]Task),
		completedTasks:  make(map[int]Task),
		mReduce:         nReduce,
		nMap:            len(files),
	}

	taskID := 0
	for _, file := range files {
		// 检查文件大小
		fileInfo, err := os.Stat(file)
		if err != nil {
			log.Fatalf("cannot stat file %v: %v", file, err)
		}

		// 如果文件小于10MB，直接作为一个任务
		if fileInfo.Size() <= 10*1024*1024 {
			c.pendingTasks = append(c.pendingTasks, Task{
				TaskID:    taskID,
				TaskType:  Map,
				File:      file,
				NReduce:   nReduce,
				TaskState: TaskWaiting,
			})
			taskID++
			c.nMap++
			continue
		}

		// 大文件拆分为多个子文件
		content, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("cannot read file %v: %v", file, err)
		}

		chunkSize := 10 * 1024 * 1024 // 10MB
		for i := 0; i < len(content); i += chunkSize {
			end := i + chunkSize
			if end > len(content) {
				end = len(content)
			}
			chunk := content[i:end]

			// 创建临时子文件
			chunkFile := fmt.Sprintf("%s.part%d", file, i/chunkSize)
			if err := ioutil.WriteFile(chunkFile, chunk, 0644); err != nil {
				log.Fatalf("cannot write chunk file %v: %v", chunkFile, err)
			}

			// 添加子文件任务
			c.pendingTasks = append(c.pendingTasks, Task{
				TaskID:    taskID,
				TaskType:  Map,
				File:      chunkFile,
				NReduce:   nReduce,
				TaskState: TaskWaiting,
			})
			taskID++
			c.nMap++
		}
	}

	// Your code here.
	go c.checkTimeouts()
	c.server()
	return &c
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pendingTasks) > 0 {
		task := c.pendingTasks[0]
		task.startTime = time.Now()
		c.pendingTasks = c.pendingTasks[1:]
		reply.Task = task
		c.inProgressTasks[task.TaskID] = task
	} else {
		reply.Task = Task{TaskType: Wait}
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := c.inProgressTasks[args.Task.TaskID]
	task.TaskState = args.Task.TaskState
	if args.Task.TaskState == TaskFinish {
		c.completedTasks[args.Task.TaskID] = args.Task
		delete(c.inProgressTasks, args.Task.TaskID)
		// 打印处理时间
		fmt.Printf("task %d finished, time cost: %v\n", args.Task.TaskID, time.Since(task.startTime))
	} else {
		c.inProgressTasks[args.Task.TaskID] = task
	}

	reply.Msg = "ok"

	return nil
}

func (c *Coordinator) checkTimeouts() {
	for {
		c.mu.Lock()
		for k, v := range c.inProgressTasks {
			if v.TaskState == TaskRunning && time.Since(v.startTime) > 10*time.Second {
				v.TaskState = TaskWaiting
				c.pendingTasks = append(c.pendingTasks, v)
				delete(c.inProgressTasks, k)
			}
		}
		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}
