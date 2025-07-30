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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// State represents the status of a Task.
type State int

const (
	Waiting State = iota // Waiting State
	Finish               // Finish State
)

type TaskState int

const (
	TaskFinish TaskState = iota
	TaskWaiting
	TaskRunning
)

type Task struct {
	TaskType  TaskType
	NReduce   int
	File      string
	TaskState TaskState
	TaskID    int
	startTime time.Time
	NInput    []string
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Exit
	Wait
)

type GetTaskArgs struct {
	State State
}

type GetTaskReply struct {
	Task Task
}

type ReportTaskArgs struct {
	Task Task
}

type ReportTaskReply struct {
	Msg string
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
