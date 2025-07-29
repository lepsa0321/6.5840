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

type ExampleReply struct {
	Y int
}

// State represents the status of a task.
type State int

const (
	Waiting State = iota // Waiting state
	Finish               // Finish state
)

type Task struct {
	TaskType TaskType
	NReduce  int
	File     string
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Exit
)

type GetTaskArgs struct {
	state State
}

type GetTaskReply struct {
	task Task
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
