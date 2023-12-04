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

type MRCallArgs struct {
	Status   int //0:等待任务 1:完成任务
	WorkerID int
	TaskID   int //完成的任务ID
}

type MRCallReply struct {
	TaskID      int
	TaskType    int //1 MAP 2 REDUCE 3 Wait
	MapFileName string
	NReduce     int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
