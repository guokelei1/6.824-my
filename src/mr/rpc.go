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
type RegisteArgs struct {
	WorkerID int
}
type RegisteReply struct {
	NReduce int
	NMap    int
}
type GetOneTaskArgs struct {
	WorkerID int
}
type GetOneTaskReply struct {
	TaskType    int //1 MAP 2 REDUCE 3 Wait 4 Close
	TaskID      int //MAP id或Reduce ID
	MapFileName string
}
type CommitTaskArgs struct {
	WorkerID int
	TaskID   int
	TaskType int //1 MAP 2 REDUCE
	Status   int // 0 Finish 1 Error
}
type CommitTaskReply struct {
	Status int
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
