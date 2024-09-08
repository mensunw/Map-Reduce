package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskFinderArgs struct {
}

type TaskFinderReply struct {
	FileName   []string
	TaskNumber int
	TaskType   int
	ReduceNum  int
}

type MapTaskFinishedArgs struct {
	//FileName   string // includes whole path name to it
	TaskNumber int
	//FilePointers *os.File
}

type MapTaskFinishedReply struct {
}

type ReduceTaskFinishedArgs struct {
	//FileName   string // includes whole path name to it
	TaskNumber int
	//FilePointers *os.File
}

type ReduceTaskFinishedReply struct {
}

type IsReduceFinishedArgs struct {
}

type IsReduceFinishedReply struct {
	Finished bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
